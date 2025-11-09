#!/usr/bin/env python3
import requests
import time
import json
import os
import sys
import csv
from collections import defaultdict

class DeduplicatedConsumer:
    """
    Multi-topic YAK consumer that:
    ✅ Joins all topics by (server_id, ts)
    ✅ Keeps only one row per server per timestamp
    ✅ Writes to CSV once per completed record
    ✅ Persists offsets and completion state to handle restarts
    ✅ Dynamically fails over to new leader
    """
#this is the consumer class
    def __init__(self, broker_list, consumer_group="default_group"):
        self.brokers = broker_list
        self.consumer_group = consumer_group
        self.current_leader = None
        self.session = requests.Session()
        self.partition_counts = {}
        self.offsets = {}

        # {(server_id, ts): {"topic-cpu": val, "topic-mem": val, ...}}
        self.records = defaultdict(lambda: {t: "" for t in ["topic-cpu", "topic-mem", "topic-net", "topic-disk"]})
        self.completed = set()  # To prevent duplicate writes

        self.columns = ["ts", "server_id", "topic-cpu", "topic-mem", "topic-net", "topic-disk"]
        self.output_file = os.path.join(os.getcwd(), f"{consumer_group}_clean_final.csv")
        self.offset_file = f"{consumer_group}_offsets.json"

        print(f"[INIT] Deduplicated consumer started for group '{consumer_group}'")
        print(f" > Output CSV: {self.output_file}")
        print(f" > Offset file: {self.offset_file}")

        self._load_offsets()
        self._load_or_init_csv()

    def _load_or_init_csv(self):
        """Create CSV if not exists, or scan it to populate 'completed' set."""
        if not os.path.exists(self.output_file):
            # File doesn't exist, create it with header
            with open(self.output_file, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(self.columns)
            print(f"[INFO] CSV initialized with columns: {', '.join(self.columns)}")
        else:
            # File does exist. Scan it to prevent re-writing rows.
            print("[INFO] Existing CSV found. Scanning to restore 'completed' state...")
            try:
                with open(self.output_file, "r", newline="", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    
                    # Find the key columns
                    try:
                        ts_idx = header.index("ts")
                        sid_idx = header.index("server_id")
                    except ValueError:
                        print("[ERROR] Could not find 'ts' or 'server_id' in CSV header. Exiting.")
                        sys.exit(1)

                    # Build the key (sid, ts) for each row
                    count = 0
                    for row in reader:
                        if len(row) > max(ts_idx, sid_idx):
                            key = (row[sid_idx], row[ts_idx])
                            self.completed.add(key)
                            count += 1
                    print(f"[INFO] Restored {count} completed records from CSV.")
            except Exception as e:
                print(f"[ERROR] Could not read existing CSV: {e}")
                sys.exit(1)

    def _load_offsets(self):
        """Load offsets from the JSON file on startup."""
        if not os.path.exists(self.offset_file):
            print("[INFO] No offset file found. Will start from offset 0.")
            return

        print(f"[INFO] Loading offsets from {self.offset_file}...")
        try:
            with open(self.offset_file, "r") as f:
                loaded_offsets = json.load(f)
                
                # Re-initialize self.offsets with correct integer keys
                self.offsets = {}
                for topic, partitions in loaded_offsets.items():
                    # JSON keys are strings, convert partition_id back to int
                    self.offsets[topic] = {int(p_id): int(offset) for p_id, offset in partitions.items()}
                
                print(f"[INFO] Successfully loaded offsets for {len(self.offsets)} topics.")

        except (FileNotFoundError, json.JSONDecodeError, TypeError) as e:
            print(f"[WARN] Could not load offsets: {e}. Starting from 0.")
            self.offsets = {}


    # ---------------- Leader Discovery ----------------
    def find_leader(self):
        print("[INFO] Searching for leader broker...")
        leader_id = None
        for broker_url in self.brokers:
            try:
                r = self.session.get(f"{broker_url}/metadata/leader", timeout=3)
                if r.status_code == 200:
                    leader_id = r.json().get("leader_id")
                    if leader_id:
                        print(f"[INFO] Found leader ID: {leader_id}")
                        break
            except requests.exceptions.RequestException as e:
                print(f"[DEBUG] Broker {broker_url} unreachable: {e}")
                continue
                
        if not leader_id:
            print("[ERROR] Could not determine leader ID from any broker.")
            return False
            
        for broker_url in self.brokers:
            try:
                r = self.session.get(f"{broker_url}/health", timeout=3)
                if r.status_code == 200 and r.json().get("broker_id") == leader_id:
                    self.current_leader = broker_url
                    print(f"[INFO] Leader found at: {broker_url}")
                    return True
            except requests.exceptions.RequestException:
                continue
                
        print(f"[ERROR] Leader {leader_id} found but not reachable at any known URL.")
        return False

    # ---------------- Topic Discovery ----------------
    def discover_all_topics(self):
        if not self.current_leader:
            print("[WARN] No leader to discover topics from.")
            return False
        try:
            r = self.session.get(f"{self.current_leader}/topics", timeout=3)
            
            if r.status_code != 200:
                print(f"[WARN] Failed to discover topics (HTTP {r.status_code}). Leader may have changed.")
                self.current_leader = None
                return False
                
            topics_info = r.json().get("topics", [])
            self.partition_counts.clear()
            found_topics = []
            for topic in topics_info:
                name = topic.get("name")
                if name in ["topic-cpu", "topic-mem", "topic-net", "topic-disk"]:
                    partitions = int(topic.get("partitions", 0))
                    self.partition_counts[name] = partitions
                    # Use setdefault to add new topics/partitions at offset 0
                    # without overwriting loaded offsets
                    topic_offsets = self.offsets.setdefault(name, {})
                    for i in range(partitions):
                        topic_offsets.setdefault(i, 0)
                    
                    found_topics.append(name)
            
            print(f"[INFO] Discovered topics: {', '.join(found_topics)}")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Could not contact leader: {e}")
            self.current_leader = None
            return False

    # ---------------- Consume ----------------
    def consume_from_partition(self, topic, partition_id):
        current_offset = self.offsets.get(topic, {}).get(partition_id, 0)
        try:
            r = self.session.get(
                f"{self.current_leader}/consume",
                params={"topic": topic, "partition": partition_id, "offset": current_offset},
                timeout=5
            )
            
            if r.status_code != 200:
                print(f"[WARN] Failed to consume {topic}:p{partition_id} (HTTP {r.status_code}). Leader may have changed.")
                self.current_leader = None
                return
                
            messages = sorted(r.json().get("messages", []), key=lambda m: m.get("offset", -1))
            if not messages:
                return # No new messages
                
            new_offset = current_offset
            for msg in messages:
                # The 'data' field contains the full message written by the leader
                # The 'payload' field within 'data' is the original producer payload
                payload = msg.get("data", {}).get("payload", {})
                ts, sid = payload.get("ts"), payload.get("server_id")
                if not ts or not sid:
                    continue

                # Logic to extract correct metric based on topic
                metric_val = ""
                if topic == "topic-cpu":
                    metric_val = payload.get("cpu_pct", "")
                elif topic == "topic-mem":
                    metric_val = payload.get("mem_pct", "")
                elif topic == "topic-disk":
                    metric_val = payload.get("disk_io", "")
                elif topic == "topic-net":
                    # Combine net_in and net_out for the single 'topic-net' column
                    net_in = payload.get("net_in", 0)
                    net_out = payload.get("net_out", 0)
                    metric_val = f"{net_in}/{net_out}" 

                if metric_val == "":
                    print(f"[WARN] Skipping msg in {topic}, missing expected metric keys in {payload}")
                    continue 

                key = (str(sid), str(ts)) # Ensure keys are strings for consistency

                # Update in-memory record
                self.records[key][topic] = metric_val
                self.records[key]["ts"] = str(ts)
                self.records[key]["server_id"] = str(sid)

                # Check completion
                if all(self.records[key][t] != "" for t in ["topic-cpu", "topic-mem", "topic-net", "topic-disk"]):
                    if key not in self.completed:
                        self._write_row(self.records[key])
                        self.completed.add(key)
                        # Clean up memory for completed record
                        del self.records[key]

                # We will ask for the next offset *after* this one
                new_offset = msg.get("offset") + 1
            
            # Update offset after processing batch
            self.offsets[topic][partition_id] = new_offset

        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Network error consuming {topic}:p{partition_id}: {e}")
            self.current_leader = None

    # ---------------- Write Row ----------------
    def _write_row(self, data):
        """Write one deduplicated row."""
        try:
            with open(self.output_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.columns)
                writer.writerow({col: data.get(col, "") for col in self.columns})
            print(f"[WRITE] {data['server_id']} @ {data['ts']} - complete row written")
        except Exception as e:
            print(f"[ERROR] Failed to write row to CSV: {e}")

    # ---------------- Main Loop ----------------
    def run(self):
        print("\n[INFO] Starting deduplicated consumer loop (Ctrl+C to stop)...")
        last_save = time.time()
        try:
            while True:
                if not self.current_leader:
                    if not self.find_leader():
                        print("[INFO] No leader found. Retrying in 3s...")
                        time.sleep(3)
                        continue
                    else:
                        # Just found a new leader, force topic re-discovery
                        self.partition_counts.clear()

                if not self.partition_counts:
                    if not self.discover_all_topics():
                        print("[INFO] Topic discovery failed. Retrying in 3s...")
                        time.sleep(3)
                        continue

                # --- Poll all partitions ---
                for topic, count in self.partition_counts.items():
                    for p in range(count):
                        self.consume_from_partition(topic, p)
                
                # --- Periodically save offsets ---
                if time.time() - last_save > 30: # Save every 30 seconds
                    self.save_offsets()
                    last_save = time.time()

                time.sleep(2) # Poll interval

        except KeyboardInterrupt:
            print("\n[INFO] Gracefully stopping consumer...")
            self.save_offsets()
            print("[INFO] Exiting.")
            sys.exit(0)

    def save_offsets(self):
        try:
            with open(self.offset_file, "w") as f:
                json.dump(self.offsets, f, indent=2)
            print(f"[INFO] Offsets saved.")
        except Exception as e:
            print(f"[ERROR] Failed to save offsets: {e}")


# ---------------- Run ----------------
if __name__ == "__main__":
    BROKER_NODES = [
        'http://192.168.191.203:5001', # Primary Leader
        'http://192.168.191.242:5002'  # Follower
    ]
    
    # Use a unique consumer group name
    consumer_group_name = "pes1ug23cs499"
    
    consumer = DeduplicatedConsumer(
        broker_list=BROKER_NODES,
        consumer_group=consumer_group_name
    )
    consumer.run()