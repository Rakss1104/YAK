# Yet Another Kafka: Follower Broker

This document explains the `follower_final.py` script, which acts as a hot-standby (follower) broker for your distributed message system. Its primary role is to passively mirror the leader broker and be ready to take over leadership if the leader fails.

## 🚀 Core Features

  * **Passive Replication:** As a follower, its main job is to listen for data on the `/internal/replicate` endpoint. It blindly accepts messages from the leader and writes them to its own local log files.
  * **Leader Election:** The broker runs a background thread (`watch_leader_status`) that constantly monitors a `leader_lease` key in Redis.
  * **Automatic Failover:** If the leader's lease in Redis expires (meaning the leader is down), this follower will immediately try to acquire the lease (`attempt_leader_election`).
  * **Promotion to Leader:** If it successfully acquires the lease, it promotes itself to **Leader** (`IS_LEADER = True`).
  * **Active-Mode Endpoints:** Once promoted to leader, its `/produce` and `/consume` endpoints become active, allowing it to serve producer and consumer requests directly.
  * **Idempotence (as Leader):** When promoted, the `/produce` endpoint ensures "exactly-once" semantics for producers. It uses Redis to lock a unique `msg_id` for a set time (`IDEMPOTENCE_EXPIRY_SECONDS`), preventing duplicate messages from being processed.
  * **Web Dashboard:** It serves a comprehensive monitoring dashboard at the `/leader` endpoint, which uses JavaScript to fetch data from `/health`, `/metrics`, and `/topics`.

## ⚙️ How It Works (Code Explanation)

### Key Global State

  * `IS_LEADER`: A boolean flag, `False` by default. This controls the broker's behavior.
  * `REDIS_CLIENT`: The connection to Redis, which is used as the central coordinator for leader election and high-water marks.
  * `TOPICS`: A dictionary holding the log file paths for each topic and partition this broker manages.

### Main Endpoints

  * **`/internal/replicate` [POST] (Follower-Only)**

      * This is the primary endpoint used when `IS_LEADER` is `False`.
      * It receives a message payload from the *current* leader.
      * It finds the correct local log file (e.g., `follower-hostname_my-topic_p0.log`) and appends the message.
      * It **does not** check for duplicates; it trusts the leader to have already done so.

  * **`/produce` [POST] (Dual-Mode)**

      * **If Follower:** Rejects the request with a `400` error, telling the client who the real leader is.
      * **If Promoted to Leader:** This endpoint becomes active.
        1.  It requires a `msg_id` in the JSON payload.
        2.  It attempts to set a lock key (`yak_msg_lock:<msg_id>`) in Redis using `nx=True` (set if not exists).
        3.  If the key *already exists*, it recognizes it as a duplicate, logs a warning, and returns a `200 OK` (so the producer doesn't retry).
        4.  If the key is *new*, it writes the message to the local log, increments the High Water Mark (HWM) in Redis, and returns `200 OK`.

  * **`/consume` [GET] (Leader-Only)**

      * This endpoint only works if `IS_LEADER` is `True`.
      * It reads messages from the local log file for a given topic/partition, starting from the requested `offset`.
      * It only serves messages up to the committed High Water Mark (HWM) stored in Redis.

  * **`/leader` [GET]**

      * Serves the `dashboard.html` file, which provides the UI.

  * **`/metrics`, `/health`, `/topics` [GET]**

      * JSON API endpoints that provide data to the dashboard's JavaScript.

### Leader Election & Failover

1.  **`watch_leader_status()`**: A background thread starts on launch. It loops forever, sleeping for half the `LEASE_TIME_SECONDS`.
2.  In each loop, it checks the `leader_lease` key in Redis.
3.  **Case 1 (No Leader):** If `leader_lease` is `None`, it calls `attempt_leader_election()`.
4.  **`attempt_leader_election()`**: This function tries to set the `leader_lease` key to its own `BROKER_ID` with an expiration time (`ex=LEASE_TIME_SECONDS`) and the `nx=True` flag.
      * **Success:** It wins the election\! It sets `IS_LEADER = True`, logs it, and starts the `_renew_lease()` thread.
      * **Failure:** Another broker won. It remains a follower.
5.  **`_renew_lease()`**: This thread only runs if the broker is the leader. It wakes up every `RENEW_INTERVAL_SECONDS` to refresh its own lease, effectively saying "I'm still alive." If it ever fails to renew (e.g., Redis disconnect), it steps down as leader (`IS_LEADER = False`).

## 📋 Prerequisites

1.  **Python 3.x**
2.  **A running Redis Server.** Your code is configured to connect to:
      * **Host:** `192.168.191.242`
      * **Port:** `6379`
      * *(You must update `REDIS_HOST` in the script if your Redis server is elsewhere.)*
3.  **Python libraries:** `Flask`, `redis`, `requests`.

## ▶️ How to Run

1.  **Create your project structure:**

      * The Flask app expects the HTML file to be in a folder named `templates`.

      * Your directory should look like this:

        ```
        your_project_folder/
        ├── follower_final.py
        ├── requirements.txt
        └── templates/
            └── dashboard.html
        ```

2.  **Save the Files:**

      * Save the Python code as `follower_final.py`.
      * Save the `requirements.txt` content (from above) as `requirements.txt`.
      * Create a `templates` folder.
      * Save the HTML code inside that folder as `dashboard.html`.

3.  **Install Dependencies:**

      * Open a terminal in `your_project_folder` and run:
        ```bash
        pip install -r requirements.txt
        ```

4.  **Run the Follower Broker:**

      * Start the broker (it will run on port `5002` as defined in the script):
        ```bash
        python3 follower_final.py
        ```
      * You will see logs indicating it has started as a follower and is monitoring the leader.

5.  **View the Dashboard:**

      * Open your web browser and go to the URL for the *follower* broker:
      * **[http://192.168.191.242:5002/leader](http://192.168.191.242:5002/leader)**
      * *(If you are running on the same machine, you can also use `http://localhost:5002/leader`)*

The dashboard will show the broker's status (Leader or Follower) and all system metrics in real-time.