import time
import os
import socket
import json
import redis
import logging
from panoseti_grpc.telemetry.logger import get_logger


def run_worker():
    my_host = socket.gethostname()
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_db = int(os.getenv('REDIS_DB', 1))

    r = redis.Redis(host=redis_host, port=6379, db=redis_db, decode_responses=True)

    print(f"[Worker {my_host}] Connecting to Redis at {redis_host}:{redis_db}", flush=True)

    # DEBUG: Check connectivity and keys
    try:
        r.ping()
        print(f"[Worker {my_host}] Redis PING successful. Keys in DB {redis_db}: {r.keys('*')}", flush=True)
    except Exception as e:
        print(f"[Worker {my_host}] Redis PING failed: {e}", flush=True)

    target_host = os.getenv('HEADNODE_IP', 'localhost')
    target_port = int(os.getenv('HEADNODE_GRPC_PORT', 50051))

    # Wait for connectivity
    while True:
        try:
            with socket.create_connection((target_host, target_port), timeout=1):
                print(f"✅ [Worker {my_host}] Linked to {target_host}:{target_port}", flush=True)
                break
        except (OSError, ConnectionRefusedError):
            time.sleep(1)

    service_name = "Distributed_Worker"
    logger = get_logger(service_name, console=False, grpc_enabled=True)
    logger.setLevel(logging.INFO)

    # FORCE LOG to verify data path
    logger.info(json.dumps({"event": "startup", "host": my_host}))
    print(f"[Worker {my_host}] Sent startup log.", flush=True)

    seq_num = 0
    while True:
        try:
            session_id = r.get("DISTRIBUTED_SESSION_ID")

            if not session_id:
                # DEBUG: Print keys if we are waiting too long
                if seq_num % 10 == 0:
                    print(f"[{my_host}] Waiting... Keys in DB {redis_db}: {r.keys('*')}", flush=True)

                seq_num += 1
                time.sleep(1.0)
                continue

            # payload object
            data = {
                "host": my_host,
                "session_id": session_id,
                "seq": seq_num,
                "ts": time.time()
            }

            # CRITICAL: Send as JSON string to ensure compatibility with all handlers
            msg = json.dumps(data)

            # Log with extra context if your handler supports it,
            # otherwise just log the string.
            logger.info(msg)

            # Print to stdout occasionally to prove we are alive
            if seq_num % 100 == 0:
                print(f"📤 [Worker {my_host}] Sent seq={seq_num}", flush=True)

            seq_num += 1
            time.sleep(0.05)

        except Exception as e:
            print(f"⚠️ [Worker {my_host}] Error: {e}", flush=True)
            time.sleep(1.0)


if __name__ == "__main__":
    run_worker()