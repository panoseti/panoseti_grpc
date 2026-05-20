import json
import os
import subprocess
import time
import urllib.parse
import urllib.request

from panoseti_grpc.telemetry.logger import get_logger

service_name = "LOKI_DEBUG"

log_dir = "/var/log/panoseti"
os.makedirs(log_dir, exist_ok=True)

logger = get_logger(service_name, log_dir=log_dir, grpc_enabled=False, per_host=True)

msg = f"TEST MESSAGE {time.time()}"
logger.info(msg)
for h in logger.handlers:
    h.flush()

print(f"Log written for {service_name}: {msg}")

print(f"Files in {log_dir}:")


print(subprocess.run(["ls", "-R", log_dir], capture_output=True, text=True).stdout)

print("Waiting 5s for Alloy to scrape and push to Loki...")
time.sleep(5)

loki_url = "http://loki:3100"
query = f'{{service="{service_name}"}}'

print("Querying Loki...")
try:
    url = f"{loki_url}/loki/api/v1/query_range?query=" + urllib.parse.quote(f'{{service="{service_name}"}}')
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        print("LOKI QUERY_RANGE RESPONSE:", json.dumps(json.loads(resp.read().decode()), indent=2))
except Exception as e:
    print("ERROR range:", e)

try:
    url = f"{loki_url}/loki/api/v1/query?query=" + urllib.parse.quote(f'{{service="{service_name}"}}')
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        print("LOKI INSTANT RESPONSE:", json.dumps(json.loads(resp.read().decode()), indent=2))
except Exception as e:
    print("ERROR instant:", e)
