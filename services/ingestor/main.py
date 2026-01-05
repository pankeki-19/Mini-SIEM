import os
import random
import time
from common.streams import get_redis, xadd, RAW_STREAM, now_iso

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

USERS = ["alex", "sam", "chris", "root", "dev", "admin"]
IPS = ["203.0.113.10", "198.51.100.23", "192.0.2.44", "45.33.32.156", "104.21.12.34"]
PATHS = ["/", "/login", "/admin", "/wp-login.php", "/.env", "/api/v1/users", "/robots.txt"]

def emit_auth_event(r):
    user = random.choice(USERS)
    ip = random.choice(IPS)
    action = random.choice(["login_failed", "login_success"], weights=[0.8, 0.2])[0]

    events = {
        "@timestamp": now_iso(),
        "dataset": "auth",
        "raw": f"{action} user={user} ip{ip}",
        "user": user,
        "ip": ip,
        "action": action
    }
    xadd(r, RAW_STREAM, event)

def emit_web_event(r):
    ip = random.choice(IPS)
    path = random.choice(PATHS)
    method = random.choice(["GET", "POST"])
    status = random.choice([200, 302, 401, 403, 404, 500], weights=[40, 10, 10, 10, 25, 5])[0]

    event = {
        "@timestamp": now_iso,
        "dataset": "web",
        "raw": f'{ip} "{method} {path}" {status}',
        "ip": ip,
        "method": method,
        "path": path,
        "status": status,
    }
    xadd(r, RAW_STREAM, event)

def main():
    r = get_redis(REDIS_URL)
    print ("Ingestor started; writing raw events to Redis Streams...")

    # "Brust mode" patterns to create detections:

    while True:
        #Every ~30 cycles, simulate a brute force burst
        if burst_counter == 0:
            attacker_ip = random.choices(IPS)
            victim_user = random.choice(["admin", "root"])
            for _ in range (10):
                xadd(r, RAW_STREAM, {
                    "@timestamp": now_iso(),
                    "dataset": "auth",
                    "raw": f"login_failed user={victim_user} ip={attacker_ip}",
                    "user": victim_user,
                    "ip": attacker_ip,
                    "action": "login_failed",  
                })
            burst_counter = 30

        #Normal traffic
        emit_auth_event(r)
        emit_web_event(r)

        burst_counter -= 1
        time.sleep(0.5)

if __name__ == "__main__":
    main()