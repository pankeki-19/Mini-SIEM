import time
from typing import Any, Dict, Optional
import redis

RAW_STREAM = "raw-events"
NORM_STREAM = "norm-events"

def get_redis(url: str) -> redis.Redis:
    return redis.Redis.from_url(url, decode_responses=True)

def xadd(r: redis.Redis, stream: str, data: Dict[str, Any]) -> str:
    #Convert everything to strings (Redis Steams field requirements)
    payload = {k: str(v) for k, v in data.items()}
    return r.xadd(stream, payload)

def xreadgroup(
        r: redis.Redis,
        group: str,
        consumer:str,
        stream: str,
        count: int = 50,
        block_ms: int = 2000,
) -> Dict[str, Any]:
    return r.xreadgroup(group, consumer, {stream: ">"}, count=count, block=block_ms)

def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

def ack(r: redis.Redis, stream: str, group: str, msg_id: str) -> None:
    r.xack(stream, group, msg_id)

def now_iso() -> str:
    return time.strftime ("%Y-%m-%dT%H:%SZ", time.gmtime())