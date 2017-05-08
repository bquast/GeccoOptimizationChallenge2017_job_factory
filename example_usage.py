from config import Config as config
import redis
import threading
import json

POOL = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=0)
r = redis.Redis(connection_pool=POOL)

data = {
    "function_name": "submit", #or can also provide "evaluate"
    "data" : [1,2,3,4]
}
r.publish(config.redis_namespace+'::enqueue_job', json.dumps(data))
