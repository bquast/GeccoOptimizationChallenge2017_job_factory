from config import Config as config
import redis
import threading
import json
import uuid

POOL = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=0)
r = redis.Redis(connection_pool=POOL)

respond_to_me_at = config.redis_namespace+'::enqueue_job_response::'+ str(uuid.uuid4())
data = {
    "respond_to_me_at": respond_to_me_at,
    "function_name": "evaluate", #or can also provide "submit"
    "data" : [1,2,3,4]
}
r.publish(config.redis_namespace+'::enqueue_job', json.dumps(data))

response_channel_name = r.blpop(respond_to_me_at)
while True:
    print r.blpop(response_channel_name)
