from config import Config as config
from job_states import JobStates
from utils import *

import redis
from rq import get_current_job
import time

POOL = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=0)


def _evaluate(data, redis_conn, response_channel):
    result = 0
    for k in range(10):
        #Register Progress
        _progress_update = {}
        _progress_update["percent_complete"] = k*1.0/10
        _progress_update["message"] = str(_progress_update["percent_complete"]) + " per cent complete"

        redis_conn.rpush(response_channel, job_progress_update(_progress_update, _progress_update["message"]))
        result += k
    return result

def _submit(data, redis_conn, response_channel):
    result = 0
    for k in range(10):
        #Register Progress
        _progress_update = {}
        _progress_update["percent_complete"] = k*1.0/10
        _progress_update["message"] = str(_progress_update["percent_complete"]) + " per cent complete"

        redis_conn.rpush(response_channel, job_progress_update(_progress_update, _progress_update["message"]))
        result += k
    return result

def evaluate_wrapper(data):
    redis_conn = redis.Redis(connection_pool=POOL)
    job = get_current_job()
    response_channel = config.redis_namespace+"::job_response::"+job.id
    # Register Job Running event
    redis_conn.rpush(response_channel, job_running_template())
    # Run the job
    result = _evaluate(data, redis_conn, response_channel)
    # Register Job Complete event
    redis_conn.rpush(response_channel, job_complete_template(result))
    return result

def submit_wrapper(data):
    redis_conn = redis.Redis(connection_pool=POOL)
    job = get_current_job()
    response_channel = config.redis_namespace+"::job_response::"+job.id
    # Register Job Running event
    redis_conn.rpush(response_channel, job_running_template())
    # Run the job
    result = _evaluate(data, redis_conn, response_channel)
    # Register Job Complete event
    redis_conn.rpush(response_channel, job_complete_template(result))
    return result
