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
        percent_complete = k*1.0/100
        update_progress(redis_conn, percent_complete, "Optional Message")
        result += k
    return result

def _submit(data, redis_conn, response_channel):
    result = 0
    for k in range(10):
        percent_complete = k*1.0/100
        update_progress(redis_conn, percent_complete, "Optional Message")
        result += k
    return result

def job_execution_wrapper(data):
    redis_conn = redis.Redis(connection_pool=POOL)
    job = get_current_job()
    response_channel = config.redis_namespace+"::job_response::"+job.id

    # Register Job Running event
    redis_conn.rpush(response_channel, job_running_template())

    if data["function_name"] == "evaluate":
        # Run the job
        result = _evaluate(data, redis_conn, response_channel)
        # Register Job Complete event
        redis_conn.rpush(response_channel, job_complete_template(result))        
    elif data["function_name"] == "submit":
        result = _submit(data, redis_conn, response_channel)
        # Register Job Complete event
        redis_conn.rpush(response_channel, job_complete_template(result))
    else:
        _error_object = job_error_template("Function not implemented error")
        redis_conn.rpush(response_channel, job_complete_template(result))
    return result
