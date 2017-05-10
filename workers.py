from config import Config as config
from job_states import JobStates
from utils import *

import redis
from rq import get_current_job
import time
import json

POOL = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=0)

def _evaluate(data, context):
    """
        Takes a single list of params and computes the score based on BluePyOpt
    """
    print "Received : ", data, " Sequence Number : ", context["data_sequence_no"]
    result = 0
    for k in range(100):
        percent_complete = k*1.0/100 * 100
        update_progress(context, percent_complete, "")
        print "Context Response Channel ::: ", context['response_channel']
        if k%20==0:
            print "Update : ", percent_complete
        result += k
    _result_object = {
        "score" : result,
    }
    return _result_object

def _submit(data, context):
    """
        Takes a single list of params and computes the score based on BluePyOpt
        Also generates the relevant Images, and uploads to a S3 bucket, and sends back the link
    """
    print "Received : ", data, " Sequence Number : ", context["data_sequence_no"]
    result = 0
    for k in range(10):
        time.sleep(1)
        percent_complete = k*1.0/100
        update_progress(context, percent_complete, "")
        result += k

    _result_object = {
        "score" : result,
        "thumbnail" : "link-to-thumbnail-for-the-submission-on-the-leaderboard",
        "image" : "link-to-image-for-the-submission-on-the-leaderboard"
    }
    return _result_object

def _update_job_event(_context, data):
    """
        Helper function to serialize JSON
        and make sure redis doesnt messup JSON validation
    """
    redis_conn = _context['redis_conn']
    response_channel = _context['response_channel']
    data['data_sequence_no'] = _context['data_sequence_no']

    redis_conn.rpush(response_channel, json.dumps(data))

def job_execution_wrapper(data):
    redis_conn = redis.Redis(connection_pool=POOL)
    job = get_current_job()

    _context = {}
    _context['redis_conn'] = redis_conn
    _context['response_channel'] = data['respond_to_me_at']
    _context['job_id'] = job.id
    _context['data_sequence_no'] = data['data_sequence_no']

    # Register Job Running event
    _update_job_event(_context, job_running_template(job.id))
    result = {}
    try:
        if data["function_name"] == "evaluate":
            # Run the job
            result = _evaluate(data["data"], _context)
            # Register Job Complete event
            _update_job_event(_context, job_complete_template(job.id, result))
        elif data["function_name"] == "submit":
            result = _submit(data["data"], _context)
            # Register Job Complete event
            _update_job_event(_context, job_complete_template(job.id, result))
        else:
            _error_object = job_error_template(job.id, "Function not implemented error")
            _update_job_event(_context, job_error_template(job.id, result))
            result = _error_object
    except Exception as e:
        print "Error : ", str(e)
        _error_object = job_error_template(job.id, str(e))
        _update_job_event(_context, _error_object)
    return result
