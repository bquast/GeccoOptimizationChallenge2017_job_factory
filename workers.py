from config import Config as config
from job_states import JobStates
from utils import *

import redis
from rq import get_current_job
import time
import json

import sys
import random

POOL = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=0)

def _evaluate(data, context):
    """
        Takes a single list of params and computes the score based on BluePyOpt
    """
    print "Received : ", data, " Sequence Number : ", context["data_sequence_no"]
    score = 0
    secondary_score = 0

    for k in range(100):
        # time.sleep(0.05)
        percent_complete = k*1.0/100 * 100
        update_progress(context, percent_complete, "")
        print "Context Response Channel ::: ", context['response_channel']
        if k%20==0:
            print "Update : ", percent_complete
        score += random.randint(1,100)*1.0/0.7 / 100
        secondary_score += random.randint(1,100)*1.0/0.7 / 100

    _result_object = {
        "score" : score,
        "secondary_score" : secondary_score,
    }
    return _result_object

def _submit(data, context):
    """
        Takes a single list of params and computes the score based on BluePyOpt
    """
    _result_object = _evaluate(data, context)
    _result_object["comment"] = ""
    _result_object["media_large"] = "https://upload.wikimedia.org/wikipedia/commons/4/44/Drift_Diffusion_Model_Accumulation_to_Threshold_Example_Graphs.png"
    _result_object["media_thumbnail"] = "https://upload.wikimedia.org/wikipedia/commons/4/44/Drift_Diffusion_Model_Accumulation_to_Threshold_Example_Graphs.png"
    _result_object["media_content_type"] = "image/jpeg"
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
    _update_job_event(_context, job_running_template(_context['data_sequence_no'], job.id))
    result = {}
    try:
        if data["function_name"] == "evaluate":
            # Run the job
            result = _evaluate(data["data"], _context)
            # Register Job Complete event
            _update_job_event(_context, job_complete_template(_context, result))
        elif data["function_name"] == "submit":
            # Run the job
            result = _submit(data["data"], _context)
            # Register Job Complete event
            _update_job_event(_context, job_complete_template(_context, result))
        else:
            _error_object = job_error_template(job.id, "Function not implemented error")
            _update_job_event(_context, job_error_template(job.id, result))
            result = _error_object
    except Exception as e:
        print "Error : ", str(e)
        _error_object = job_error_template(_context['data_sequence_no'], job.id, str(e))
        _update_job_event(_context, _error_object)
    return result
