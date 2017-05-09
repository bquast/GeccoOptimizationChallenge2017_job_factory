from job_states import JobStates
import json

def response_template(job_id):
    _message = {}
    _message["job_id"] = job_id
    _message["job_state"] = None
    _message["data"] = {}
    _message["message"] = "Placeholder Message"

    return _message

def job_enqueud_template(job_id):
    response = response_template(job_id)
    response["job_state"] = JobStates.ENQUEUED
    return response

def job_running_template(job_id):
    response = response_template(job_id)
    response["job_state"] = JobStates.RUNNING
    return response

def job_error_template(job_id, message=""):
    response = response_template(job_id)
    response["job_state"] = JobStates.ERROR
    response["message"] = message
    return response

def job_progress_update(job_id, progress_object, message=""):
    response = response_template(job_id)
    response["job_state"] = JobStates.PROGRESS_UPDATE
    response["data"] = progress_object
    response["message"] = message
    return response

def job_complete_template(job_id, result, message=""):
    response = response_template(job_id)
    response["job_state"] = JobStates.COMPLETE
    response["data"] = result
    response["message"] = message
    return response

# TODO: Refactor all job events to use context instead
def update_progress(context, percent_complete, message=""):
        #Register Progress
        _progress_update = {}
        _progress_update["percent_complete"] = percent_complete
        _progress_update["message"] = message
        context['redis_conn'].rpush(context['response_channel'], json.dumps(job_progress_update(context['job_id'], _progress_update, _progress_update["message"])))
