from job_states import JobStates

def response_template():
    _message = {}
    _message["job_state"] = None
    _message["data"] = {}
    _message["message"] = "Placeholder Message"

    return _message

def job_enqueud_template():
    response = response_template()
    response["job_state"] = JobStates.ENQUEUED
    return response

def job_running_template():
    response = response_template()
    response["job_state"] = JobStates.RUNNING
    return response

def job_error_template(message=""):
    response = response_template()
    response["job_state"] = JobStates.ERROR
    response["message"] = message
    return response

def job_progress_update(progress_object, message=""):
    response = response_template()
    response["job_state"] = JobStates.PROGRESS_UPDATE
    response["data"] = progress_object
    response["message"] = message
    return response

def job_complete_template(result, message=""):
    response = response_template()
    response["job_state"] = JobStates.COMPLETE
    response["data"] = result
    response["message"] = message
    return response

def update_progress(redis_conn, response_channel, percent_complete, message=""):
        #Register Progress
        _progress_update = {}
        _progress_update["percent_complete"] = percent_complete
        _progress_update["message"] = message
        redis_conn.rpush(response_channel, job_progress_update(_progress_update, _progress_update["message"]))
