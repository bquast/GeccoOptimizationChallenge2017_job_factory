#!/usr/bin/env python

from config import Config as config
from job_states import JobStates
from utils import *

import redis
from rq import Queue

import threading
import signal
import sys
import json

from workers import evaluate

POOL = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=0)
r = redis.Redis(connection_pool=POOL)

JOB_QUEUE = Queue(connection=r)

class Listener(threading.Thread):
    def __init__(self, r, channels, config, JOB_QUEUE, REDIS_POOL):
        threading.Thread.__init__(self)
        self.redis = r
        self.REDIS_POOL = REDIS_POOL
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(channels)
        self.JOB_QUEUE = JOB_QUEUE
        self.config = config

    def work(self, item):
        print "WORKER : ", item['channel'], ":", item['data']
        #item["data"] = json.loads(item["data"])
        if item["data"]!=1:#TODO: Figure out why the first event has data 1 and fix this block
            item['data'] = json.loads(item["data"])
            if item["data"]["function_name"] == "evaluate":
                redis_conn = redis.Redis(connection_pool=self.REDIS_POOL)

                job = JOB_QUEUE.enqueue(evaluate, item["data"])
                # job = JOB.enqueue(evaluate, item["data"], redis_conn)
                response_channel = self.config.redis_namespace+"::job_response::"+job.id
                #Register that the job has been enqueue
                redis_conn.rpush(response_channel, job_enqueud_template())


    def run(self):
        for item in self.pubsub.listen():
            self.work(item)

if __name__ == "__main__":
    r = redis.Redis()
    client = Listener(r, [config.redis_namespace+'::enqueue_job'], config, JOB_QUEUE, POOL)
    client.start()

def signal_handler(signal, frame):
        sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


# Subscribe to the namespace+commands channel
# In case of a new job, enqueue job.
# Use the job-id to keep adding responses to a redis list


# """
# A job_factory listens on the `commands` channel, and executes the challenge specific function
# """
# conn.enqueue("function_name", function_data)
# # ..
# # ...
# # ....
# # responds with :
# {
#     job_enqueue_state : crowdai.job_state.ENQUEUED
#                       : crowdai.job_state.WAITING
#                       : crowdai.job_state.RUNNING
#                       : crowdai.job_state.COMPLETE
#                       : crowdai.job_state.ERROR
#                       : crowdai.job_state.TIMEOUT
#     data : {
#     }
#     message : "MESSAGE"
# }
#
#
#
# }
# {
#     job_enqueue_status: True #If the Job Enqueue went well
#     job_enqueue_message: "" #Optional Message in case of job enqueue failure
#     job_input_channel: namespace+"::"+"input_channel_name"
#     job_output_channel: namespace+"::"+"output_channel_name"
# }
