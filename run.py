#!/usr/bin/env python

from config import Config as config
from job_states import JobStates
from utils import *

import redis
from rq import Queue

import threading
import signal
import sys

def signal_handler(signal, frame):
        sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

import json

from workers import job_execution_wrapper

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
        if item["data"]!=1: #TODO: Remove this conditional
            item['data'] = json.loads(item["data"])
            redis_conn = redis.Redis(connection_pool=self.REDIS_POOL)

            job = JOB_QUEUE.enqueue(job_execution_wrapper, item["data"])
            response_channel = self.config.redis_namespace+"::job_response::"+job.id
            # Note the job will automatically know the job.id by using
            # the get_current_job() function

            # Now we need to pass the response_channel to the client requesting the enque
            # TODO: Validate respond_to_me_at
            redis_conn.rpush(item["data"]["respond_to_me_at"], response_channel)
            # Note we are passing a new response_channel for the  job response to guarantee
            # that individual job responses are not hijacked


            # Register that the job has been enqueue
            # TODO: Try to make sure that this is the first message in the response_channel
            #       Possibly by using Job.create ?
            redis_conn.rpush(response_channel, job_enqueud_template())


    def run(self):
        for item in self.pubsub.listen():
            self.work(item)

if __name__ == "__main__":
    r = redis.Redis()
    client = Listener(r, [config.redis_namespace+'::enqueue_job'], config, JOB_QUEUE, POOL)
    client.start()
