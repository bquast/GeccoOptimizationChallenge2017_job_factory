#!/usr/bin/env python

from config import Config
import redis
import threading


POOL = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=0)
r = redis.Redis(connection_pool=POOL)

class Listener(threading.Thread):
    def __init__(self, r, channels):
        threading.Thread.__init__(self)
        self.redis = r
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(channels)

    def work(self, item):
        print item['channel'], ":", item['data']

    def run(self):
        for item in self.pubsub.listen():
            if item['data'] == "KILL":
                self.pubsub.unsubscribe()
                print self, "unsubscribed and finished"
                break
            else:
                self.work(item)

if __name__ == "__main__":
    r = redis.Redis()
    client = Listener(r, [config.redis_namespace+'::enqueue_job'])
    client.start()

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
