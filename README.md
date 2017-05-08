# GeccoOptimizationChallenge2017_job_factory

Implementation of a CrowdAI Job Factory for the CrowdAI GeccoOptimizationChallenge2017

# Installation Instructions
```
sudo apt-get install redis-server
git clone git@github.com:spMohanty/GeccoOptimizationChallenge2017_job_factory.git
cd GeccoOptimizationChallenge2017_job_factory
pip install -r requirements.txt
python run.py
# Then in a separate tab
rqworker
# Then in a separate tab
rq-dashboard
# Now you should be able to track job status at localhost:9181
# Then to test if everything works, in a separate tab run
python example_usage.py
```

# Author
S.P. Mohanty <sharada.mohanty@epfl.ch>
