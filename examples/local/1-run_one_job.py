import logging
import os
import sys
import time
sys.path.insert(0, '..')
from lithopserve.orchestrator import Orchestrator, Job
from lithopserve.job_policies import no_policy

DATASET_SIZE = 100

task_manager_config = {
    'load': {'batch_size': 1, 'max_concurrency': 32},
    'preprocess': {'batch_size': 1, 'num_cpus': 2},
    'predict': {'batch_size': 32,'interop': 4, 'intraop': 2, 'n_models': 4}
}

dataset = "2024-02-29_03h38m10s"
with open(f"../datasets/{dataset}.txt") as f:
    urls = f.read().splitlines()
if DATASET_SIZE:
    urls = urls[:DATASET_SIZE]

# Print the urls that are repeated
from collections import Counter
print([k for k, v in Counter(urls).items() if v > 1])

date_time = time.strftime("%Y-%m-%d_%Hh%Mm%Ss", time.localtime())
orchestrator = Orchestrator(initialize=False, ec2_host_machine=False,
                            logging_level=logging.DEBUG, job_policy=no_policy)
job = Job(urls, f'{dataset}_{date_time}', orchestrator_backend="local", dynamic_split=True, split_size=1,
          num_task_managers=4)
orchestrator.run_job(job)
