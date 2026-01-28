import logging
import sys
import time
sys.path.insert(0, '..')
from lithopserve.orchestrator import Orchestrator, Job

DATASET_SIZE = 2

task_manager_config = {
    'load': {'batch_size': 1, 'max_concurrency': 8},
    'preprocess': {'batch_size': 1, 'max_concurrency': 2},
    'predict': {'batch_size': 32,'interop': 4, 'intraop': 2, 'n_models': 4}
}

lambda_fexec_args = {'runtime': 'off_sample_311', 'runtime_memory': 3008}

dataset = "2024-02-19_08h36m35s"
with open(f"../datasets/{dataset}.txt") as f:
    urls = f.read().splitlines()
urls.pop(0)
if DATASET_SIZE:
    urls = urls[:DATASET_SIZE]

date_time = time.strftime("%Y-%m-%d_%Hh%Mm%Ss", time.localtime())
orchestrator = Orchestrator(fexec_args = lambda_fexec_args, ec2_host_machine=True, initialize=True, logging_level=logging.INFO)
job = Job(urls, job_name=f'{dataset}_{date_time}', orchestrator_backend="aws_lambda", speculation_enabled=True, keep_alive=True, dynamic_split=True)
result =orchestrator.run_job(job)
print(result['input'])
print(result['output'])

