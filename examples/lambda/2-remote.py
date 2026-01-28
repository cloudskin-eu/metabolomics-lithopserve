import sys
import time
sys.path.insert(0, '..')
from lithopserve.orchestrator import Orchestrator, Job

task_manager_config = {
    'load': {'batch_size': 1, 'max_concurrency': 32},
    'preprocess': {'batch_size': 1, 'max_concurrency': 2},
    'predict': {'batch_size': 32,'interop': 4, 'intraop': 2, 'n_models': 4}
}

lambda_fexec_args = {'runtime': 'off_sample_311', 'runtime_memory': 3008}

dataset = "2024-02-15_22h39m22s"
with open(f"../datasets/{dataset}.txt") as f:
    urls = f.read().splitlines()

DATASET_SIZE = 4
urls.pop(0)
if DATASET_SIZE:
    urls = urls[:DATASET_SIZE]

date_time = time.strftime("%Y-%m-%d_%Hh%Mm%Ss", time.localtime())
orchestrator = Orchestrator(fexec_args = lambda_fexec_args, ec2_host_machine=False, initialize=False)
job = Job(urls,job_name= f'{dataset}_{date_time}', orchestrator_backend="aws_lambda", dynamic_split=False)
orchestrator.run_job(job)

