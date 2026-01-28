import lithops
from off_sample_orchestrator.orchestrator import Orchestrator, Job
from off_sample_orchestrator.job_policies import no_policy

k8s_fexec_args={'runtime': 'account/pytorch-image:latest', 'runtime_memory': 3008, 'runtime_cpu': 2,}

config_dict = {
    "load": {"max_concurrency": 32, "batch_size": 1},
    "preprocess": {"max_concurrency": 2, "batch_size": 1},
    "predict": {"batch_size": 32, "interop": 1, "intraop": 1, "n_models": 1}
}

if __name__ == "__main__":
    orchestrator = Orchestrator(fexec_args_dict={'k8s': k8s_fexec_args}, initialize=True, job_policy=no_policy)
    fexec = lithops.FunctionExecutor()
    object_list = fexec.storage.storage_handler.list_objects(fexec.storage.bucket, "2024-01-03_11h10m14s")
    keys = [obj['Key'] for obj in object_list]
    NUM_IMAGES= 32
    keys = keys[:NUM_IMAGES]
    print(keys)
    job = Job(keys, job_name=f'Test', orchestrator_backend="k8s", bucket="bucket", num_task_managers=1, output_storage="local", output_location="./result/results")
    result = orchestrator.run_job(job)
    print(result)
