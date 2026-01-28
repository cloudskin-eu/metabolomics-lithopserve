import sys
from off_sample_orchestrator.orchestrator import Orchestrator
if __name__ == '__main__':
    k8s_fexec_args = {'runtime': 'account/pytorch-image:latest', 'runtime_memory': 3008, 'runtime_cpu': 2}
    orchestrator = Orchestrator(fexec_args_dict={'k8s': k8s_fexec_args}, initialize=False)
    orchestrator.delete_runtime()
    orchestrator.redeploy_runtime()
