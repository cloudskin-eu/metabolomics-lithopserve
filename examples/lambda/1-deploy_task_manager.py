from lithopserve.orchestrator import Orchestrator

lambda_fexec_args = {'runtime': 'off_sample_311', 'runtime_memory': 3008}

if __name__ == '__main__':
    orchestrator = Orchestrator(fexec_args=lambda_fexec_args, ec2_host_machine=False, initialize=False)
    orchestrator.delete_runtime()
    if orchestrator.check_runtime_status():
        print("Runtime is available")
    else:
        print("Runtime is not available")
        orchestrator.redeploy_runtime()
