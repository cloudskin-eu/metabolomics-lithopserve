import threading
from flask import Flask, request, jsonify
import sys
import time
sys.path.insert(0, '..')
from lithopserve.orchestrator import Orchestrator, Job
from lithopserve.job_policies import job_policy_2

app = Flask(__name__)
orchestrator = Orchestrator(initialize=False, ec2_host_machine=False, job_policy=job_policy_2, max_job_managers=1,
                            max_task_managers=1)

@app.route('/enqueue', methods=['POST'])
def enqueue():
    try:
        job_dict = request.get_json()
        inputs = job_dict['inputs']
        job_name = job_dict['job_name']
        job = Job(inputs, job_name, orchestrator_backend="local", )
        orchestrator.enqueue_job(job)
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


def run_job_scheduler():
    orchestrator.run_orchestrator()

if __name__ == '__main__':
    thread = threading.Thread(target=run_job_scheduler)
    thread.start()
    app.run(host='0.0.0.0', port=5556)
