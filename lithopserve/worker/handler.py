#
# (C) Copyright IBM Corp. 2020
# (C) Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import zlib
import time
import json
import uuid
import base64
import pickle
import logging
import traceback
import multiprocessing as mp
from queue import Queue, Empty
from threading import Thread
from multiprocessing import Process, Pipe
from tblib import pickling_support
from types import SimpleNamespace
from multiprocessing.managers import SyncManager

from lithopserve.version import __version__
from lithopserve.config import extract_storage_config
from lithopserve.storage import InternalStorage
from lithopserve.worker.jobrunner import JobRunner
from lithopserve.worker.utils import LogStream, custom_redirection, \
    get_function_and_modules, get_function_data
from lithopserve.constants import JOBS_PREFIX, LITHOPS_TEMP_DIR, MODULES_DIR
from lithopserve.utils import setup_lithops_logger, is_unix_system
from lithopserve.worker.status import create_call_status
from lithopserve.worker.utils import SystemMonitor
from lithopserve.worker.profiler_context import profiling_context

from lithopserve.util.metrics import PrometheusExporter

pickling_support.install()

logger = logging.getLogger(__name__)


class ShutdownSentinel:
    """Put an instance of this class on the queue to shut it down"""
    pass


def create_job(payload: dict) -> SimpleNamespace:
    job = SimpleNamespace(**payload)
    storage_config = extract_storage_config(job.config)
    internal_storage = InternalStorage(storage_config)
    job.func = get_function_and_modules(job, internal_storage)
    job.data = get_function_data(job, internal_storage)

    return job


def function_handler(payload):
    """
    Default function entry point called from Serverless backends
    """
    job = create_job(payload)
    setup_lithops_logger(job.log_level)

    worker_processes = min(job.worker_processes, len(job.call_ids))
    logger.info(f'Tasks received: {len(job.call_ids)} - Worker processes: {worker_processes}')

    if worker_processes == 1:
        work_queue = Queue()
        for call_id in job.call_ids:
            data = job.data.pop(0)
            work_queue.put((job, call_id, data))
        work_queue.put(ShutdownSentinel())
        python_queue_consumer(0, work_queue, )
    else:
        manager = SyncManager()
        manager.start()
        work_queue = manager.Queue()
        job_runners = []

        for call_id in job.call_ids:
            data = job.data.pop(0)
            work_queue.put((job, call_id, data))

        for pid in range(worker_processes):
            work_queue.put(ShutdownSentinel())
            p = mp.Process(target=python_queue_consumer, args=(pid, work_queue,))
            job_runners.append(p)
            p.start()

        for runner in job_runners:
            runner.join()

        manager.shutdown()

    # Delete modules path from syspath
    module_path = os.path.join(MODULES_DIR, job.job_key)
    if module_path in sys.path:
        sys.path.remove(module_path)

    os.environ.pop('__LITHOPS_TOTAL_EXECUTORS', None)


def python_queue_consumer(pid, work_queue, initializer=None, callback=None):
    """
    Listens to the job_queue and executes the individual job tasks
    """
    logger.info(f'Worker process {pid} started')
    while True:
        try:
            event = work_queue.get(block=True)
        except Empty:
            break
        except BrokenPipeError:
            break

        if isinstance(event, ShutdownSentinel):
            break

        task, call_id, data = event
        task.call_id = call_id
        task.data = data
        task.pid = pid

        initializer(pid, task) if initializer is not None else None

        prepare_and_run_task(task)

        callback(pid, task) if callback is not None else None

    logger.info(f'Worker process {pid} finished')


def prepare_and_run_task(task):
    task.start_tstamp = time.time()

    if '__LITHOPS_ACTIVATION_ID' not in os.environ:
        act_id = str(uuid.uuid4()).replace('-', '')[:12]
        os.environ['__LITHOPS_ACTIVATION_ID'] = act_id

    os.environ['LITHOPS_WORKER'] = 'True'
    os.environ['PYTHONUNBUFFERED'] = 'True'
    os.environ.update(task.extra_env)

    storage_backend = task.config['lithopserve']['storage']
    bucket = task.config[storage_backend]['storage_bucket']
    task.task_dir = os.path.join(LITHOPS_TEMP_DIR, bucket, JOBS_PREFIX, task.job_key, task.call_id)
    task.log_file = os.path.join(task.task_dir, 'execution.log')
    task.stats_file = os.path.join(task.task_dir, 'job_stats.txt')
    os.makedirs(task.task_dir, exist_ok=True)

    with open(task.log_file, 'a') as log_strem:
        task.log_stream = LogStream(log_strem)
        with custom_redirection(task.log_stream):
            run_task(task)

    # Unset specific job env vars
    for key in task.extra_env:
        os.environ.pop(key, None)


def send_metrics(call_status, prometheus, job):
    for metric_name, metric_value in call_status.status.items():
        labels_dict = {
            'job_id': job.job_id,
            'call_id': job.call_id,
            'executor_id': job.executor_id,
        }

        labels = list(labels_dict.items())

        if isinstance(metric_value, list):
            for index, value in enumerate(metric_value):
                list_labels_dict = labels_dict.copy()
                list_labels_dict['index'] = str(index)
                list_labels = list(list_labels_dict.items())

                prometheus.send_metric(
                    name=metric_name,
                    value=value,
                    type='gauge',
                    labels=list_labels
                )

        else:
            prometheus.send_metric(
                name=metric_name,
                value=metric_value,
                type='gauge',
                labels=labels
            )


def run_task(task):
    """
    Runs a single job within a separate process
    """
    setup_lithops_logger(task.log_level)

    backend = os.environ.get('__LITHOPS_BACKEND', '')
    logger.info(f"Lithops v{__version__} - Starting {backend} execution")
    logger.info(f"Execution ID: {task.job_key}/{task.call_id}")

    env = task.extra_env
    env['LITHOPS_CONFIG'] = json.dumps(task.config)
    env['__LITHOPS_SESSION_ID'] = '-'.join([task.job_key, task.call_id])
    os.environ.update(env)

    storage_config = extract_storage_config(task.config)
    internal_storage = InternalStorage(storage_config)
    call_status = create_call_status(task, internal_storage)

    runtime_name = task.runtime_name
    memory = task.runtime_memory
    timeout = task.execution_timeout

    if task.runtime_memory:
        logger.debug(f'Runtime: {runtime_name} - Memory: {memory}MB - Timeout: {timeout} seconds')
    else:
        logger.debug(f'Runtime: {runtime_name} - Timeout: {timeout} seconds')

    job_interruped = False

    try:
        # send init status event
        call_status.send_init_event()

        handler_conn, jobrunner_conn = Pipe()
        jobrunner = JobRunner(task, jobrunner_conn, internal_storage)

        prom_enabled = jobrunner.lithops_config['lithopserve'].get('telemetry')
        prom_config = jobrunner.lithops_config.get('prometheus', {})
        prometheus = PrometheusExporter(prom_enabled, prom_config)

        profiler_timeout = jobrunner.lithops_config['lithopserve'].get('profiler_timeout')

        logger.debug('Starting JobRunner process')
        # if task.pid != 0:
        logger.debug(f'JobRunner will run in a separate process with PID {task.pid}')
        jrp = Process(target=jobrunner.run) if is_unix_system() else Thread(target=jobrunner.run)

        process_id = os.getpid() if is_unix_system() else mp.current_process().pid
        # jrp.start()

        sys_monitor = SystemMonitor(process_id)
        sys_monitor.start()

        # if task.pid != 0:
        logger.debug('JobRunner is running in a separate process')
        jrp.start()
        if task.log_level == logging.DEBUG:
            with profiling_context(jobrunner_conn, process_id, prometheus, jobrunner.job, profiler_timeout):
                jrp.join(task.execution_timeout)
            logger.debug('Profiling completed')
        else:
            jrp.join(task.execution_timeout)
        # else:
        #     logger.debug('JobRunner is running in the main thread')
        #     jobrunner.run()
        #     jrp = jobrunner



        sys_monitor.stop()

        logger.debug('JobRunner process finished')

        cpu_info = sys_monitor.get_cpu_info()
        call_status.add('worker_func_cpu_usage', cpu_info['usage'])
        call_status.add('worker_func_cpu_system_time', round(cpu_info['system'], 8))
        call_status.add('worker_func_cpu_user_time', round(cpu_info['user'], 8))

        net_io = sys_monitor.get_network_io()
        call_status.add('worker_func_sent_net_io', net_io['sent'])
        call_status.add('worker_func_recv_net_io', net_io['recv'])

        mem_info = sys_monitor.get_memory_info()
        call_status.add('worker_func_rss', mem_info['rss'])
        call_status.add('worker_func_vms', mem_info['vms'])
        call_status.add('worker_func_uss', mem_info['uss'])

        if jrp.is_alive():
            # If process is still alive after jr.join(job_max_runtime), kill it
            try:
                jrp.terminate()
            except Exception:
                # thread does not have terminate method
                pass
            msg = ('Function exceeded maximum time of {} seconds and was '
                   'killed'.format(task.execution_timeout))
            raise TimeoutError('HANDLER', msg)

        if not handler_conn.poll():
            logger.error('No completion message received from JobRunner process')
            logger.debug('Assuming memory overflow...')
            # Only 1 message is returned by jobrunner when it finishes.
            # If no message, this means that the jobrunner process was killed.
            # 99% of times the jobrunner is killed due an OOM, so we assume here an OOM.
            msg = 'Function exceeded maximum memory and was killed'
            raise MemoryError('HANDLER', msg)

        if os.path.exists(task.stats_file):
            with open(task.stats_file, 'r') as fid:
                for line in fid.readlines():
                    key, value = line.strip().split(" ", 1)
                    try:
                        call_status.add(key, float(value))
                    except Exception:
                        call_status.add(key, value)
                    if key in ['exception', 'exc_pickle_fail']:
                        call_status.add(key, eval(value))

    except KeyboardInterrupt:
        job_interruped = True
        logger.debug("Job interrupted")

    except Exception:
        # internal runtime exceptions
        print('----------------------- EXCEPTION !-----------------------')
        traceback.print_exc(file=sys.stdout)
        print('----------------------------------------------------------')
        call_status.add('exception', True)

        pickled_exc = pickle.dumps(sys.exc_info())
        pickle.loads(pickled_exc)  # this is just to make sure they can be unpickled
        call_status.add('exc_info', str(pickled_exc))

    finally:
        if not job_interruped:
            call_status.add('worker_end_tstamp', time.time())

            # Flush log stream and save it to the call status
            task.log_stream.flush()
            if os.path.isfile(task.log_file):
                with open(task.log_file, 'rb') as lf:
                    log_str = base64.b64encode(zlib.compress(lf.read())).decode()
                    call_status.add('logs', log_str)

            call_status.send_finish_event()

        logger.info("Finished")
