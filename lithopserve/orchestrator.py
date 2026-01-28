import json
import random
import socket
import subprocess
import logging
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import threading
from threading import Thread
import os
import shutil
import concurrent
from concurrent import futures
from queue import Queue
from .included_function.grpc_assets.split_grpc_pb2 import splitResponse
from .included_function.grpc_assets.split_grpc_pb2_grpc import SPLITRPCServicer, add_SPLITRPCServicer_to_server
import grpc
from lithopserve import FunctionExecutor
from .constants_orchestrator import MAX_JOB_MANAGERS, MAX_TASK_MANAGERS, \
    DEFAULT_IP, DYNAMIC_SPLIT, SPLIT_ENUMERATOR_THREAD_POOL_SIZE, DEFAULT_ORCHESTRATOR, \
    ORCHESTRATOR_BACKENDS, EC2_HOST_MACHINE, OUTPUT_STORAGE, DEFAULT_TASK_MANAGER_CONFIG, EC2_METADATA_SERVICE, \
    SPECULATIVE_EXECUTION, SPECULATION_MULTIPLIER, SPECULATION_QUARTILE, SPECULATION_INTERVAL_MS, MAX_SPLIT_RETRIES, \
    LOGGING_FORMAT, SPECULATION_ADD_TASK_MANAGER, DEFAULT_TASK_MANAGERS, DEFAULT_SPLIT_SIZE, AWS_LAMBDA_BACKEND, \
    LOCAL_BACKEND, EXCEPTION_PERCENTAGE, DELAY_PERCENTAGE, DELAY_MAX, KEEP_ALIVE_INTERVAL, KEEP_ALIVE, LOCAL_MODEL_PATH, \
    DEFAULT_MEMORY, COST_PER_MS_MB

from .job_policies import default_job_policy
from lithopserve.util.metrics import PrometheusExporter

logger = logging.getLogger()


class Job:
    '''
    Class that represents a job to be executed by the orchestrator
    :param input: list. The input data to be processed
    :param job_name: str. The name of the job
    :param bucket: str. The bucket where the input data is stored
    :param split_size: int. The size of the splits
    :param num_task_managers: int. The number of task managers to be used
    :param dynamic_split: bool. True if the split is dynamic, False if it is static
    :param ip: str. The IP address of the orchestrator
    :param task_manager_config: dict. The configuration of the task manager
    :param orchestrator_backend: str. The backend to be used by the orchestrator
    :param output_storage: str. The storage to be used for the output
    :param output_location: str. The location where the output will be stored
    :param output_bucket: str. The bucket where the output will be stored
    :param speculation_enabled: bool. True if speculative execution is enabled
    :param speculation_multiplier: int. The multiplier for speculative execution
    :param speculation_quartile: float. The quartile for speculative execution
    :param speculation_interval_ms: int. The interval for speculative execution
    :param max_split_retries: int. The maximum number of retries for a split
    '''

    def __init__(self,
                 input: list,
                 job_name: str = None,
                 bucket: str = None,
                 split_size: int = None,
                 num_task_managers: int = None,
                 dynamic_split: bool = DYNAMIC_SPLIT,
                 ip: str = None,
                 task_manager_config: dict = DEFAULT_TASK_MANAGER_CONFIG,
                 orchestrator_backend: str = DEFAULT_ORCHESTRATOR,
                 output_storage: str = None,
                 output_location: str = None,
                 output_bucket: str = None,
                 speculation_enabled: bool = SPECULATIVE_EXECUTION,
                 speculation_multiplier: int = SPECULATION_MULTIPLIER,
                 speculation_quartile: float = SPECULATION_QUARTILE,
                 speculation_interval_ms: int = SPECULATION_INTERVAL_MS,
                 speculation_add_task_manager: bool = SPECULATION_ADD_TASK_MANAGER,
                 max_split_retries: int = MAX_SPLIT_RETRIES,
                 exception_percentage: int = EXCEPTION_PERCENTAGE,
                 delay_percentage: int = DELAY_PERCENTAGE,
                 delay_max: int = DELAY_MAX,
                 keep_alive: bool = KEEP_ALIVE,
                 keep_alive_interval: int = KEEP_ALIVE_INTERVAL,
                 local_model_path: str = LOCAL_MODEL_PATH,
                 get_cloudwatch_report=True,
                 local_function=None,
                 heterogeneous_splits = None,
                 memory: int = DEFAULT_MEMORY,
                 ):
        self.input = input
        if not job_name:
            self.job_name = str(time.strftime("%Y%m%d-%H%M%S"))
        else:
            self.job_name = job_name

        self.num_inputs = len(input)
        self.bucket = bucket
        self.split_size = split_size
        self.num_task_managers = num_task_managers
        self.output = None
        self.orchestrator_stats = {
            "created": time.time(),
            "assigned": None,
            "started": None,
            "finished": None
        }
        self.dynamic_split = dynamic_split
        self.ec2_metadata = None
        self.task_manager_config = task_manager_config

        if orchestrator_backend not in ORCHESTRATOR_BACKENDS:
            self.orchestrator_backend = DEFAULT_ORCHESTRATOR
        else:
            self.orchestrator_backend = orchestrator_backend
            if orchestrator_backend == "local" and not ip:
                ip = DEFAULT_IP
        if not dynamic_split:
            ip = None
        self.ip = ip
        self.invoke_output = None

        if output_storage not in OUTPUT_STORAGE:
            self.output_storage = None
        else:
            self.output_storage = output_storage
        self.output_location = output_location
        self.output_bucket = output_bucket
        self.speculation_enabled = speculation_enabled
        self.speculation_multiplier = speculation_multiplier
        self.speculation_quartile = speculation_quartile
        self.speculation_interval = speculation_interval_ms / 1000
        self.speculation_add_task_manager = speculation_add_task_manager
        self.split_info = None
        self.max_split_retries = max_split_retries
        self.exception_percentage = exception_percentage
        self.delay_percentage = delay_percentage
        self.delay_max = delay_max
        self.extra_task_managers = 0
        self.extra_invoke_output = None
        self.local_model_path = local_model_path
        self.keep_alive = keep_alive
        self.keep_alive_interval = keep_alive_interval
        self.get_cloudwatch_report = get_cloudwatch_report
        # Check that if the orchestrator_backend is local, the local_function is not None
        self.local_function = local_function
        self.heterogeneous_splits = heterogeneous_splits
        self.memory = memory

    def to_dict(self):
        '''
        Returns the job as a dictionary
        :return: dict. The job as a dictionary
        '''
        return {
            "input": self.input,
            "job_name": self.job_name,
            "bucket": self.bucket,
            "split_size": self.split_size,
            "num_task_managers": self.num_task_managers,
            "extra_task_managers": self.extra_task_managers,
            "output": self.output,
            "orchestrator_stats": self.orchestrator_stats,
            "dynamic_split": self.dynamic_split,
            "ec2_metadata": self.ec2_metadata,
            "task_manager_config": self.task_manager_config,
            "orchestrator_backend": self.orchestrator_backend,
            "ip": self.ip,
            "output_storage": self.output_storage,
            "output_location": self.output_location,
            "output_bucket": self.output_bucket,
            "speculation_enabled": self.speculation_enabled,
            "speculation_multiplier": self.speculation_multiplier,
            "speculation_quartile": self.speculation_quartile,
            "speculation_interval": self.speculation_interval,
            "split_info": self.split_info,
            "max_split_retries": self.max_split_retries,
            "invoke_output": self.invoke_output,
            "extra_invoke_output": self.extra_invoke_output
        }

    def to_json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls(**data)


class ResourceProvisioner():
    '''
    Class that represents the resource provisioner for the orchestrator.
    :param job_policy: function. The job policy to be used by the orchestrator. It assigns the parameters to the job based on the input size.
                                 It receives the job, the maximum number of task managers and the maximum split size as parameters.
    :param max_job_managers: int. The maximum number of job managers to be used. The orchestrator will not exceed this number, it has the priority.
    :param max_task_managers: int. The maximum number of task managers. The orchestrator will not exceed this number, it has the priority.
    '''

    def __init__(self, job_policy, max_job_managers: int = 1, max_task_managers: int = 1):
        self.job_policy = job_policy
        self.max_job_managers = max_job_managers
        self.max_task_managers = max_task_managers
        self.count_job_managers = 0
        self.count_task_managers = 0

    def increment_count(self, num_task_managers):
        '''
        Increments the count of task managers by num_task_managers and job managers by 1
        :param num_task_managers: int. The number of task managers to be added
        '''
        self.count_task_managers += num_task_managers
        self.count_job_managers += 1
        logging.info(
            f"Incrementing count. Now {self.count_task_managers} task managers and {self.count_job_managers} job managers.")

    def decrement_count(self, num_task_managers):
        '''
        Decrements the count of task managers by num_task_managers and job managers by 1
        '''
        self.count_task_managers -= num_task_managers
        self.count_job_managers -= 1
        logging.info(
            f"Decrementing count. Now {self.count_task_managers} task managers and {self.count_job_managers} job managers.")

    def check_available_resources(self, job):
        '''
        Checks if there are available resources for the job
        :param job: Job. The job to be checked
        :return: bool. True if there are available resources, False otherwise
        '''
        logging.info(
            f"Available resources: ({self.count_task_managers}/{self.max_task_managers}) task managers and ({self.count_job_managers}/{self.max_job_managers}) job managers.")
        num_task_managers = job.num_task_managers
        if self.count_job_managers < self.max_job_managers and self.count_task_managers < self.max_task_managers:
            if self.count_task_managers + num_task_managers <= self.max_task_managers:
                logging.info(
                    f"Adding {num_task_managers} task managers will not exceed the maximum number of task managers {self.max_task_managers}.")
                return True
            else:
                logging.info(
                    f"Adding {num_task_managers} task managers will exceed the maximum number of task managers {self.max_task_managers}.")
                return False
        else:
            logging.info(f"Maximum number of task managers {self.max_task_managers} reached. No available resources.")
            return False

    def correct_resources(self, job):
        '''
        Corrects the resources of the job if the number of task managers exceeds the maximum number of task managers.
        Also corrects the number of task managers if it is greater than the number of inputs.
        :param job: Job. The job to be corrected
        :return: Job. The job with the corrected resources
        '''
        if self.count_task_managers + job.num_task_managers > self.max_task_managers:
            job.num_task_managers = self.max_task_managers - self.count_task_managers
            logger.info(f"Correcting resources. Using {job.num_task_managers} task managers.")

        if not job.num_task_managers:
            job.num_task_managers = 1

        if not job.split_size:
            job.split_size = 1

        if job.num_task_managers > job.num_inputs:
            job.num_task_managers = job.num_inputs
            logger.info(f"Correcting resources. Using {job.num_task_managers} task managers.")
        return job

    def apply_job_policy(self, job: Job):
        '''
        Assigns the parameters to the job based on the input size
        :param job: Job. The job to be assigned the parameters
        :return: Job. The job with the parameters assigned
        '''
        if not job.num_task_managers:
            job.num_task_managers = DEFAULT_TASK_MANAGERS
        if not job.split_size:
            job.split_size = DEFAULT_SPLIT_SIZE
        job = self.job_policy(job)
        return job

    def get_cloudwatch_report(self, fexec: FunctionExecutor, job):
        session = fexec.compute_handler.backend.aws_session
        logs_client = session.client('logs', region_name='eu-west-1')
        invoke_stats = job.output['lithops_stats']
        if invoke_stats:
            runtime_name = fexec.compute_handler.config['aws_lambda']['runtime']
            runtime_memory = fexec.compute_handler.config['aws_lambda']['runtime_memory']
            function_name = fexec.compute_handler.backend._format_function_name(runtime_name=runtime_name,
                                                                                runtime_memory=runtime_memory)
            log_group_name = f'/aws/lambda/{function_name}'

            for i, stat in enumerate(invoke_stats):
                count = 0
                while 'log_message' not in invoke_stats[i] and count < 30:
                    activation_id = stat['activation_id']
                    start_time = stat['host_submit_tstamp']
                    end_time = stat['host_status_done_tstamp']
                    response = logs_client.filter_log_events(
                        logGroupName=log_group_name,
                        filterPattern=f"\"REPORT RequestId: {activation_id}\"",
                        startTime=int(start_time * 1000),
                        endTime=int(end_time * 1000),
                    )
                    if response['events']:
                        for event in response['events']:
                            message = event['message']
                            print(f"Log Message: {message}")

                            # Search for the billed duration in the log message
                            if activation_id in message:
                                # Add message to the invoke stats and update in job
                                invoke_stats[i]['log_message'] = message
                                break
                    else:
                        time.sleep(1)
                        count += 1
            job.output['lithops_stats'] = invoke_stats
        return job

    def lithops_call(self, fexec: FunctionExecutor, payloads: list, exception_str: bool = True):
        '''
        Calls map_async of lithops with the payloads
        :param fexec: FunctionExecutor. The lithops function executor
        :param payloads: list. The payloads to be sent to the task managers
        :param timeout: int. The timeout for the call
        :param exception_str: bool. True if the exception should be returned as a string, False otherwise
        :return: dict. The results of the task managers
        '''
        num_task_managers = len(payloads)
        try:
            payload_list = []
            for payload in payloads:
                payload_list.append({'payload': payload})
            logger.info(f"Calling {num_task_managers} task managers.")
            futures = fexec.map_async(map_iterdata=payload_list)
            return futures
        except Exception as e:
            if exception_str:
                try:
                    e = str(e)
                except Exception as e2:
                    pass
            logger.error(f"Error in lithops call: {e}")
            logger.error(
                f"Depending on the error, you may need to redeploy the runtime. Use the redeploy_runtime method of the orchestrator.")
            return e

    def local_call(self, fexec: FunctionExecutor, payloads: list, local_function=None):
        '''
        Calls a pool of local task managers with the payloads
        :param payloads: list. The payloads to be sent to the task managers
        :return: list. The results of the task managers
        '''
        num_task_managers = len(payloads)
        try:
            payload_list = []
            for payload in payloads:
                payload_list.append({'payload': payload})
            logger.info(f"Calling {num_task_managers} task managers.")
            futures = fexec.map(map_function=local_function, map_iterdata=payload_list)
            return futures
        except Exception as e:
            try:
                e = str(e)
            except Exception as e2:
                pass
            logger.error(f"Error in lithops call: {e}")
            logger.error(
                f"Depending on the error, you may need to redeploy the runtime. Use the redeploy_runtime method of the orchestrator.")
            return e

    def wait_futures(self, fexec: FunctionExecutor, futures: list, timeout: int = None,
                     exception_str: bool = True):
        '''
        Waits for the futures of the task managers.
        :param fexec: FunctionExecutor. The function executor to be used
        :param futures: list. The futures of the task managers
        :param timeout: int. The timeout for the call
        :param exception_str: bool. True if the exception should be returned as a string, False otherwise
        '''
        num_task_managers = len(futures)
        results = []
        try:
            results = fexec.get_result(futures, timeout=timeout)
            logger.info(f"Finished {num_task_managers} task managers")
            try:
                stats = fexec.stats(futures)

            except Exception as e:
                stats = None
                logger.error(f"Error getting stats: {e}")
            lithops_results = {'lithops_results': results, 'lithops_stats': stats, 'lithops_config': fexec.config}
        except Exception as e:
            if exception_str:
                e = str(e)
            lithops_results = {'lithops_results': results, 'lithops_stats': None, 'lithops_config': fexec.config,
                               'error': e}
            logger.error(f"Error in lithops call: {e}")
            logger.error(
                f"Depending on the error, you may need to redeploy the runtime. Use the redeploy_runtime method of the orchestrator.")
        return lithops_results

    def call(self, fexec, payloads: list, local_function=None, exception_str: bool = True):
        '''
        Calls the function executor with the payloads. If the function executor is not provided, it calls the local function.
        :param fexec: FunctionExecutor. The function executor to be used
        '''
        num_task_managers = len(payloads)
        try:
            payload_list = []
            for payload in payloads:
                payload_list.append({'payload': payload})
            logger.info(f"Calling {num_task_managers} task managers.")
            if fexec.backend == 'localhost':
                futures = fexec.map(map_function=local_function, map_iterdata=payload_list)
            else:
                futures = fexec.map_async(map_iterdata=payload_list)
            return futures
        except Exception as e:
            if exception_str:
                try:
                    e = str(e)
                except Exception as e2:
                    pass
            logger.error(f"Error in lithops call: {e}")
            logger.error(
                f"Depending on the error, you may need to redeploy the runtime. Use the redeploy_runtime method of the orchestrator.")
            return e

    def save_output(self, fexec, job):
        '''
        Saves the output of the job to the output storage
        :param fexec: FunctionExecutor. The function executor to be used
        :param job: Job. The job to be saved
        :return: bool. True if the output was saved, False otherwise
        '''
        try:
            if job.output_storage == "s3" and fexec:
                logger.info(f"Saving output to s3 bucket {job.bucket} with key {job.output_location}.json")
                json_string = json.dumps(job.to_dict())
                fexec.storage.put_object(bucket=fexec.storage.bucket, key=f"{job.output_location}.json",
                                         body=json_string)
                return True
            elif job.output_storage == "local":
                logger.info(f"Saving output to local file {job.output_location}.json")
                # Create directory if it does not exist
                os.makedirs(os.path.dirname(job.output_location), exist_ok=True)
                with open(f"{job.output_location}.json", "w") as file:
                    json.dump(job.to_dict(), file, indent=4)
                return True
        except Exception as e:
            logger.error(f"Error saving output: {e}")
            return False

        logger.info(f"No Output storage was declared. Output not saved, but printed as json. ")
        # Print the json to the logger
        logger.info(json.dumps(job.to_dict()))
        return True

    def get_ec2_metadata(self):
        '''
        ONLY USE IF INSIDE AN EC2 MACHINE
        Retrieves the metadata of the EC2 instance. Uses the EC2 instance metadata service
        https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
        :return: dict. The metadata of the EC2 instance
        '''
        import urllib.request
        def get_instance_metadata(metadata_path):
            try:
                # Make a GET request to the EC2 instance metadata service
                response = urllib.request.urlopen(metadata_path)

                # Decode the response
                metadata = response.read().decode()

                # Return the metadata as a string
                return metadata.strip()
            except Exception as e:
                logger.error(f"An error occurred while retrieving metadata: {e}")
                return None

        ec_2_metadata = {}
        ec_2_metadata['instance_id'] = get_instance_metadata(f'{EC2_METADATA_SERVICE}/instance-id')
        ec_2_metadata['mac'] = get_instance_metadata(f'{EC2_METADATA_SERVICE}/mac')
        ec_2_metadata['subnet_id'] = get_instance_metadata(
            f'{EC2_METADATA_SERVICE}/network/interfaces/macs/{ec_2_metadata["mac"]}/subnet-id')
        ec_2_metadata['security_group_name'] = get_instance_metadata(
            f'{EC2_METADATA_SERVICE}/security-groups')
        ec_2_metadata['security_group_id'] = get_instance_metadata(
            f'{EC2_METADATA_SERVICE}/network/interfaces/macs/{ec_2_metadata["mac"]}/security-group-ids')
        ec_2_metadata['ip'] = get_instance_metadata(f'{EC2_METADATA_SERVICE}/local-ipv4')
        logger.info(f"EC2 metadata: {ec_2_metadata}")
        return ec_2_metadata


class Split():
    '''
    Class that represents a split to be executed by the orchestrator.
    :param SID: str. The split ID
    :param split: list. The split to be executed
    :param TID: str. The task manager ID
    :param start_time: float. The time when the split started
    :param end_time: float. The time when the split ended
    :param output: dict. The output of the split
    :param retries_count: int. The number of retries of the split
    '''

    def __init__(self, SID: str, split: list):
        self.SID = SID
        self.split = split
        self.TID = None
        self.start_time = None
        self.end_time = None
        self.output = {}
        self.retries_count = 0

    def to_dict(self):
        '''
        Returns the split as a dictionary
        :return: dict. The split as a dictionary
        '''
        return {
            "SID": self.SID,
            "split": self.split,
            "TID": self.TID,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "output": self.output,
            "retries_count": self.retries_count
        }

    def to_dict_reduced(self):
        '''
        Returns the split as a dictionary with reduced information
        :return: dict. The split as a dictionary
        '''
        return {
            "SID": self.SID,
            "TID": self.TID,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "retries_count": self.retries_count
        }


class ThreadSafeDict:
    '''
    Class that represents a thread-safe dictionary.
    '''

    def __init__(self):
        self.dict = {}
        self.lock = threading.Lock()

    def update(self, key, value):
        '''
        Updates the dictionary with the key and value. If key does not exist, it is created.
        :param key: str. The key to be updated
        :param value: any. The value to be updated
        '''
        with self.lock:
            self.dict[key] = value

    def get(self, key):
        '''
        Gets the value of the key
        :param key: str. The key to get the value
        :return: any. The value of the key
        '''
        with self.lock:
            return self.dict.get(key)

    def remove(self, key):
        '''
        Removes the key from the dictionary.
        :param key: str. The key to be removed
        '''
        with self.lock:
            if key in self.dict:
                del self.dict[key]

    def pop(self, key):
        '''
        Pops the key from the dictionary.
        :param key: str. The key to be popped
        :return: any. The value of the key
        '''
        with self.lock:
            return self.dict.pop(key)

    def sort(self):
        '''
        Sorts the dictionary by the values.
        '''
        try:
            with self.lock:
                self.dict = dict(sorted(self.dict.items(), key=lambda item: item[1]))
        except Exception as e:
            logger.error(f"Error sorting dictionary: {e}")

    def to_list(self):
        '''
        Returns the dictionary as a list.
        :return: list. The dictionary as a list
        '''
        with self.lock:
            return list(self.dict)

    def keys(self):
        '''
        Returns the keys of the dictionary.
        :return: list. The keys of the dictionary
        '''
        with self.lock:
            return self.dict.keys()

    def empty(self):
        '''
        Returns True if the dictionary is empty, False otherwise.
        :return: bool. True if the dictionary is empty, False otherwise
        '''
        with self.lock:
            return not bool(self.dict)

    def length(self):
        '''
        Returns the length of the dictionary.
        :return: int. The length of the dictionary
        '''
        with self.lock:
            return len(self.dict)

    def has(self, key):
        '''
        Returns True if the key is in the dictionary, False otherwise.
        '''
        with self.lock:
            return key in self.dict


class ThreadSafeList():
    def __init__(self):
        # initialize the list
        self._list = list()
        # initialize the lock
        self._lock = threading.Lock()

    # add a value to the list
    def append(self, value):
        # acquire the lock
        with self._lock:
            # append the value
            self._list.append(value)

    # remove and return the last value from the list
    def pop(self):
        # acquire the lock
        with self._lock:
            # pop a value from the list
            return self._list.pop()

    # read a value from the list at an index
    def get(self, index):
        # acquire the lock
        with self._lock:
            # read a value at the index
            return self._list[index]

    # return the number of items in the list
    def length(self):
        # acquire the lock
        with self._lock:
            return len(self._list)

    def empty(self):
        # acquire the lock
        with self._lock:
            return not bool(self._list)

    def extend(self, values):
        # acquire the lock
        with self._lock:
            self._list.extend(values)

    def to_list(self):
        # acquire the lock
        with self._lock:
            return self._list


class Speculator():
    '''
    Class that represents the speculator for the orchestrator.
    :param job_manager: JobManager. The job manager to be used
    :param speculation_multiplier: int. The multiplier for speculative execution
    :param speculation_quartile: float. The quartile for speculative execution
    :param speculation_interval: int. The interval for speculative execution
    '''

    def __init__(self, job_manager, speculation_multiplier, speculation_quartile, speculation_interval):
        self.job_manager = job_manager
        self.speculation_multiplier = speculation_multiplier
        self.speculation_quartile = speculation_quartile
        self.speculation_interval = speculation_interval
        self.speculation_thread = None
        self.tid_check_dict = ThreadSafeDict()
        for tid in range(self.job_manager.job.num_task_managers):
            self.tid_check_dict.update(str(tid + 1), 0)

    def speculate(self):
        """
        Speculates that a TID is taking too long and adds the split back to the pending queue.
        Every speculation_interval seconds:
        1- Calculates the median time of execution of the done splits.
        2- Orders the tid_check_dict by the time of the last check.
        3- Checks if the time of the last check is greater than the median time of execution multiplied by the speculation_multiplier.
        4- If it is, speculates that the TID is taking too long. If the split is not a copy, creates the copy and adds it to the pending queue.
        5- Checks if all splits are done. If they are, stops the speculation.
        """
        logger.info("Starting speculation.")
        while True:
            # Get the times of execution of the done splits
            tid_times = []
            for sid in self.job_manager.split_controller.done_splits.to_list():
                split = self.job_manager.split_controller.done_splits.get(sid)
                tid = split.TID
                start_time = split.start_time
                end_time = split.end_time
                tid_times.append((end_time - start_time))

            # Get median time of execution
            median_time = sorted(tid_times)[len(tid_times) // 2]

            # Order the tid_check_dict by the time of the last check
            self.tid_check_dict.sort()
            tids = self.tid_check_dict.to_list()

            # For each TID
            for tid in tids:
                tid_last_time = self.tid_check_dict.get(tid)
                if tid_last_time:
                    current_tid_time = time.time() - tid_last_time
                    # Check if the time of the last check is greater than the median time of execution multiplied by the speculation_multiplier
                    if current_tid_time > median_time * self.speculation_multiplier:
                        logger.info(f"Speculating that TID {tid} is taking too long. Time: {current_tid_time} seconds")
                        sid = self.job_manager.split_controller.find_sid_with_tid(tid)
                        if sid and sid.isdigit():
                            split_copy = self.job_manager.split_controller.create_copy(sid)
                            if split_copy:
                                self.job_manager.split_controller.put_split_pending(split_copy)
                                if self.job_manager.job.speculation_add_task_manager and split_copy.retries_count <= 1:
                                    self.job_manager.add_extra_task_executors(1)
                            else:
                                logger.debug(f"Split reached maximum retries.")

            if self.job_manager.split_controller.finished_all_splits():
                # Print the SIDs of the done splits
                logger.info(f"All splits are done: {self.job_manager.split_controller.done_splits.to_list()}")
                logger.info(f"Stopping speculation. Now waiting for Task Managers to finish...")
                break
            else:
                # There are still splits to be done
                sids = self.job_manager.split_controller.running_splits.to_list()
                logger.debug(f"Running splits now: {sids}")
            time.sleep(self.speculation_interval)

    def speculator_can_start(self):
        '''
        Returns True if the speculator can start, False otherwise.
        :return: bool. True if the speculator can start, False otherwise
        '''
        if self.speculation_thread is None:
            num_done_splits = self.job_manager.split_controller.done_splits.length()
            num_splits = len(self.job_manager.split_controller.splits)
            logger.debug(f"{num_done_splits / num_splits} currently to get {self.speculation_quartile} quartile.")
            if num_done_splits / num_splits > self.speculation_quartile:
                logger.info(
                    f"{num_done_splits}/{num_splits} splits are above the {self.speculation_quartile} quartile. Starting to speculate.")
                return True
            else:
                return False
        else:
            return False

    def start_speculator_thread(self):
        '''
        Starts the speculator thread if the number of done splits is above the speculation_quartile.
        '''
        self.speculation_thread = threading.Thread(target=self.speculate)
        self.speculation_thread.start()


class SplitEnumerator(SPLITRPCServicer):
    '''
    Class that represents the split enumerator for the orchestrator.
    :param job_manager: JobManager. The job manager to be used
    '''

    def __init__(self, job_manager):
        """
        Constructor for the SplitEnumerator class
        """
        self.job_manager = job_manager
        self.speculator = None
        if self.job_manager.job.speculation_enabled:
            self.speculator = Speculator(job_manager, job_manager.job.speculation_multiplier,
                                         job_manager.job.speculation_quartile, job_manager.job.speculation_interval)
        self.prom = job_manager.prom

    def get_outputs_request(self, request):
        '''
        Gets the outputs from the request.
        :param request: urlResponse. The request to get the outputs
        :return: dict. The outputs of the request
        '''
        outputs = {}
        # Get results from the request
        for result in request.outputs:
            try:
                result_value_dict = eval(result.value)
            except Exception as e:
                result_value_dict = result.value
            if result.key:
                outputs.update({result.key: result_value_dict})
        return outputs

    def pop_running_split(self, request_sid, outputs):
        '''
        Ends the running split by poping it from the running splits.
        :param request_sid: str. The SID of the request
        :param outputs: dict. The outputs of the request
        :return: Split. The split that was ended
        '''
        try:
            logger.debug(f"Poping request SID {request_sid} from the running splits.")
            done_split = self.job_manager.split_controller.running_splits.pop(request_sid)
        except Exception as e:
            logger.debug(f"Request SID {request_sid} not found in the running splits.")
            return None

        done_split.end_time = time.time()
        done_split.output = outputs
        return done_split

    def conclude_split(self, request_sid, done_split):
        '''
        Handles the split by adding it to the done splits.
        :param request_sid: str. The SID of the request
        :param done_split: Split. The split that was done
        '''
        # If it is a copy of another split
        if "_" in request_sid:
            # Get the original SID
            original_sid = request_sid.split("_")[0]
            # Get the original split
            original_split = self.job_manager.split_controller.running_splits.get(original_sid)
            # If the original slit is still running
            if original_split:
                logger.info(
                    f"The copy of split {original_sid} with SID {request_sid} is done before the original. Time: {done_split.end_time - done_split.start_time} seconds.")
                with self.job_manager.split_controller.running_splits.lock:
                    original_split.retries_count = self.job_manager.split_controller.max_split_retries
                done_split.SID = original_sid
                self.job_manager.split_controller.done_splits.update(original_sid, done_split)
            else:
                # If the original split is not running, it is because it was already done before
                logger.info(f"Original split {original_sid} finished before the copy.")
        else:
            # If the split is not a copy, add it to the done splits
            self.job_manager.split_controller.done_splits.update(request_sid, done_split)
            logger.info(f"Split {request_sid} done. Time: {done_split.end_time - done_split.start_time} seconds")

    def Assign(self, request, context):
        '''
        Assigns the split to the task manager.
        :param request: urlResponse. The request to be assigned
        :param context: grpc context. The context of the request
        :return: urlResponse. The response of the request
        '''
        if self.speculator:
            self.speculator.tid_check_dict.update(request.tid, time.time())
        keep_alive = request.keep_alive
        if keep_alive:
            logger.debug(f"Keep alive from TID {request.tid}")
            return splitResponse(inputs=[], sid=None)

        # If the request carries a finished split
        sids = self.job_manager.split_controller.running_splits.to_list()
        logger.debug(f"Running splits when requested: {sids}")

        # If the request carries outputs
        if request.outputs and request.sid:
            outputs = self.get_outputs_request(request)
            done_split = self.pop_running_split(request.sid, outputs)
            if done_split:
                self.prom.send_metric(
                    name='current_batch',
                    value=0,
                    type='gauge',
                    labels=(
                        ('task_manager', f"{int(request.tid):03d}"),
                    )
                )

                self.prom.send_metric(
                    name='batch_finished',
                    value=time.time(),
                    type='gauge',
                    labels=(
                        ('batch_id', done_split.SID),
                        ('task_manager', f"{int(request.tid):03d}"),
                    )
                )

                self.prom.send_metric(
                    name='batch_summary',
                    value=1,  # or 0, value doesn't matter for metadata
                    type='gauge',
                    labels=(
                        ('batch_id', done_split.SID),
                        ('task_manager', f"{int(request.tid):03d}"),
                        ('start_time', str(done_split.start_time)),
                        ('end_time', str(done_split.end_time)),
                    )
                )

                # Save the output of the split
                self.conclude_split(request.sid, done_split)

        # If there is speculator
        if self.speculator and request.tid:
            # Update the time of the last check of the TID
            self.speculator.tid_check_dict.update(request.tid, time.time())
            if self.speculator.speculator_can_start():
                # If the speculator can start, start the speculator thread
                self.speculator.start_speculator_thread()

        # Get next split from the split controller
        split = self.job_manager.split_controller.get_next_split()

        # Put split in running state if there is a split
        split = self.job_manager.split_controller.put_split_running(split, request.tid)

        # If there is a split to run, send it to the task manager
        if split:
            self.prom.send_metric(
                name='split_inputs_count',
                value=len(split.split),
                type='gauge',
                labels=(
                    ('batch_id', split.SID),
                    ('task_manager', f"{int(request.tid):03d}"),
                )
            )

            self.prom.send_metric(
                name='batch_started',
                value=time.time(),
                type='gauge',
                labels=(
                    ('batch_id', split.SID),
                    ('task_manager', f"{int(request.tid):03d}"),
                )
            )

            self.prom.send_metric(
                name='current_batch',
                value=int(split.SID),
                type='gauge',
                labels=(
                    ('task_manager', f"{int(request.tid):03d}"),
                    # ('batch_size', str(len(split.split)))
                )
            )

            logger.info(f"Sending split {split.SID} to task manager {request.tid}")
            print(split.split)
            return splitResponse(inputs=split.split, sid=split.SID)
        else:
            # If there are no more splits to run, end the task manager
            self.prom.delete_metric(
                name='current_batch',
                labels=(
                    ('task_manager', f"{int(request.tid):03d}"),
                )
            )

            logger.info(f"Ending task manager {request.tid}")
            if self.speculator:
                self.speculator.tid_check_dict.remove(request.tid)
                tids = self.speculator.tid_check_dict.to_list()
                logger.debug(f"Running Task Managers: {tids}")
            return splitResponse(inputs=[], sid=None)


class SplitController():
    '''
    Class with the structures and operations to handle the three states of the splits: pending, running and done.
    :param job_input: list. The input data to be processed
    :param split_size: int. The size of the splits
    :param max_split_retries: int. The maximum number of retries for a split
    '''

    def __init__(self, job_input, split_size, max_split_retries, prom:PrometheusExporter, heterogeneous_splits=None):
        self.pending_queue = Queue()
        self.running_splits = ThreadSafeDict()
        self.done_splits = ThreadSafeDict()
        split_inputs = [job_input[i:i + split_size] for i in range(0, len(job_input), split_size)]
        self.splits = [Split(str(i + 1), split) for i, split in enumerate(split_inputs)]

        if heterogeneous_splits:
            self.splits = [Split(str(i + 1), split) for i, split in enumerate(heterogeneous_splits)]

        self.split_size = split_size
        self.max_split_retries = max_split_retries
        logger.info(f"Created {len(self.splits)} splits with size {split_size}")
        sids = [split.SID for split in self.splits]
        logger.debug(f"Splits: {sids}")

        self.prom = prom

    def initialize_pending_queue(self):
        '''
        Initializes the pending queue with the splits.
        '''
        for split in self.splits:
            self.pending_queue.put(split)

    def has_pending_or_running_splits(self):
        '''
        Returns True if there are pending or running splits, False otherwise.
        '''
        return not (self.pending_queue.empty() and self.running_splits.empty())

    def finished_all_splits(self):
        '''
        Returns True if all splits are done, False otherwise.
        '''
        done_splits = self.done_splits.length()
        if done_splits == len(self.splits):
            logger.info(f"All splits finished. Done splits: {done_splits}/{len(self.splits)}")
            return True

    def create_copy(self, sid):
        '''
        Creates a copy of a split if it is already in the running splits. If the split has already been retried the maximum number of times, it returns None.
        :param sid: str. The SID of the split
        :return: Split. The copy of the split. None if the split has already been retried the maximum number of times.
        '''
        split = self.running_splits.get(sid)
        current_retries_count = split.retries_count
        # If the split exists and has not been retried the maximum number of times
        if split and current_retries_count < self.max_split_retries:
            split.retries_count += 1
            current_retries_count = split.retries_count
            # Create a new SID for the copy.
            new_sid = f"{str(int(split.SID))}_{current_retries_count}"
            logger.info(
                f"Split {split.SID} is already in the running splits. Creating a copy of the split with SID {new_sid} (retry {split.retries_count}).")
            new_split = Split(new_sid, split.split)
            new_split.retries_count = current_retries_count
            return new_split
        else:
            return None

    def order_running_splits(self):
        '''
        Orders the running splits by the start time.
        '''
        start = time.time()
        with self.running_splits.lock:
            running_splits_list = [(sid, split) for sid, split in self.running_splits.dict.items()]
            sorted_running_splits_list = sorted(running_splits_list, key=lambda x: x[1].start_time)
            sorted_running_splits_dict = {sid: split for sid, split in sorted_running_splits_list}
            self.running_splits.dict = sorted_running_splits_dict
        end = time.time()
        logger.debug(f"Ordered running splits in {end - start} seconds.")
        pass

    def get_next_split(self):
        '''

        '''
        if self.finished_all_splits() or self.pending_queue.empty():
            return None
        else:
            split = self.pending_queue.get()
            return split

    def put_split_running(self, split, tid):
        if split:
            logger.debug(f"Split {split.SID} is not in the running splits. Adding it to the running splits.")
            split.TID = tid
            split.start_time = time.time()
            self.running_splits.update(split.SID, split)
            return split
        else:
            return None

    def put_split_pending(self, split):
        if split:
            logger.debug(f"Adding {split.SID} to the pending queue.")
            self.pending_queue.put(split)
            return split
        else:
            return None

    def find_sid_with_tid(self, tid):
        for sid in self.running_splits.to_list():
            split = self.running_splits.get(sid)
            with self.running_splits.lock:
                if split.TID == tid:
                    return sid
        return None

    def move_split_to_pending(self, sid):
        split = self.running_splits.get(sid)
        self.running_splits.remove(sid)
        self.pending_queue.put(split)
        logger.info(f"Split {sid} moved from running to pending queue")


class JobManager:
    def __init__(self,
                 fexec_args: dict,
                 job: Job,
                 resource_provisioner: ResourceProvisioner,
                 split_enumerator_thread_pool_size: int,
                 split_enumerator_port: int,
                 ):
        self.fexec_args = fexec_args
        self.job = job
        self.resource_provisioner = resource_provisioner
        self.split_enumerator_thread_pool_size = split_enumerator_thread_pool_size
        self.split_enumerator_port = split_enumerator_port
        self.split_controller = None
        self.server_thread = None
        self.running_server_flag = None
        self.stop_server_flag = None
        self.extra_futures = None
        self.fexec = None
        self.prom = None
        self.alive_check_table = None

    def serve(self):
        """
        Starts the gRPC server and waits for the stop_server_flag to be set.
        This method is to be called by the start_server method, using a separate thread.
        """
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.split_enumerator_thread_pool_size))
        add_SPLITRPCServicer_to_server(SplitEnumerator(self), server)
        logger.info(f"Starting gRPC server at {self.job.ip}:0")
        self.split_enumerator_port = server.add_insecure_port(f'{self.job.ip}:0')
        logger.info(f"gRPC server started at {self.job.ip}:{self.split_enumerator_port}")
        server.start()
        self.running_server_flag.set()
        self.stop_server_flag.wait()
        server.stop(0)

    def start_server(self):
        """
        Starts the gRPC server and waits for the server_thread to finish.
        """
        self.running_server_flag = threading.Event()
        self.stop_server_flag = threading.Event()
        self.server_thread = Thread(target=self.serve, args=())
        self.server_thread.start()
        self.running_server_flag.wait()

    def close_server(self):
        """ Stops the gRPC server and waits for the server_thread to finish."""
        self.stop_server_flag.set()
        self.server_thread.join()

    def build_dynamic_payload(self):
        """
        Builds the payload for the dynamic split.
        The payload is a list of dictionaries, each dictionary containing the information for a task manager.
        The information includes the task manager ID, the IP, the port, the configuration, the flag to raise an exception and the delay for the task manager.
        """
        tids = list(range(self.job.num_task_managers))
        num_trues = int(len(tids) * self.job.exception_percentage / 100)
        random_list = [True] * num_trues + [False] * (len(tids) - num_trues)
        random.shuffle(random_list)

        num_trues = int(len(tids) * self.job.delay_percentage / 100)
        delay_list = [random.randint(0, self.job.delay_max) for i in range(num_trues)] + [0] * (len(tids) - num_trues)
        random.shuffle(delay_list)

        payload_list = []
        for i in range(len(tids)):
            payload = {'body':
                {
                    'tid': str(tids[i] + 1),
                    'ip': self.job.ip,
                    'port': self.split_enumerator_port,
                    'keep_alive': self.job.keep_alive,
                    'keep_alive_interval': self.job.keep_alive_interval,
                    'config': self.job.task_manager_config,
                    'to_raise_exception': random_list[i],
                    'to_delay': delay_list[i]
                }
            }
            if self.job.orchestrator_backend != "local":
                payload['body']['bucket'] = self.job.bucket
            payload_list.append(payload)
        return payload_list

    def build_static_payload(self):
        """
        Builds the payload for the static split.
        The payload is a list of dictionaries, each dictionary containing the inputs for each task manager.
        """

        def divide_dict_into_chunks(items, num_chunks):
            chunk_size = len(items) // num_chunks
            remainder = len(items) % num_chunks
            chunks = []
            start = 0
            for i in range(num_chunks):
                end = start + chunk_size
                if i < remainder:
                    end += 1
                chunks.append(items[start:end])
                start = end
            return chunks

        dataset = self.job.input
        payload_list = []
        splits = divide_dict_into_chunks(dataset, self.job.num_task_managers)
        for split in splits:
            payload = {'body':
                {
                    'split': split,
                    'config': self.job.task_manager_config,
                }
            }
            if self.job.orchestrator_backend != "local":
                payload['body']['bucket'] = self.job.bucket
            payload_list.append(payload)
        return payload_list

    def build_payload(self):
        """
        Builds the payload for the job. If the job is dynamic, it builds the dynamic payload.
        If the job is static, it builds the static payload.
        """
        if self.job.dynamic_split:
            return self.build_dynamic_payload()
        else:
            return self.build_static_payload()

    def splits_outputs(self):
        """
        Returns the outputs of the splits as a dictionary.
        """
        all_outputs = {}
        sids = self.split_controller.done_splits.to_list()
        logger.info(f"Done splits: {sids}")
        for sid in sids:
            split_outputs = self.split_controller.done_splits.get(sid).output
            for output in split_outputs:
                all_outputs.update({output: split_outputs[output]})
        # Assign the inputs that were not split to the output with None as value
        for input in self.job.input:
            if input not in all_outputs:
                all_outputs.update({input: None})
        return all_outputs

    def splits_times(self):
        """
        Returns the start and end times of the splits as a dictionary.
        """
        times = {}
        for sid in self.split_controller.done_splits.to_list():
            split = self.split_controller.done_splits.get(sid)
            times.update({split: {"start_time": split.start_time, "end_time": split.end_time}})
        return times

    def initialize_dynamic_job(self):
        """
        Initializes the dynamic job. It sets the IP of the job and initializes the split controller and the extra futures.
        """
        if self.job.ec2_metadata:
            self.job.ip = self.job.ec2_metadata["ip"]
        if not self.job.ip:
            self.job.ip = DEFAULT_IP
        self.split_controller = SplitController(self.job.input, self.job.split_size, self.job.max_split_retries, self.prom, self.job.heterogeneous_splits )
        self.extra_futures = ThreadSafeList()
        self.split_controller.initialize_pending_queue()
        self.start_server()

    def close_dynamic_job(self, invoke_output):
        """
        Closes the dynamic job. It stops the server, gets the outputs of the splits, the times of the splits,
        and the split info.
        """
        self.close_server()
        self.job.output = invoke_output
        self.job.split_times = self.splits_times()
        self.job.split_info = [self.split_controller.done_splits.get(sid).to_dict_reduced() for sid in
                               self.split_controller.done_splits.to_list()]
        self.job.invoke_output = self.splits_outputs()

    def add_extra_task_executors(self, num_extra_task_managers):
        """
        Adds extra task managers to the job updates and adds the futures of the extra task managers to the extra futures.
        """
        logger.info(f"Adding {num_extra_task_managers} extra task managers.")
        logger.info(
            f"New TIDs: {list(range(self.job.num_task_managers + 1, self.job.num_task_managers + num_extra_task_managers + 1))}")
        payload_list = []
        for tid in range(self.job.num_task_managers, self.job.num_task_managers + num_extra_task_managers):
            payload = {'body':
                {
                    'tid': str(tid + 1),
                    'ip': self.job.ip,
                    'port': self.split_enumerator_port,
                    'config': self.job.task_manager_config
                }
            }
            if self.job.orchestrator_backend != "local":
                payload['body']['bucket'] = self.job.bucket
            payload_list.append(payload)
        futures = self.resource_provisioner.call(self.fexec, payload_list, self.job.local_function)
        self.job.num_task_managers += num_extra_task_managers
        self.extra_futures.extend(futures)
        logger.info(f"Extra task managers added.")

    def wait_extra_task_executors(self):
        logger.info(f"Waiting for extra task managers to finish.")
        list_of_futures = self.extra_futures.to_list()
        invoke_output = self.resource_provisioner.wait_futures(self.fexec, list_of_futures)
        logger.info(f"Extra task managers finished.")
        self.job.extra_task_managers = len(list_of_futures)
        return invoke_output

    def run(self):
        self.job.orchestrator_stats["assigned"] = time.time()

        self.fexec = FunctionExecutor(**self.fexec_args)
        self.prom = self.fexec.invoker.prometheus

        if self.job.dynamic_split: self.initialize_dynamic_job()


        if self.job.orchestrator_backend != "local":
            if not self.job.bucket:
                self.job.bucket = self.fexec.config['aws_s3']['storage_bucket']
        else:
            os.makedirs('/tmp/off_sample_orchestrator/', exist_ok=True)
            # split directory from file in local_model_path
            directory, filename = os.path.split(self.job.local_model_path)
            shutil.copyfile(self.job.local_model_path, f'/tmp/off_sample_orchestrator/{filename}')

        if self.prom:
            self.send_startup_metrics()

        payload = self.build_payload()
        self.job.orchestrator_stats["started"] = time.time()

        if not self.job.dynamic_split:
            self.send_static_metrics(payload)

        futures = self.resource_provisioner.call(self.fexec, payload, self.job.local_function)
        invoke_output = self.resource_provisioner.wait_futures(self.fexec, futures)

        if not self.job.dynamic_split:
            self.retrieve_static_metrics()

        if self.job.dynamic_split:
            if not self.extra_futures.empty():
                extra_invoke_output = self.wait_extra_task_executors()
                self.job.extra_invoke_output = extra_invoke_output

            self.close_dynamic_job(invoke_output)
        else:
            self.job.output = invoke_output

        self.job.orchestrator_stats["finished"] = time.time()
        if self.job.get_cloudwatch_report:
            self.job = self.resource_provisioner.get_cloudwatch_report(self.fexec, self.job)

        if self.prom:
            self.send_final_metrics()

        self.resource_provisioner.save_output(self.fexec, self.job)
        if self.job.orchestrator_backend == "local":
            os.remove(f'/tmp/off_sample_orchestrator/{filename}')
        return self.job.to_dict()


    def send_static_metrics(self, payload):
        self._static_batch_times = {}

        if not self.prom:
            return

        for tid, split_payload in enumerate(payload):
            batch_id = tid + 1
            split = split_payload['body']['split']
            start_time = time.time()
            self._static_batch_times[batch_id] = start_time

            self.prom.send_metric(
                name='split_inputs_count',
                value=len(split),
                type='gauge',
                labels=(
                    ('batch_id', batch_id),
                    ('task_manager', f"{int(batch_id):03d}"),
                )
            )
            self.prom.send_metric(
                name='batch_started',
                value=start_time,
                type='gauge',
                labels=(
                    ('batch_id', batch_id),
                    ('task_manager', f"{int(batch_id):03d}"),
                )
            )
            self.prom.send_metric(
                name='current_batch',
                value=batch_id,
                type='gauge',
                labels=(
                    ('task_manager', f"{int(batch_id):03d}"),
                )
            )

    def retrieve_static_metrics(self):
        """
        Sends Prometheus metrics for static batching after results are collected.
        """
        if not self.prom or not hasattr(self, '_static_batch_times'):
            return

        # Results are in self.job.output (set above: self.job.output = invoke_output)
        # Usually it's a dict: {'lithops_results': [...], ...}, but check for structure
        results = []
        if isinstance(self.job.output, dict) and 'lithops_results' in self.job.output:
            results = self.job.output['lithops_results']
        elif isinstance(self.job.output, list):  # Sometimes only results are returned
            results = self.job.output

        num_batches = len(self._static_batch_times)
        end_times = {}

        # Try to use accurate timing per batch if possible, otherwise use now
        for tid in range(num_batches):
            batch_id = tid + 1
            end_times[batch_id] = time.time()

        for batch_id, start_time in self._static_batch_times.items():
            end_time = end_times[batch_id]
            self.prom.send_metric(
                name='current_batch',
                value=0,
                type='gauge',
                labels=(
                    ('task_manager', f"{int(batch_id):03d}"),
                )
            )
            self.prom.send_metric(
                name='batch_finished',
                value=end_time,
                type='gauge',
                labels=(
                    ('batch_id', batch_id),
                    ('task_manager', f"{int(batch_id):03d}"),
                )
            )
            self.prom.send_metric(
                name='batch_summary',
                value=1,
                type='gauge',
                labels=(
                    ('batch_id', batch_id),
                    ('task_manager', f"{int(batch_id):03d}"),
                    ('start_time', str(start_time)),
                    ('end_time', str(end_time)),
                )
            )

            self.prom.delete_metric(
                name='current_batch',
                labels=(
                    ('task_manager', f"{int(batch_id):03d}"),
                )
            )

    def send_startup_metrics(self):
        """
        Sends the startup metrics to the Prometheus exporter.
        """
        self.prom.send_metric(name='job_start_timestamp',
                                value=self.job.orchestrator_stats['assigned'],
                                type='gauge',
                                labels=(('job_label', 'None'),)
                                )


        self.prom.send_metric(name='job_num_requests',
                              value=self.job.num_inputs,
                              type='gauge',
                              labels=(('job_label', 'None'),)
                              )

        self.prom.send_metric(name='job_batch_size',
                              value=self.job.split_size,
                              type='gauge',
                              labels=(('job_label', 'None'),)
                              )

        self.prom.send_metric(name='job_num_task_managers',
                              value=self.job.num_task_managers,
                              type='gauge',
                              labels=(('job_label', 'None'),)
                              )

        for i in range(self.job.num_task_managers):
            self.prom.send_metric(
                name='current_batch',
                value=-1,
                type='gauge',
                labels=(('task_manager', f"{i + 1:03d}"),)
            )
        num_splits = self.job.num_inputs // self.job.split_size if self.job.num_inputs % self.job.split_size == 0 else (self.job.num_inputs // self.job.split_size) + 1
        self.prom.send_metric(name='job_num_splits',
                                value=num_splits,
                                type='gauge',
                                labels=(('job_label', 'None'),)
                                )

        self.prom.send_metric(name='job_memory',
                                value=self.job.memory,
                                type='gauge',
                                labels=(('job_label', 'None'),)
                                )

        self.prom.send_metric(
            name='job_cost_per_ms_mb',
            value=COST_PER_MS_MB,  # Your constant
            type='gauge',
            labels=(('job_label', 'None'),)
        )



    def send_final_metrics(self):
        """
        Sends the final metrics to the Prometheus exporter.
        """
        lithops_stats = self.job.output['lithops_stats']

        init_durations = []
        exec_durations = []
        # For every task manager, get the startup timestamp
        for tid, stats in enumerate(lithops_stats):
            host_job_create_tstamp = stats['host_job_create_tstamp']
            worker_func_start_tstamp = stats['worker_func_start_tstamp']
            worker_func_end_tstamp = stats['worker_func_end_tstamp']
            host_status_done_tstamp = stats['host_status_done_tstamp']

            init_durations.append(worker_func_start_tstamp - host_job_create_tstamp)
            exec_durations.append(worker_func_end_tstamp - worker_func_start_tstamp)

            self.prom.send_metric(
                name='host_job_create_tstamp',
                value=host_job_create_tstamp,
                type='gauge',
                labels=(
                    ('task_manager', f"{tid+1:03d}"),
                )
            )

            self.prom.send_metric(
                name='worker_func_start_tstamp',
                value=worker_func_start_tstamp,
                type='gauge',
                labels=(
                    ('task_manager', f"{tid+1:03d}"),
                )
            )

            self.prom.send_metric(
                name='worker_func_end_tstamp',
                value=worker_func_end_tstamp,
                type='gauge',
                labels=(
                    ('task_manager', f"{tid+1:03d}"),
                )
            )

            self.prom.send_metric(
                name='host_status_done_tstamp',
                value=host_status_done_tstamp,
                type='gauge',
                labels=(
                    ('task_manager', f"{tid+1:03d}"),
                )
            )

        init_accumulated = sum(init_durations)
        cost_init = init_accumulated * 1000 * self.job.memory * COST_PER_MS_MB
        exec_accumulated = sum(exec_durations)
        cost_exec = exec_accumulated * 1000 * self.job.memory * COST_PER_MS_MB
        cost_job = cost_init + cost_exec

        job_latency =self.job.orchestrator_stats['finished'] - self.job.orchestrator_stats['started']
        job_throughput = self.job.num_inputs / job_latency if job_latency > 0 else 0
        job_throughput_per_cost = job_throughput / cost_job if cost_job > 0 else 0

        # Now send the job metrics in general
        self.prom.send_metric(name='job_latency',
                              value=job_latency,
                              type='gauge',
                              labels=(('job_label', 'None'),))

        self.prom.send_metric(name='job_throughput',
                              value=job_throughput,
                              type='gauge',
                              labels=(('job_label', 'None'),))

        self.prom.send_metric(name='job_throughput_per_cost',
                              value=job_throughput_per_cost,
                              type='gauge',
                              labels=(('job_label', 'None'),))

        self.prom.send_metric(name='job_init_duration',
                              value=init_accumulated,
                              type='gauge',
                              labels=(('job_label', 'None'),))

        self.prom.send_metric(name='job_exec_duration',
                              value=exec_accumulated,
                              type='gauge',
                              labels=(('job_label', 'None'),))

        self.prom.send_metric(name='job_total_duration',
                                value=init_accumulated + exec_accumulated,
                                type='gauge',
                                labels=(('job_label', 'None'),))

        self.prom.send_metric(name='job_cost_init',
                              value=cost_init,
                              type='gauge',
                              labels=(('job_label', 'None'),))

        self.prom.send_metric(name='job_cost_exec',
                              value=cost_exec,
                              type='gauge',
                              labels=(('job_label', 'None'),))

        self.prom.send_metric(name='job_cost',
                              value=cost_job,
                              type='gauge',
                              labels=(('job_label', 'None'),))

        self.prom.send_metric(name='job_finished_timestamp',
                                value=self.job.orchestrator_stats['finished'],
                                type='gauge',
                                labels=(('job_label', 'None'),)
                                )




class LearningPlane():
    '''
    LearningPlane WILL BE the intelligence behind the orchestrator.
    At the moment it only assigns the split_size and num_task_managers based on the input size.
    '''

    def __init__(self):
        pass

    def update(self):
        pass


class Orchestrator:
    """
    The Orchestrator is used to coordinate the Job Managers. It will enqueue the jobs and start the Job Managers.
    Important Parameters:
    :param fexec_args: dict. The arguments to be used in the FunctionExecutor7
    :param orchestrator_backends: list. The orchestrator backends to be used and initialized
    :param initialize: bool. If the orchestrator should be initialized
    :param job_policy: JobPolicy. The job policy to be used
    :param ec2_host_machine: bool. If the host machine is an EC2 instance
    :param max_job_managers: int. The maximum number of job managers to be used
    :param max_task_managers: int. The maximum number of task managers to be used

    Secondary Parameters:
    :param split_enumerator_thread_pool_size: int. The thread pool size for the split enumerator
    :param logging_level: int. The logging level
    :param job_pool_executor: Executor. The executor to be used in the job pool
    """

    def __init__(self,
                 fexec_args: dict = {},
                 orchestrator_backends: list = ORCHESTRATOR_BACKENDS,
                 initialize=True,
                 job_policy=default_job_policy,
                 ec2_host_machine: bool = EC2_HOST_MACHINE,
                 max_job_managers=MAX_JOB_MANAGERS,
                 max_task_managers=MAX_TASK_MANAGERS,
                 split_enumerator_thread_pool_size: int = SPLIT_ENUMERATOR_THREAD_POOL_SIZE,
                 logging_level=logging.INFO,
                 job_pool_executor=ProcessPoolExecutor(),
                 ):
        # Set the logger configuration
        logging.basicConfig(
            level=logging_level,
            format=LOGGING_FORMAT,
            handlers=[logging.StreamHandler()]
        )

        # check if orchestrator_backend is valid
        if all(backend not in ORCHESTRATOR_BACKENDS for backend in orchestrator_backends):
            raise ValueError(
                f"Invalid orchestrator_backend: {orchestrator_backends}. Must be of {ORCHESTRATOR_BACKENDS}")
        self.orchestrator_backends = orchestrator_backends

        # Check if the thread pool size is valid
        if split_enumerator_thread_pool_size < 1:
            raise ValueError(
                f"Invalid split_enumerator_thread_pool_size: {split_enumerator_thread_pool_size}. Must be greater than 0")
        self.split_enumerator_thread_pool_size = split_enumerator_thread_pool_size

        # Check if the max_job_managers is valid
        if max_job_managers < 1:
            raise ValueError(f"Invalid max_job_managers: {max_job_managers}. Must be greater than 0")

        # Check if the max_task_managers is valid
        if max_task_managers < 1:
            raise ValueError(f"Invalid max_task_managers: {max_task_managers}. Must be greater than 0")

        # Initialize the ResourceProvisioner
        self.resource_provisioner = ResourceProvisioner(job_policy, max_job_managers, max_task_managers)

        self.fexec_args = fexec_args
        self.ec_2_metadata = None
        if AWS_LAMBDA_BACKEND in orchestrator_backends:
            if ec2_host_machine:
                self.ec_2_metadata = self.resource_provisioner.get_ec2_metadata()

                logger.info(f"EC2 metadata: {self.ec_2_metadata}")
                self.fexec_args['vpc'] = {'subnets': [self.ec_2_metadata['subnet_id']],
                                          'security_groups': [self.ec_2_metadata['security_group_id']]}

        self.job_queue = Queue()
        self.stop_server_flag = threading.Event()

        self.keep_running = True
        self.job_pool_executor = job_pool_executor

        self.learning_plane = LearningPlane()
        if initialize:
            if AWS_LAMBDA_BACKEND in orchestrator_backends:
                if self.check_runtime_status(fexec=None, double_check=True):
                    logger.info(f"Runtime is available")
                else:
                    logger.info(f"Runtime is not available")
                    self.redeploy_runtime(fexec=None)
            elif LOCAL_BACKEND in orchestrator_backends:
                self.copy_default_included_function()
        logger.info(f"Orchestrator initialized")

    def check_runtime_status(self, fexec: FunctionExecutor = None, double_check: bool = True):
        '''
        Checks if the runtime is available. If double_check is True, it will test the runtime by calling a function.
        :param fexec: FunctionExecutor. The FunctionExecutor to be used
        :param double_check: bool. If True, it will test the runtime by calling a function
        :return: bool. True if the runtime is available, False otherwise
        '''
        logger.info(f"Checking Lithops backend configuration...")
        if not fexec:
            fexec = FunctionExecutor(**self.fexec_args)
        runtime_meta = fexec.invoker.get_runtime_meta(fexec._create_job_id('A'),
                                                      fexec.config['aws_lambda']['runtime_memory'])
        if runtime_meta:
            if double_check:
                logger.info(f"Runtime {fexec.backend} found.")
                logger.info(f'Testing call')
                lithops_futures = self.resource_provisioner.lithops_call(fexec,
                                                                         payloads=[{'body': {'ping': True}}])
                lithops_results = self.resource_provisioner.wait_futures(fexec, futures=lithops_futures,
                                                                         timeout=30, exception_str=False)
                if isinstance(lithops_results, Exception):
                    logger.info(f"Runtime {fexec.backend} found but not working.")
                    return False
                logger.info(f"Runtime {fexec.backend} found and working.")
            return True
        else:
            logger.info(f"Runtime {fexec.backend} not found.")
            return False

    def redeploy_runtime(self, fexec: FunctionExecutor = None, initialize: bool = True):
        '''
        Redeploys the runtime by deleting it, creating it again and testing it.
        '''
        if not fexec:
            fexec = FunctionExecutor(**self.fexec_args)
        fexec.compute_handler.delete_runtime(fexec.config['aws_lambda']['runtime'],
                                             fexec.config['aws_lambda']['runtime_memory'])
        exists_included_function = self.copy_default_included_function()

        logger.info(f"Runtime {self.fexec_args['runtime']} not found. Creating runtime...")
        if initialize:
            lithops_futures = self.resource_provisioner.lithops_call(fexec, payloads=[{'body': {'ping': True}}])
            lithops_results = self.resource_provisioner.wait_futures(fexec, futures=lithops_futures, timeout=30,
                                                                     exception_str=False)
            logger.info(f"Runtime created and returned: {lithops_results}")

    def copy_default_included_function(self):
        if not os.path.isdir("included_function"):
            exists_included_function = False
            logger.info(f"Using default included_function")
            # Get directory of this file
            current_dir = os.path.dirname(os.path.realpath(__file__))
            # Get working directory
            cwd = os.getcwd()
            # Copy the current_dit/included_function to the current working directory using a python library
            import shutil
            shutil.copytree(f"{current_dir}/included_function", f"{cwd}/included_function")
        else:
            logger.info(f"Using included_function found in the current directory")
            exists_included_function = True
        return exists_included_function

    def delete_runtime(self):
        '''
        Deletes the runtime.
        '''
        fexec = FunctionExecutor(**self.fexec_args)
        fexec.compute_handler.delete_runtime(fexec.config['aws_lambda']['runtime'],
                                             fexec.config['aws_lambda']['runtime_memory'])

    def enqueue_job(self, job: Job):
        """
        Enqueues a job to later be processed by a JobManager.
        :param job: Job. The job to be enqueued
        """
        self.job_queue.put(job)

    def dequeue_job(self):
        """
        Dequeues a job from the job_queue.
        :return: Job. The job dequeued
        """
        job = self.job_queue.get()
        return job

    def get_available_port(self):
        """
        Gets an available port to be used by the Job Manager that is also valid for gRPC.
        """
        ip = self.ec_2_metadata["ip"] if self.ec_2_metadata else DEFAULT_IP
        return 0
        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.bind(('', 0))
                    assigned_port = sock.getsockname()[1]
                except Exception:
                    continue  # Try another port if binding fails

            # Now test the port with gRPC
            try:
                test_server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
                port_binding = test_server.add_insecure_port(f'{ip}:{assigned_port}')
                if port_binding == 0:
                    logger.info(f"Port {assigned_port} cannot be used by gRPC. Trying another.")
                    continue  # gRPC could not bind to the port
                else:
                    test_server.stop(0)  # Clean up the test server
                    return assigned_port
            except Exception as e:
                logger.info(f"Port {assigned_port} caused gRPC error ({e}). Trying another.")
                continue

    def run_orchestrator(self):
        '''
        Runs the orchestrator. Gets Jobs out of the queue and starts Job Managers if there are available resources.
        '''
        future_to_job = {}  # Dictionary to store future and corresponding job
        while (self.keep_running):
            if not self.job_queue.empty():
                job = self.dequeue_job()
                self.learning_plane.update()
                _job = self.resource_provisioner.apply_job_policy(job)
                _job = self.resource_provisioner.correct_resources(_job)
                if self.resource_provisioner.check_available_resources(_job):
                    self.resource_provisioner.increment_count(_job.num_task_managers)
                    future = self.start_job_manager(_job)
                    future_to_job[future] = _job  # Save future and job together
                else:
                    self.enqueue_job(job)

            # Check if is there any future that is done
            done_futures = [f for f in future_to_job if f.done()]
            for future in done_futures:
                job = future_to_job.pop(future)
                logger.info(f"Job {job.job_name} finished.")
                self.resource_provisioner.decrement_count(job.num_task_managers)
            time.sleep(1)

    def run_job(self, job):
        '''
        Runs one job. If there are available resources, it will start a Job Manager to process the job.
        This method is used to run a single job without starting the orchestrator.
        :param job: Job. The job to be processed
        :return: dict. The job as a dictionary
        '''
        self.learning_plane.update()
        job = self.resource_provisioner.apply_job_policy(job)
        job = self.resource_provisioner.correct_resources(job)
        if self.resource_provisioner.check_available_resources(job):
            self.resource_provisioner.increment_count(job.num_task_managers)
            future = self.start_job_manager(job)
            result = future.result()
            self.resource_provisioner.decrement_count(job.num_task_managers)
            return result
        else:
            raise ValueError(f"Job {job.job_name} could not be started due to lack of resources")

    def start_job_manager(self, job):
        '''
        Starts a Job Manager to process the job.
        :param job: Job. The job to be processed
        :return: future. The future of the Job Manager
        '''

        available_port = self.get_available_port()
        job.ec2_metadata = self.ec_2_metadata
        job_manager = JobManager(fexec_args=self.fexec_args, job=job, resource_provisioner=self.resource_provisioner,
                                 split_enumerator_thread_pool_size=self.split_enumerator_thread_pool_size,
                                 split_enumerator_port=available_port)

        future = self.job_pool_executor.submit(job_manager.run)

        return future

    def force_cold_start(self, sleep_time=600):
        '''
        Forces a cold start by deleting the runtime and creating it again.
        '''
        fexec = FunctionExecutor(**self.fexec_args)
        runtime_name = fexec.config['aws_lambda']['runtime']
        runtime_memory = fexec.config['aws_lambda']['runtime_memory']
        fexec.compute_handler.backend.force_cold(runtime_name, runtime_memory)
        time.sleep(sleep_time)


