import yaml
from matplotlib import pyplot as plt
from torch import multiprocessing
import re
import json
from typing import Optional, Any
import concurrent
import os
import numpy
import torch
import logging
from .task_executor import TaskExecutor
from .constants import VALID_BATCH_FORMATS, VALID_BATCH_FORMATS_NAMES, DEFAULT_BATCH_SIZE, \
                        DEFAULT_TASK_MANAGER_CONFIG_FILE, LOGGING_FORMAT

from .queuewrapper import ThreadingQueue, InputPipeQueue, OutputPipeQueue
import time

logger = logging.getLogger()

class TaskManager:
    def __init__(self, config_file="task_manager_config.yml", config_dict=None, logging_level=logging.INFO, time_log_file='/tmp/task_manager/time_log.txt'):
        """ Construct a TaskManager and load the configuration file or dictionary.
        If both dictionary and config file are set, the file will be ignored.

        Example of YAML file:

        load:
          max_concurrency: 16
          batch_size: 1

        preprocess:
          num_cpus: 4
          batch_size: 8
          intraop: 1

        predict:
          n_models: 8
          interop: 4
          intraop: 2

        Example of config dictionary:

        config_dict = {
                'load': {'batch_size': 1, 'max_concurrency': 16},
                'preprocess': {'batch_size': 8, 'intraop': 1, 'num_cpus': 4},
                'predict': {'interop': 4, 'intraop': 2, 'n_models': 8}
               }


        :param config_file: Path to config YAML file.
        :param config_dict: Dictionary with configuration
        """
        logging.basicConfig(
            level=logging_level,
            format=LOGGING_FORMAT,
            handlers=[logging.StreamHandler()]
        )
        self.task_executors = []
        self.thread_executor = None
        self.interop = None
        self.logging_level=logging_level
        self.first_run = True
        self.time_log_file = time_log_file
        if config_dict:
            self.config_dict = config_dict
        else:
            self.config_dict = self.__read_config_file(config_file)
        self.benchmarks=None

    def __read_config_file(self, config_file: str = DEFAULT_TASK_MANAGER_CONFIG_FILE):
        if os.path.isfile(config_file):
            logger.info(f"Configuration file found in directory. ")
            with open(config_file, 'r') as file:
                config_dict = yaml.safe_load(file)
                return config_dict
        else:
            return None

    def task(
            self,
            mode: str = None,
            previous: Optional[callable] = None,
            batch_size: Optional[int] = None,
            batch_format: Optional[str] = None,
            max_concurrency: Optional[int] = None,
            num_cpus: Optional[int] = None,
            n_models: Optional[int] = None,
            jit_model: Optional[callable] = None,
            interop: Optional[int] = None,
            intraop: Optional[int] = None
    ) -> callable:
        """ A decorator function to add a task to the scheduler
        This function is used to define a task and add it to the scheduler.

        Rules:
        - The different tasks will be declared in the desired order of execution.
        - previous function is the reference, not the string name.
        - In mode threading, max_concurrecy regulates the amount of threads created.
        - In mode multiprocessing, num_cpus regulates the amount of processes that will be created.
        - In mode torchscript, n_models regulates the amount of models that will be parallelised.
        - A task only executes in one mode, and in the same manner, it will use max_concurrency OR num_cpus OR n_models
        - Interop threads can only be set or changed once. https://pytorch.org/docs/stable/generated/torch.set_num_interop_threads.html

        Examples:
        inferencer = TaskManager(config_file=config_file)

        @inferencer.task(mode="threading")
        def load(image_dict):
            result_dict = {}
            for key in image_dict:
                image_data = resources.loadimage(key)
                result_dict.update({key: image_data})
            return result_dict

        @inferencer.task(mode="multiprocessing", previous=load, batch_format="bytes")
        def preprocess(image_dict):
            result_dict = {}
            for key, value in image_dict.items():
                tensor = resources.transform_image(value)
                result_dict.update({key: tensor})
            return result_dict

        @inferencer.task(mode="torchscript", previous=preprocess, batch_format="tensor", jit_model=jit_model)
        def predict(tensor_dicts, ensemble):
            tensors = []
            for key, value in tensor_dicts.items():
                tensors.append(value)
            prediction_results = OffSampleTorchscriptFork(ensemble).predict(tensors)
            result_dict = {}
            for key, prediction_result in zip(tensor_dicts.keys(), prediction_results):
                result_dict.update({key: prediction_result})
            return result_dict



        :param mode: Parallelisation mode. One of: multiprocessing, threading or torchscript
        :param previous: Reference to the previous function in the pipeline
        :param batch_size: An integer. By default, is set to 1
        :param batch_format: Specify the type of batch expected to receive. One of VALID_BATCH_FORMATS in constrants.py
        :param max_concurrency: The concurrency in the threading mode. By default, is set to torch.multiprocessing.cpu_count().
        :param num_cpus: The number of CPUs in the multiprocessing mode. By default, is set to torch.multiprocessing.cpu_count().
        :param n_models: Number of models that will be parallelised in torchscript mode. By default, is set to torch.multiprocessing.cpu_count().
        :param jit_model: Reference to a ScriptModule to be parallelised using the Ensemble.
        :param interop: Thread parallelisation between operations. This applies to any mode. Can only be set once or changed once.
        :param intraop:Thread parallelisation within an operation. This applies to any mode.
        :return:
        """
        def decorator(func: callable) -> callable:
            # Initialization of temporary variables
            _num_cpus = None
            _max_concurrency = None
            _n_models = None
            _batch_size = None
            _interop = None
            _intraop = None

            # Checking if the config file was loaded, use its values
            if self.config_dict:
                if func.__name__ in self.config_dict:
                    if "max_concurrency" in self.config_dict[func.__name__]:
                        _max_concurrency = self.config_dict[func.__name__]["max_concurrency"]
                    if "num_cpus" in self.config_dict[func.__name__]:
                        _num_cpus = self.config_dict[func.__name__]["num_cpus"]
                    if "n_models" in self.config_dict[func.__name__]:
                        _n_models = self.config_dict[func.__name__]["n_models"]
                    if "batch_size" in self.config_dict[func.__name__]:
                        _batch_size = self.config_dict[func.__name__]["batch_size"]
                    if "interop" in self.config_dict[func.__name__]:
                        _interop = self.config_dict[func.__name__]["interop"]
                    if "intraop" in self.config_dict[func.__name__]:
                        _intraop = self.config_dict[func.__name__]["intraop"]

            # If user passed an argument, we use it instead of the config file
            if num_cpus:
                if _num_cpus:
                    logger.info(
                        f"Task {func.__name__} - The num_cpus passed by parameter ({num_cpus}) overwrites the num_cpus in config file ({_num_cpus}).")
                _num_cpus = num_cpus
            if max_concurrency:
                if _max_concurrency:
                    logger.info(
                        f"Task {func.__name__} - The max_concurrency passed by parameter ({max_concurrency}) overwrites the max_concurrency in config file ({_max_concurrency}).")
                _max_concurrency = max_concurrency
            if n_models:
                if _n_models:
                    logger.info(
                        f"Task {func.__name__} - The n_models passed by parameter ({n_models}) overwrites the n_models in config file ({_n_models}).")
                _n_models = n_models
            if batch_size:
                if _batch_size:
                    logger.info(
                        f"Task {func.__name__} - The batch_size passed by parameter ({batch_size}) overwrites the batch_size in config file ({_batch_size}).")
                _batch_size = batch_size
            if interop:
                if _interop:
                    logger.info(
                        f"Task {func.__name__} - The interop passed by parameter ({interop}) overwrites the interop in config file ({_interop}).")
                _interop = interop
            if intraop:
                if _intraop:
                    logger.info(
                        f"Task {func.__name__} - The intraop passed by parameter ({intraop}) overwrites the intraop in config file ({_intraop}).")
                _intraop = intraop

            # Check if user is trying to use num_cpus, max_concurrency and/or n_models all at once for a task
            if ((_num_cpus and _max_concurrency and _n_models) or (_num_cpus and _max_concurrency) or (
                    _num_cpus and _n_models) or (_max_concurrency and _n_models)):
                raise ValueError(
                    f"Task {func.__name__} - A Task must use max_concurrency (Threading) or num_cpus (Multiprocessing) or n_models (Torchscript)  ")

            # Setting max_workers to cpu count. max_workers represents num_cpus, max_concurrency and n_models in the 3 modes.
            if mode == "multiprocessing":
                if not _num_cpus or _num_cpus <= 0:
                    _num_cpus = torch.multiprocessing.cpu_count()
                    logger.info(
                        f"Task {func.__name__} - Setting num_cpus to Default Multiprocessing CPU count ({_num_cpus})")

            elif mode == "threading":
                if not _max_concurrency or _max_concurrency <= 0:
                    _max_concurrency = torch.multiprocessing.cpu_count()
                    logger.info(
                        f"Task {func.__name__} - Setting max_concurrency to Default Multiprocessing CPU count ({_max_concurrency})")
                if not self.thread_executor:
                    self.thread_executor = concurrent.futures.ThreadPoolExecutor()
            elif mode == "torchscript":
                if not _n_models or _n_models <= 0:
                    _n_models = torch.multiprocessing.cpu_count()
                    logger.info(
                        f"Task {func.__name__} - Setting n_models to Default Multiprocessing CPU count ({_n_models})")
                if not jit_model:
                    raise ValueError(
                        f"Task {func.__name__} - A Torchscript Task needs a nn.Module, function, class type, dictionary, or list to compile. Use the parameter 'model'.")
            else:
                logger.warning(
                    f"Task {func.__name__} - A Task must use max_concurrency (Threading) or num_cpus (Multiprocessing) or n_models (Torchscript).")
                raise ValueError(
                    f"Task {func.__name__} - A Task must use max_concurrency (Threading) or num_cpus (Multiprocessing) or n_models (Torchscript)  ")

            # Setting default batch size
            if not _batch_size or _batch_size <= 0:
                _batch_size = DEFAULT_BATCH_SIZE
                logger.info(f"Task {func.__name__} - Setting batch_size to ({_batch_size})")

            # Check if batch format is valid
            if batch_format not in VALID_BATCH_FORMATS_NAMES:
                raise TypeError(f"Task {func.__name__} - The given batch format {batch_format} is not allowed, must be one of {VALID_BATCH_FORMATS_NAMES}.")
            for i, format in enumerate(VALID_BATCH_FORMATS_NAMES):
                if batch_format == format:
                    if batch_format== "tensor":
                        _batch_format = numpy.ndarray
                    else:
                        _batch_format = VALID_BATCH_FORMATS[i]

            # Check if interop was executed before. Interop can only be set once https://pytorch.org/docs/stable/generated/torch.set_num_interop_threads.html
            if _interop and self.interop:
                logger.warning(f"Task {func.__name__} - Inter-op threads was set before in a previous task. Interop will keep being {torch.get_num_interop_threads()}.")
            elif _interop and not self.interop and _interop > 0:
                self.interop = _interop
                logger.info(f"Task {func.__name__} -Interop threads will be set to {self.interop} at start of task. This number won't be changed after.")
            else:
                logger.info(f"Task {func.__name__} - Interop threads not set or invalid. Interop threads has by default{torch.get_num_interop_threads()}. This number can be changed.")

            if _intraop and _intraop > 0:
                self.intraop = _intraop
                logger.info(f"Task {func.__name__} -Intraop threads will be set to {self.intraop} at start of task.")
            else:
                logger.info(f"Task {func.__name__} -Intraop threads not set or invalid. Intraop threads has by default{torch.get_num_threads()}. This number can be changed.")

            if previous:
                created = False
                for task_index, task_executor in enumerate(self.task_executors):
                    if task_executor.function == previous:
                        if _num_cpus:
                            if isinstance(task_executor.output_queue, ThreadingQueue):
                                input_queue = InputPipeQueue(num_pipes=_num_cpus-1, main_process=True)
                                task_executor.output_queue = input_queue
                            else:
                                input_queue = task_executor.output_queue
                            output_queue = OutputPipeQueue(num_pipes=_num_cpus-1, main_process=True)
                        else:
                            input_queue = task_executor.output_queue
                            output_queue = ThreadingQueue()
                        task_executor = TaskExecutor(_batch_size, _batch_format, _max_concurrency, _num_cpus, _n_models,
                                                     jit_model, input_queue, output_queue, func, _interop, _intraop, self.time_log_file)
                        self.task_executors.insert(task_index + 1, task_executor)
                        created = True
                if not created:
                    raise ValueError(f"Task {func.__name__} - previous task {previous} not found")
            else:
                if _num_cpus:
                    input_queue = InputPipeQueue(num_pipes=_num_cpus-1, main_process=True)
                    output_queue = OutputPipeQueue( num_pipes=_num_cpus-1, main_process=True)
                else:
                    input_queue = ThreadingQueue()
                    output_queue = ThreadingQueue()
                task_executor = TaskExecutor(_batch_size, _batch_format, _max_concurrency, _num_cpus, _n_models,
                                             jit_model, input_queue, output_queue, func, _interop, _intraop, self.time_log_file)
                self.task_executors.append(task_executor)

            return func

        return decorator

    def __get_ordered_queues(self) -> list[Any]:
        # Retrieve and return the ordered list of queues from task_executors
        queues = []
        for task_executor in self.task_executors:
            queues.append(task_executor.input_queue)
        queues.append(task_executor.output_queue)
        return queues

    def __empty_queues(self, queues: list[Any]):
        # Empty all queues in the provided list of queues
        for queue in queues:
            if isinstance(queue, ThreadingQueue):
                queue.counter = 0
                while not queue.empty():
                    try:
                        queue.get_nowait()
                    except Exception:
                        continue
            elif queue.closed:
                queue.reopen()

    def __close_queues(self):
        for task_executor in self.task_executors:
            task_executor.input_queue.close()
            task_executor.output_queue.close()

    def __reset_queues(self):
        for i, task_executor in enumerate(self.task_executors):
            if i>0:
                previous_executor = self.task_executors[i-1]
                if task_executor.num_cpus:
                    if isinstance(previous_executor.output_queue, ThreadingQueue):
                        task_executor.input_queue = InputPipeQueue(task_executor.num_cpus-1)
                        previous_executor.output_queue = task_executor.input_queue
                    else:
                        task_executor.input_queue = previous_executor.output_queue
                    task_executor.output_queue = OutputPipeQueue( task_executor.num_cpus-1)
                else:
                    task_executor.input_queue = previous_executor.output_queue
                    task_executor.output_queue = ThreadingQueue()

            else:
                if task_executor.num_cpus:
                    task_executor.input_queue = InputPipeQueue( task_executor.num_cpus-1)
                    task_executor.output_queue = OutputPipeQueue(task_executor.num_cpus-1)
                else:
                    task_executor.input_queue = ThreadingQueue()
                    task_executor.output_queue = ThreadingQueue()




    def __launch_threads(self, tasks_threading):
        # Launch threads for tasks that use threading
        thread_registry = []
        for task_executor in tasks_threading:
            for _ in range(task_executor.max_concurrency - 1):
                thread = self.thread_executor.submit(task_executor.execute)
                thread_registry.append(thread)
        return thread_registry

    def __launch_processes(self, tasks_multiproc):
        # Launch processes for tasks that use multiprocessing
        process_registry = []
        for task_executor in tasks_multiproc:
            torch.set_num_threads(1)
            logger.info(f"Task {task_executor.function.__name__} - Intraop threads set to {1}")
            for id in range(task_executor.num_cpus - 1):
                process = multiprocessing.Process(target=task_executor.execute, args=(id,))
                process.start()
                process_registry.append(process)
        return process_registry

    def __launch_torchscript(self, tasks_torchscript, num_inputs):
        # Launch TorchScript tasks
        for task_executor in tasks_torchscript:
            batch_size = task_executor.batch_size
            # Create a list of batch sizes, dividing the number of inputs
            num_full_batches = num_inputs // batch_size
            remaining_data = num_inputs % batch_size
            batch_list = [batch_size] * num_full_batches
            if remaining_data > 0:
                batch_list.append(remaining_data)
            all_results = {}
            for batch_size in batch_list:
                task_executor.batch_size = batch_size
                results = task_executor.execute()
                all_results.update(results)
            return all_results

    def __dequeue_queue(self, queue, num_outputs):
        # Dequeue results from a queue and return them as a dictionary
        results = {}
        for _ in range(num_outputs):
            image_dict = queue.get()
            results.update(image_dict)
        return results

    def __enqueue_queue(self, queue, dict):
        # Enqueue data into a queue and update monitoring dictionary with start time
        for key, value in dict.items():
            queue.put({key: value})

    def shutdown_executors(self):
        # Shutdown both thread and process executors
        self.thread_executor.shutdown()


    def shutdown_task_manager(self):
        self.shutdown_executors()
        self.__close_queues()


    def __update_config(self, config_dict: dict, time_log_file=None):
        for key, value in config_dict.items():
            for executor in self.task_executors:
                executor.time_log_file = time_log_file

                if executor.function.__name__ == key:
                    for key_config, value_config in value.items():
                        if value_config and value_config>0:
                            if key_config == "max_concurrency" and executor.max_concurrency is not None:
                                executor.max_concurrency = value_config
                                executor.nworkers = value_config
                                logger.info(f"Task {executor.function.__name__} - max_concurrency set to {value_config}")
                            elif key_config == "num_cpus" and executor.num_cpus is not None:
                                logger.info(f"Task {executor.function.__name__} - num_cpus set to {value_config}")
                                executor.num_cpus = value_config
                                executor.nworkers = value_config


                            elif key_config == "n_models" and executor.n_models is not None:
                                executor.n_models = value_config
                                executor.nworkers = value_config
                                logger.info(f"Task {executor.function.__name__} - n_models set to {value_config}")
                            if key_config == "batch_size" and executor.batch_size is not None:
                                executor.batch_size = value_config
                                logger.info(f"Task {executor.function.__name__} - batch_size set to {value_config}")
                            if key_config == "intraop" and executor.intraop is not None:
                                executor.intraop = value_config
                                logger.info(f"Task {executor.function.__name__} - intraop set to {value_config}")

                            if key_config == "interop" and executor.interop is not None and executor.interop_set == False:
                                executor.interop = value_config
                                logger.info(f"Task {executor.function.__name__} - interop set to {value_config}")

    def process_tasks(self, input_dict: dict, config_dict: dict=None, time_log_file=None) -> dict:
        """ Executes the Scheduler witht the tasks that were set before
        :param input_dict: Dictionary with {key:value} where value must be a VALID_BATCH_FORMATS from constants.py
        :return: Dictionary with the key set in the input_dict and value the result after all the task execution
        """
        if time_log_file:
            self.time_log_file=time_log_file
        self.benchmarks = []
        start_time = time.time()
        if not self.first_run:
            self.__close_queues()
            self.__reset_queues()
            self.shutdown_executors()
            self.thread_executor = concurrent.futures.ThreadPoolExecutor()

        if config_dict:
            self.__close_queues()
            self.__update_config(config_dict, self.time_log_file)
            self.__reset_queues()


        # Get all queues from task_executors and order them in execution order
        queues = self.__get_ordered_queues()

        # If it doesn't exist, create folder of time logs. Extract the folder from the time_log_file
        if not os.path.exists(os.path.dirname(self.time_log_file)):
            os.makedirs(os.path.dirname(self.time_log_file))

        with open(self.time_log_file, 'w+') as f:
            for key in input_dict:
                f.write(f"{key},start,,{time.time()}\n")



        # Initialitzations
        total_threads = 0
        total_procs = 0
        tasks_threading = []
        tasks_multiproc = []
        tasks_torchscript = []

        # Divide tasks in 3 different types
        for task_executor in self.task_executors:
            if task_executor.num_cpus:
                tasks_multiproc.append(task_executor)

                # Substract 1 because of one of the functions being executed in main process
                total_procs = total_procs + task_executor.num_cpus - 1
            elif task_executor.max_concurrency:
                tasks_threading.append(task_executor)

                # Substract 1 because of one of the functions being executed in main process
                total_threads = total_threads + task_executor.max_concurrency - 1
            elif task_executor.ens:
                tasks_torchscript.append(task_executor)


        # Set the max_workers of the process and thread executors
        if self.thread_executor:
            self.thread_executor._max_workers = total_threads

        # Launch processes and threads
        process_registry = self.__launch_processes(tasks_multiproc)
        thread_registry = self.__launch_threads(tasks_threading)

        # Enqueue the input dictionary in the first queue of the pipeline
        self.__enqueue_queue(queues[0], input_dict)
        # print("putting none")
        self.task_executors[0].input_queue.put(None)
        # For every task executor that is either multiprocessing or threading
        for task_executor in self.task_executors:
            if task_executor.num_cpus:
                # Execute one time in main process
                task_executor.execute(-1)
                self.benchmarks.append(time.time() - start_time)
                start_time = time.time()

            if task_executor.max_concurrency:
                # Execute one time in main process
                task_executor.execute()
                self.benchmarks.append(time.time() - start_time)
                start_time = time.time()

        # Execute
        results = self.__launch_torchscript(tasks_torchscript, len(input_dict))
        if not results:
            results = self.__dequeue_queue(queues[len(queues) - 1], len(input_dict))

        self.benchmarks.append(time.time() - start_time)
        for task_executor in self.task_executors:
            if task_executor.num_cpus:
                task_executor.input_queue.close()
                task_executor.output_queue.close()
        for process in process_registry:
            process.join()

        for thread in thread_registry:
            thread.result()

        self.shutdown_executors()
        self.__close_queues()
        self.first_run = False
        for key in input_dict:
            with open(self.time_log_file, 'a') as f:
                f.write(f"{key},end,,{time.time()}\n")
        return results


    def stringify_config_dict(self):
        """ Creates a string representing the configuration used during the execution

        :return:
        """
        string_file_name = ""
        for key, value in self.config_dict.items():
            json_string = json.dumps(value)
            string_file_name = string_file_name + json_string
        file_name = re.sub(r'[/\\:*?"<>|{}]', '', string_file_name)
        file_name = file_name.replace(",","")
        return file_name

    def get_monitoring_times(self):
        if self.time_log_file:
            with open(self.time_log_file, 'r') as file:
                lines = file.readlines()

            times = {'total': 0.0, 'load': 0.0, 'transform': 0.0, 'predict': 0.0}

            # Variables per emmagatzemar els temps inicials i finals de cada fase
            start_time = 0
            load_start_times = []
            load_finished_times = []
            preprocess_start_times = []
            preprocess_finished_times = []
            predict_start_times = []
            predict_finished_times = []

            # Processar cada línia del fitxer
            for line in lines:
                parts = line.strip().split(',')
                action = parts[1]
                status = parts[2]
                timestamp = float(parts[3])

                if action == 'start':
                    start_time = timestamp
                elif action == 'load' and status == 'started':
                    load_start_times.append(timestamp)
                elif action == 'load' and status == 'finished':
                    load_finished_times.append(timestamp)
                elif action == 'preprocess' and status == 'started':
                    preprocess_start_times.append(timestamp)
                elif action == 'preprocess' and status == 'finished':
                    preprocess_finished_times.append(timestamp)
                elif action == 'predict' and status == 'started':
                    predict_start_times.append(timestamp)
                elif action == 'predict' and status == 'finished':
                    predict_finished_times.append(timestamp)
                elif action == 'end':
                    times['total'] = timestamp - start_time
                    times['load'] = sum(load_finished_times) / len(load_finished_times) - sum(load_start_times) / len(
                        load_start_times)
                    times['transform'] = sum(preprocess_finished_times) / len(preprocess_finished_times) - sum(
                        preprocess_start_times) / len(preprocess_start_times)
                    times['predict'] = sum(predict_finished_times) / len(predict_finished_times) - sum(
                        predict_start_times) / len(predict_start_times)

            return times['load'], times['transform'], times['predict']
        else:
            return None, None, None


    def get_log_file_content(self):
        if self.time_log_file:
            with open(self.time_log_file, 'r') as file:
                log_content = file.read()
                log_content = ''.join(log_content)
                return log_content

    def log_to_dict(self):
        content = self.get_log_file_content()
        parsed_data = {}
        lines = content.strip().split('\n')
        for line in lines:
            parts = line.split(',')
            key, action, status, timestamp = parts
            if key not in parsed_data:
                parsed_data[key] = {}
            if status == '':
                parsed_data[key][action] = float(timestamp)
            else:
                parsed_data[key][f"{action} {status}"] = float(timestamp)
        return parsed_data

    def plot_images(self, save: Optional[bool]=False, plot_path: Optional[str] = 'gantt_chart.png', show: Optional[bool]=False) -> bool:
        """ Creates a plot of each image execution, divided by tasks.

        :param save: If true, saves the file in plot_path
        :param plot_path: Path to the file where the plot will be saved
        :param show: If true, shows the plot
        :return: If plotting was correctly executed
        """
        try:
            monitoring_dict = self.log_to_dict()
            function_data = {}
            i = 0

            for key, value in monitoring_dict.items():
                image_complete_monitoring=[]
                ordered_keys = sorted(value, key=value.get)
                ordered_values = [value[key] for key in ordered_keys]
                for j in range(len(ordered_keys)):
                    if j < len(ordered_keys):
                        if "error" in ordered_keys[j]:
                            image_complete_monitoring.append(("buffering", ordered_values[j], ordered_values[j+1]))
                        if "started" in ordered_keys[j]:
                            image_complete_monitoring.append((ordered_keys[j].split(" ")[0], ordered_values[j], ordered_values[j+1]))
                        if "ended" in ordered_keys[j]:
                            image_complete_monitoring.append(("buffering", ordered_values[j], ordered_values[j+1]))
                        if ordered_keys=="start":
                            image_complete_monitoring.append(("buffering", ordered_values[j], ordered_values[j + 1]))
                        j += 1
                function_data["image " + str(i)] = image_complete_monitoring
                i += 1


            default_colors = ["purple", "red", "orange", "blue", "brown", "cyan", "pink"]
            state_colors = {}
            legend_colors = {}
            for j, task_executor in enumerate(self.task_executors):
                state_colors[task_executor.function.__name__] = default_colors[j]
                legend_colors[task_executor.function.__name__] = default_colors[j]
            state_colors['buffering']='white'

            min_time = float('inf')
            for states in function_data.values():
                for _, start_time, end_time in states:
                    min_time = min(min_time, start_time, end_time)

            # Create a Gantt chart
            fig, ax = plt.subplots(figsize=(38, 20))
            function_names = list(function_data.keys())

            # Adjust the spacing between functions
            vertical_spacing = 1.0

            for i, (function, states) in enumerate(function_data.items()):
                for state, start_time, end_time in states:
                    ax.broken_barh([(start_time - min_time, end_time - start_time)], (i * vertical_spacing, 0.8),
                                   facecolors=state_colors[state])

            # Customize the chart
            ax.set_yticks([i * vertical_spacing + 0.4 for i in range(len(function_names))])
            ax.set_yticklabels(function_names)
            ax.set_xlabel('Time')
            ax.set_title('Function State Gantt Chart')

            # Create a legend for the state colors
            legend_labels = [plt.Rectangle((0, 0), 1, 1, color=color) for state, color in legend_colors.items()]
            legend_states = [state for state in legend_colors.keys()]
            ax.legend(legend_labels, legend_states, title='States', loc='upper left')

            plt.tight_layout()

            if save:
                plt.savefig(plot_path)

            if show:
                plt.show()
            return True
        except Exception as e:
            logger.error(f"Problem during plotting {e}")
            return False

