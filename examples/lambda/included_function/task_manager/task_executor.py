import logging
import time
from typing import Optional

import numpy as np
import torch
from .torchscript_ensemble import ModelEnsemble
from torch.multiprocessing import Queue
from .queuewrapper import ThreadingQueue, InputPipeQueue, OutputPipeQueue

logger = logging.getLogger()


class TaskExecutor:
    def __init__(
            self,
            batch_size: int,
            batch_format: str,
            max_concurrency: int,
            num_cpus: int,
            n_models: int,
            jit_model: callable,
            input_queue: Queue,
            output_queue: Queue,
            function: callable,
            interop: int,
            intraop: int,
            time_log_file: str
    ):
        # Constructs a TaskExecutor with its function and parallelization configuration
        self.batch_size = batch_size
        self.batch_format = batch_format
        self.max_concurrency = max_concurrency
        self.num_cpus = num_cpus
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.function = function
        self.intraop = intraop
        self.interop = interop
        self.interop_set = False
        self.jit_model = jit_model
        self.n_models = n_models
        if jit_model and n_models:
            self.ens = True
        else:
            self.ens = None
        if self.num_cpus:
            self.nworkers = self.num_cpus
        elif self.max_concurrency:
            self.nworkers = self.max_concurrency
        else:
            self.nworkers = n_models
        self.time_log_file = time_log_file

    def call_inner_function(self, dicts: dict) -> dict:
        # Calls function in executor. If torchscript mode, call also ends the executor (puts None in input_queue).
        logger.info(f"{self.function.__name__} started")
        if self.ens:
            results = self.function(dicts, self.ens)
        else:
            results = self.function(dicts)
        logger.info(f"{self.function.__name__} ended")
        return results

    def valid_type_check(self, value: Optional[str]) -> bool:
        # Checks if batch has the expected format for this executor
        if self.batch_format and isinstance(value, self.batch_format):
            return True
        elif self.batch_format is None and value is None:
            return True
        else:
            raise Exception(f"Object of type {self.batch_format} expected, but got {type(value)}")

    def dequeue_dicts(self, process_id) -> dict:
        # Dequeues dictionary and checks if format is valid
        dequeued_dicts = {}
        for _ in range(self.batch_size):

            if self.ens and isinstance(self.input_queue, OutputPipeQueue):
                image_dict = self.input_queue.get_any()
            else:
                image_dict = self.input_queue.get(process_id)

            if not image_dict:
                # print(f"{self.function.__name__} - Dequeued None")
                return dequeued_dicts, True

            for key, value in image_dict.items():
                try:
                    self.valid_type_check(value)
                    # If the input queue is a pipe and the value is a numpy, convert it to tensor.
                    if isinstance(self.input_queue, OutputPipeQueue) and isinstance(value, np.ndarray):
                        tensor = torch.from_numpy(value)
                        image_dict.update({key: tensor})
                except Exception as e:
                    if isinstance(value, Exception):
                        logger.error(
                            f"{self.function.__name__} - Exception from previous task found, passing {key} to the next")
                        self.output_queue.put({key: value}, process_id)
                    else:
                        logger.error(f"{self.function.__name__} - Error during type check. {e}")
                        self.output_queue.put({key: e}, process_id)
            dequeued_dicts.update(image_dict)


        return dequeued_dicts, False

    def set_intraop_threads(self):
        if self.intraop:
            torch.set_num_threads(self.intraop)

    def set_interop_threads(self):
        # If interop_threads was not set before, set it with torch.set_num_interop_threads()
        if self.interop and not self.interop_set:
            torch.set_num_interop_threads(self.interop)
            logger.info(f"Interop threads set to {self.interop} from now on.")
            self.interop_set = True

    def update_queue_counter(self):
        if isinstance(self.output_queue, ThreadingQueue) or isinstance(self.output_queue, InputPipeQueue):
            with self.output_queue.counter_lock:
                self.output_queue.counter += 1
                if self.output_queue.counter == self.nworkers:
                    self.output_queue.stop()
    def __initialize_ens(self):
        if self.jit_model and self.n_models:
            lstm = ModelEnsemble(model=self.jit_model, n_models=self.n_models)
            self.ens = torch.jit.script(lstm)

    def execute(self, process_id: int = None):
        # Dequeues batch_size times, executes the batch and enqueues in the next queue.
        self.set_intraop_threads()
        self.set_interop_threads()
        self.__initialize_ens()
        while (True):
            dequeued_dicts, last = self.dequeue_dicts(process_id)
            if not dequeued_dicts or last:
                self.input_queue.put(None)
                if not dequeued_dicts:
                    self.update_queue_counter()
                    return
            try:
                for key in dequeued_dicts:
                    # save in log file
                    if self.time_log_file:
                        with open(self.time_log_file, 'a') as f:
                            f.write(f"{key},{self.function.__name__},started,{time.time()}\n")

                results = self.call_inner_function(dequeued_dicts)

                for key, value in results.items():
                    if not self.ens:
                        # If the output queue is a pipe and the value is a tensor, convert it to numpy.
                        # Tensors are not serializable in pipes.
                        if isinstance(self.input_queue, InputPipeQueue) and torch.is_tensor(value):
                            value = value.numpy()
                        self.output_queue.put({key: value}, process_id)
                    if self.time_log_file:
                        with open(self.time_log_file, 'a') as f:
                            f.write(f"{key},{self.function.__name__},finished,{time.time()}\n")
            except Exception as e:
                logger.error(f"Error executing task {self.function.__name__}. {e}")
                print(f"Error executing task {self.function.__name__}. {e}")
                logger.error(f" Affected inputs:")
                for key, value in dequeued_dicts.items():
                    logger.error(f"{key}")
                    self.output_queue.put({key: e}, process_id)
                results = dequeued_dicts

            if self.ens:
                return results

            if last:
                self.input_queue.put(None)
                self.update_queue_counter()
                return