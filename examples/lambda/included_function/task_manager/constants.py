import numpy
from torch import Tensor

VALID_BATCH_FORMATS_NAMES = ["bytes", "tensor", "numpy", None]
VALID_BATCH_FORMATS = [bytes, Tensor, numpy.ndarray, None]

DEFAULT_BATCH_SIZE = 1

DEFAULT_TASK_MANAGER_CONFIG_FILE = "default_task_manager_config.yml"
LOGGING_FORMAT = "%(asctime)s [%(threadName)s] [%(levelname)s] [%(filename)s:%(lineno)d] [%(funcName)s] %(message)s"