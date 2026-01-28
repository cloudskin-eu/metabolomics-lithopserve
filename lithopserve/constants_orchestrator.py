import os
current_dir = os.path.dirname(os.path.realpath(__file__))

# Dynamic split (True) or static split (False)
DYNAMIC_SPLIT = True

# Max number of threads to use for the split enumerator
SPLIT_ENUMERATOR_THREAD_POOL_SIZE = 1000

# Max number of concurrent Job Managers being used
MAX_JOB_MANAGERS = 1

# Task Manager config
DEFAULT_SPLIT_SIZE = 32
DEFAULT_TASK_MANAGERS = 50
MAX_TASK_MANAGERS = 100

# Supported backends
LOCAL_BACKEND = "local"
AWS_LAMBDA_BACKEND = "aws_lambda"
ORCHESTRATOR_BACKENDS = {LOCAL_BACKEND, AWS_LAMBDA_BACKEND}
DEFAULT_ORCHESTRATOR = LOCAL_BACKEND

# Default IP address for the orchestrator
DEFAULT_IP = "127.0.0.1"

# Supported output storage
OUTPUT_STORAGE = {"local", "s3"}

# True if the orchestrator is running on an EC2 host machine.
# It automatically deploys the function inside the same VPC, subnet and security group as the host machine
EC2_HOST_MACHINE = True

# It shouldn't be needed to modify this parameter: it refers to the metadata service of AWS EC2.
EC2_METADATA_SERVICE = "http://169.254.169.254/latest/meta-data/"

# Default configuration parameters for the task manager.
DEFAULT_TASK_MANAGER_CONFIG = {
    'load': {'batch_size': 1, 'max_concurrency': 32},
    'preprocess': {'batch_size': 1, 'num_cpus': 2},
    'predict': {'batch_size': 32, 'interop': 4, 'intraop': 2, 'n_models': 4}
}

SPECULATIVE_EXECUTION = False
SPECULATION_MULTIPLIER = 2
SPECULATION_QUARTILE = 0.75
SPECULATION_INTERVAL_MS = 500
SPECULATION_ADD_TASK_MANAGER = True
MAX_SPLIT_RETRIES = 1
COST_PER_MS_MB = 0.000000000016276
DEFAULT_MEMORY = 1024  # in MB

KEEP_ALIVE = True
KEEP_ALIVE_INTERVAL = 2

LOGGING_FORMAT = "%(asctime)s [%(threadName)s] [%(levelname)s] [%(filename)s:%(lineno)d] [%(funcName)s] %(message)s"

EXCEPTION_PERCENTAGE = 0
DELAY_PERCENTAGE = 0
DELAY_MAX = 0

LOCAL_MODEL_PATH = "model.pt"