
def default_job_policy(job):
    split_size = job.split_size
    num_inputs = len(job.input)
    num_task_managers = int(num_inputs / split_size)
    job.num_task_managers = int(num_task_managers) if num_task_managers == int(num_task_managers) else int(num_task_managers) + 1
    return job

def no_policy(job):
    return job


def job_policy_1(job):
    num_task_managers = job.num_task_managers
    num_inputs = len(job.input)
    split_size = num_inputs / num_task_managers
    job.split_size = int(split_size) if split_size == int(split_size) else int(split_size) + 1
    return job

def job_policy_2(job):
    if len(job.input)  <  job.split_size:
        job.split_size = len(job.input)

    num_task_managers = len(job.input) / job.split_size
    num_task_managers = int(num_task_managers) if num_task_managers == int(num_task_managers) else int(num_task_managers) + 1

    if num_task_managers > job.num_task_managers:
        num_task_managers = job.num_task_managers
    job.num_task_managers = num_task_managers
    return job


def job_policy_3(job):
    '''
    '''

    config_dict = {
        1: {"split_size": 1, "num_task_managers": 1},
        2: {"split_size": 1, "num_task_managers": 1},
        4: {"split_size": 1, "num_task_managers": 1},
        8: {"split_size": 1, "num_task_managers": 1},
        16: {"split_size": 2, "num_task_managers": 2},
        32: {"split_size": 2, "num_task_managers": 2},
        64: {"split_size": 4, "num_task_managers": 2},
        128: {"split_size": 4, "num_task_managers": 2},
        256: {"split_size": 4, "num_task_managers": 2},
        512: {"split_size": 4, "num_task_managers": 2},
        1024: {"split_size": 8, "num_task_managers": 2},
        2048: {"split_size": 8, "num_task_managers": 2},
        4096: {"split_size": 8, "num_task_managers": 2},
        8192: {"split_size": 8, "num_task_managers": 2},
        16384: {"split_size": 8, "num_task_managers": 2},
        32768: {"split_size": 16, "num_task_managers": 2},
        65536: {"split_size": 16, "num_task_managers": 2},
        131072: {"split_size": 16, "num_task_managers": 2},
        262144: {"split_size": 32, "num_task_managers": 2},
        524288: {"split_size": 32, "num_task_managers": 2},
        1048576: {"split_size": 32, "num_task_managers": 2},
    }

    input_sizes = list(config_dict.keys())

    def nearest(input_sizes, num_inputs):
        nearest = None
        nearest_num = None
        min_difference = float('inf')

        for num in input_sizes:
            difference = abs(num - num_inputs)
            if difference < min_difference:
                min_difference = difference
                nearest_num = num
            if difference == 0:
                nearest = num
                break

        return nearest

    configs = config_dict[nearest(input_sizes, len(job.input))]

    # Assign the parameters to the job if they are not already assigned
    if not job.split_size:
        job.split_size = configs["split_size"]
    if not job.num_task_managers:
        job.num_task_managers = configs["num_task_managers"]

    return job
