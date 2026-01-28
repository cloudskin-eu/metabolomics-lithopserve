# # Lithops Serve

Lithops Serve expands Lithops capabilities to serving of AI models. It manages model deployment to serverless backends and orchestrates batched inference workloads across distributed workers.  


---
## Overview

Lithops Serve introduces:
- A central orchestrator for managing inference jobs
- Task Managers that execute preprocessing and inference
- Support for local, AWS Lambda, and Kubernetes backends
- Configurable batching, parallelism, and execution policies

---

## Requirements and Environment Setup

- Python **>3.8**
- Conda is recommended

### Conda setup (recommended)

    mkdir -p ~/miniconda3
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
    rm -rf ~/miniconda3/miniconda.sh

    ~/miniconda3/bin/conda init bash
    conda create -n python311 python=3.11

---

## Installation

### Build and install locally

    python -m pip install --upgrade build
    python -m build
    pip install dist/lithopserve-*.whl

### Install from GitHub

    pip install git+https://github.com/cloudskin-eu/metabolomics-lithopserve

---

## PyTorch (CPU-only)

    pip install torch==2.1.0 torchvision==0.16.0 --index-url https://download.pytorch.org/whl/cpu

---

## Examples

Examples are available in the `examples/` directory and cover:
- Local execution
- AWS Lambda backend
- Kubernetes backend

---
## Architecture

Lithops Serve follows a master–worker architecture composed of an **orchestrator** and multiple **task managers**. The orchestrator is responsible for job-level coordination, while task managers execute the actual data processing and inference.

---

### High-level Execution Flow

1. A user defines a **job** over a dataset.
2. The job is submitted to the **orchestrator**.
3. The orchestrator splits the dataset into smaller **splits**.
4. Each split is assigned to a **task manager**.
5. Task managers execute the task pipeline and return or store results.

Both the split size and the level of parallelism are configurable.

---

### Task Manager

A **Task Manager** is the code executed inside each function invocation (e.g. local process, AWS Lambda, Kubernetes pod).

It defines a fixed pipeline of tasks, typically:
- Download
- Preprocess
- Inference

Key characteristics:
- Inputs flow **sequentially** through the task pipeline
- Each task can use **threading or multiprocessing**
- The user controls the **parallelism level per task**
- A task manager processes one split at a time

---

### Orchestrator

The **orchestrator** is responsible for:
- Splitting datasets into jobs and splits
- Scheduling and invoking task managers
- Tracking job and split status
- Enforcing resource limits and execution policies

The orchestrator can manage multiple jobs concurrently and ensures that system-wide limits are respected.

---

## Orchestrator Initialization

The orchestrator currently supports the following execution backends:
- Local
- AWS Lambda

### Example

    from lithopserve import Orchestrator

    orchestrator = Orchestrator(
        fexec_args={'runtime': 'off_sample_311', 'runtime_memory': 3008},
        initialize=False
    )

### Key parameters

- **fexec_args**: Dictionary of Lithops `FunctionExecutor` arguments
- **orchestrator_backend**: Backends to be used (default: ['aws_lambda', 'local'])
- **initialize**: If True, backend resources are deployed if needed
- **job_policy**: Policy that defines how splits are scheduled (default: default)
- **ec2_host_machine**: Deploy Lambda in the same VPC, subnet, and security group
- **max_job_managers**: Maximum number of simultaneous jobs
- **max_task_managers**: Maximum number of simultaneous task managers

---

## Task Manager Deployment

- **Local backend**: No deployment required
- **AWS Lambda backend**: Task managers must be deployed

Deployment can be done automatically by setting `initialize=True` or manually.

### Manual deployment example

    from lithopserve import Orchestrator

    orchestrator = Orchestrator(
        fexec_args={'runtime': 'off_sample_311', 'runtime_memory': 3008},
        initialize=False
    )
    orchestrator.redeploy_runtime()

---

## Job Definition

A **job** represents an inference request over a dataset.

### Creating a job

    job = Job(
        urls,
        job_name,
        orchestrator_backend="aws_lambda",
        split_size=split_size,
        num_task_managers=num_task_managers
    )

### Key job parameters

- **input**: List of URLs to process
- **job_name**: Name of the job
- **bucket**: Source bucket (if omitted, files are downloaded from the internet)
- **split_size**: Number of inputs per split
- **num_task_managers**: Number of task managers to use
- **orchestrator_backend**: Backend used for execution (default: aws_lambda)
- **speculation_enabled**: Reassign stuck splits to other task managers
- **keep_alive**: Task managers periodically ping the orchestrator
- **output_storage**: Storage backend for results (local or s3)
- **output_location**: Local path or object storage path
- **output_bucket**: Destination bucket for results

---

### Split and Resource Resolution Rules

Even if `split_size` and `num_task_managers` are explicitly set, the orchestrator applies the following rules in order:

1. **Default values**  
   Used when parameters are not specified  
   See `constants_lithopserve.py`

2. **Job policy**  
   Defines how many splits are created and how they are scheduled  
   - Default policy: one split per task manager  
   - Other policies may create more splits than task managers  
   - Task managers can request additional splits dynamically  
   See `job_policies.py`

3. **Maximum limits**  
   The final number of task managers is capped by system-wide limits


### Job Submission

Jobs must be submitted to the orchestrator to start execution.

#### Run synchronously

    result = orchestrator.run_job(job)

Blocks until the job finishes and returns results.

#### Enqueue asynchronously

    orchestrator.enqueue_job(job)

- Supports multiple jobs executing concurrently
- Results must be stored to disk or cloud storage
- Output configuration is defined in the job


# Acknowledgements
<img width="80px" src="https://cloudskin.eu/assets/img/europe.jpg" alt="European flag" />

CLOUDSKIN has received funding from the European Union’s Horizon research and innovation programme under grant agreement No 101092646.
https://cloudskin.eu
