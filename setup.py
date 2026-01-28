#!/usr/bin/env python
from setuptools import setup, find_packages
from itertools import chain


install_requires = [
    'flask',
    'Click',
    'tabulate',
    'pillow',
    'numpy',
    'pandas',
    'PyYAML',
    'python-dateutil',
    'pika',
    'tqdm',
    'lxml',
    'tblib',
    'docker',
    'boto3',
    'requests',
    'seaborn',
    'paramiko',
    'matplotlib',
    'kubernetes',
    'ibm-cos-sdk',
    'ibm-code-engine-sdk',
    'redis',
    'ibm-vpc',
    'cloudpickle',
    'tblib',
    'ps-mem',
    'psutil',
    'grpcio==1.51.1',
    'protobuf==4.21.12',
]


extras_require = {
    'gcp': [
        'httplib2',
        'google-cloud-storage',
        'google-cloud-pubsub',
        'google-api-python-client',
        'google-auth'
    ],
    'aliyun': [
        'aliyun-fc2',
        'oss2'
    ],
    'azure': [
        'azure-mgmt-resource',
        'azure-mgmt-compute',
        'azure-mgmt-network',
        'azure-identity',
        'azure-storage-blob',
        'azure-storage-queue'
    ],
    'multiprocessing': [
        'pynng'
    ],
    'joblib': [
        'joblib',
        'diskcache',
        'numpy'
    ],
    'oracle': [
        'oci',
    ]
}

extras_require["all"] = list(set(chain.from_iterable(extras_require.values())))


# how to get version info into the project
exec(open('lithopserve/version.py').read())
setup(
    name='lithopserve',
    version=__version__,
    url='https://github.com/cloudskin-eu/metabolomics-lithopserve',
    author='Josep Calero',
    description='An orchestrator of serverless functions for image classification integrated into Lithops',
    author_email='josep.calero@urv.cat',
    packages=find_packages(),
    install_requires=install_requires,
    dependency_links=[
        'https://download.pytorch.org/whl/cpu/torch-2.0.1%2Bcpu-cp311-cp311-linux_x86_64.whl',
        'https://download.pytorch.org/whl/cpu/torchvision-0.15.2%2Bcpu-cp311-cp311-linux_x86_64.whl'
    ],
    extras_require=extras_require,
    include_package_data=True,
    entry_points='''
        [console_scripts]
        lithopserve=lithopserve.scripts.cli:lithops_cli
    ''',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
    ],
    python_requires='>=3.6',
)