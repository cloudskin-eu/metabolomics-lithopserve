# client.py
import os
import time

import requests

DATASET_SIZE = 128

dataset = "2024-02-29_03h38m10s"
with open(f"../datasets/{dataset}.txt") as f:
    urls = f.read().splitlines()
if DATASET_SIZE:
    urls = urls[:DATASET_SIZE]

date_time = time.strftime("%Y-%m-%d_%Hh%Mm%Ss", time.localtime())

response = requests.post("http://127.0.0.1:5556/enqueue", json={'inputs':urls,'job_name': f'{dataset} {date_time}'})
print(response.json())

