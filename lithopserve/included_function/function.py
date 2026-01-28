import json
from .commons.resources import PredictResource
import time
import torch
import os
from .task_manager.task_manager import TaskManager
from .commons.model import OffSampleTorchscriptFork
import logging
import grpc
from .grpc_assets import split_grpc_pb2
from .grpc_assets import split_grpc_pb2_grpc

CONNECTION_RETRIES = 10

start = time.time()
jit_model = torch.jit.load("/function/bin/model.pt", torch.device('cpu'))
print(f"Model loading took {time.time() - start} seconds")
resources = PredictResource()

BUCKET = "off-sample-eu"
config_dict = {
    'load': {'batch_size': 0, 'max_concurrency': 0},
    'preprocess': {'batch_size': 0, 'num_cpus': 0},
    'predict': {'interop': 0, 'intraop': 0, 'n_models': 0}
}
manager = TaskManager(config_dict=config_dict, logging_level=logging.ERROR)
@manager.task(mode="threading")
def load(image_dict):
    result_dict = {}
    for key in image_dict:
        # print(f"Downloading image {key} from disk")
        image_data = resources.downloadimage(key, s3_bucket=BUCKET )
        # print("Downloading image finished")
        result_dict.update({key: image_data})
    return result_dict

@manager.task(mode="multiprocessing", previous=load, batch_format="bytes")
def preprocess(image_dict):
    result_dict = {}
    for key, value in image_dict.items():
        # print("Transformation started", key)
        tensor = resources.transform_image(value)
        result_dict.update({key: tensor})
        # print("Transformation finished", key)
    return result_dict


@manager.task(mode="torchscript", previous=preprocess, batch_format="tensor", jit_model=jit_model)
def predict(tensor_dicts, ensemble):
    # print("Predicting images")
    tensors = []
    for key, value in tensor_dicts.items():
        tensors.append(value)
    prediction_results = OffSampleTorchscriptFork(ensemble).predict(tensors)
    result_dict = {}
    for key, prediction_result in zip(tensor_dicts.keys(), prediction_results):
        result_dict.update({key: prediction_result})
    return result_dict


def request_split(stub, finished_urls, tid, sid):
    retries = 0
    while retries < CONNECTION_RETRIES:
        try:
            response = stub.Assign(split_grpc_pb2.splitRequest(outputs=finished_urls, tid=tid, sid=sid))
            return response
        except Exception as e:
            print(f"Failed to connect - {e}")
            print("Retrying")
            retries += 1
            time.sleep(1)
    print("Failed to connect to gRPC server")
    return None

def process_batches(batch, config_dict):
    if isinstance(batch, dict):
        batch = list(batch.items())
    input_dicts = {}
    for input in batch:
        input_dicts.update({input: None})
    prediction_dicts = manager.process_tasks(input_dicts, config_dict)
    time_log = manager.get_log_file_content()
    print(f"Finished processing: {prediction_dicts}")
    return prediction_dicts, time_log





def default_function(id, payload, storage):
    global config_dict
    global manager
    tid = None
    global BUCKET
    print("Function started")
    try:
        if "WARM_START_FLAG" in os.environ:
            is_cold_start = False
            print("Cold start")
        else:
            is_cold_start = True
            os.environ["WARM_START_FLAG"] = "True"
            print("Warm start")
        payload = payload["body"]

        # Convert payload into a dictionary if it is a string
        if isinstance(payload, str):
            print("Converting payload to dictionary")
            payload = json.loads(payload)
        print(f"Payload: {payload}")

        if 'ping' in payload:
            if payload['ping']:
                return {
                    'statusCode': 200,
                    'body': "Function just returns after loading dependencies"
                }
        if "bucket" in payload:
            BUCKET = payload["bucket"]

        if 'config' in payload.keys():
            config_dict = payload["config"]
        else:
            config_dict = None

        time_logs = []
        if 'split' in payload:
            batch = payload['split']
            prediction_dicts, time_logs = process_batches(batch, config_dict)
            result = {'predictions': prediction_dicts}
        else:
            tid = payload['tid']
            sid = None
            ip = payload['ip']
            port = payload['port']
            channel = grpc.insecure_channel(f'{ip}:{port}')
            stub = split_grpc_pb2_grpc.SPLITRPCStub(channel)
            batch = 1 # Dummy value to start the loop
            rpc_dict = split_grpc_pb2.Dict(key="", value="")
            finished_urls = [rpc_dict]
            all_results = {}
            while batch:
                response = request_split(stub, finished_urls, tid, sid)
                if response:
                    batch = response.inputs
                    sid = response.sid
                    if 'to_delay' in payload:
                        if payload['to_delay'] > 0:
                            print(f"Delaying for {payload['to_delay']} seconds")
                            time.sleep(payload['to_delay'])
                    if 'to_raise_exception' in payload:
                        if payload['to_raise_exception']:
                            print("Raising exception")
                            raise Exception("Test exception")
                    if batch:
                        prediction_dicts, time_log = process_batches(batch, config_dict)
                        time_logs.append(time_log)
                        for key, result in prediction_dicts.items():
                            rpc_dict = split_grpc_pb2.Dict(key=key, value=str(result))
                            all_results.update({key: str(result)})
                            finished_urls.append(rpc_dict)
                else:
                    batch = None
            result = {'predictions': None}
        return {
            'statusCode': 200,
            'tid': tid,
            'body': result,
            'time_log': None,
            'cold_start': is_cold_start
        }
    except Exception as e:
        return {
            'statusCode': 400,
            'tid': tid,
            'body': str(e)
        }
