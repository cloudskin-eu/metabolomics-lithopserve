import json
import threading

from commons.resources import PredictResource
import time
import sys
import torch
import os
from task_manager.task_manager import TaskManager
from commons.model import OffSampleTorchscriptFork
import logging
import grpc
from grpc_assets import split_grpc_pb2
from grpc_assets import split_grpc_pb2_grpc

CONNECTION_RETRIES = 5
start = time.time()
# Get working directory
cwd = os.getcwd()
import os
current_dir = os.path.dirname(os.path.realpath(__file__))
jit_model = torch.jit.load('/tmp/lithopserve/model.pt', torch.device('cpu'))
resources = PredictResource()

config_dict = {
               'load': {'batch_size': 0, 'max_concurrency': 0},
               'preprocess': {'batch_size': 0, 'num_cpus': 0},
               'predict': {'batch_size': 0,'interop':0, 'intraop': 0, 'n_models': 0}
}
manager = TaskManager(config_dict=config_dict, logging_level=logging.ERROR)
@manager.task(mode="threading")
def load(image_dict):
    result_dict = {}
    for key in image_dict:
        # print(f"Downloading image {key} from disk")
        image_data = resources.downloadimage(key, None)
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
            response = stub.Assign(split_grpc_pb2.splitRequest(outputs=finished_urls, tid=tid, sid=sid, keep_alive=False))
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
    return prediction_dicts, time_log


def send_liveness_check_thread(stub, tid, keepalive_interval=1):
    stop_server_flag = threading.Event()
    def send_liveness_check():
        while not stop_server_flag.is_set():
            try:
                # Send a liveness check request to the server
                response = stub.Assign(split_grpc_pb2.splitRequest(outputs=[], tid=tid, sid=None, keep_alive=True))
            except grpc.RpcError as e:
                print("Error sending liveness check:", e)
                return
            # Sleep for 1 second before sending the next liveness check
            time.sleep(keepalive_interval)

    # Start the thread
    liveness_check_thread = threading.Thread(target=send_liveness_check, daemon=True)
    liveness_check_thread.start()
    return stop_server_flag


def default_function(payload):
    global config_dict
    global manager
    tid = None
    try:
        payload = payload["body"]

        if 'do_nothing' in payload:
            if payload['do_nothing']:
                return {
                    'statusCode': 200,
                    'body': "Function just returns after loading dependencies"
                }
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
            keep_alive = payload['keep_alive']
            if keep_alive:
                keep_alive_interval = payload['keep_alive_interval']
                stop_server_flag = send_liveness_check_thread(stub = stub, tid = tid, keepalive_interval=keep_alive_interval)
            batch = 1 # Dummy value to start the loop
            rpc_dict = split_grpc_pb2.Dict(key="", value="")
            finished_urls = [rpc_dict]
            all_results = {}
            while batch:
                response = request_split(stub, finished_urls, tid, sid)
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
            result = {'predictions': None}
        return {
            'statusCode': 200,
            'tid': tid,
            'body': result,
            'time_logs': None
        }
    except Exception as e:
        return {
            'statusCode': 400,
            'tid': tid,
            'body': str(e)
        }


if __name__ == '__main__':
    json_payload = sys.argv[1]
    payload = json.loads(json_payload)
    result = default_function(payload)
    manager.shutdown_task_manager()
    print(result)

