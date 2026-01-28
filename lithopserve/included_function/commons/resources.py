import requests
import boto3
import botocore
import io
import torch.nn.functional as F
import torch
from PIL import Image
from torchvision import transforms

class PredictResource:
    def __init__(self):
        client_config = botocore.config.Config(
            max_pool_connections=1000,
        )
        self.s3_client = boto3.client('s3', config=client_config)

    def loadimage(self, image):
        with open(image, 'rb') as file:
            file_content = file.read()
            return file_content

    def downloadimage(self, key, s3_bucket):
        if 'http' in key:
            response = requests.get(key)
            object_data = response.content
        else:
            response = self.s3_client.get_object(Bucket=s3_bucket, Key=key)
            object_data = response['Body'].read()
        return object_data

    def transform_image(self, image_data):
        composed_transforms = transforms.Compose([
            transforms.ToTensor(),
            transforms.Lambda(lambda x: F.grid_sample(x.unsqueeze(0), torch.nn.functional.affine_grid(
                torch.eye(2, 3, dtype=torch.float32).unsqueeze(0), [1, 3, 224, 224], True), mode='bilinear',
                                                      padding_mode='reflection', align_corners=True)),
            transforms.Lambda(lambda x: x.squeeze(0)),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])
        image = Image.open(io.BytesIO(image_data)).convert('RGB')
        transformed_img = composed_transforms(image)
        return transformed_img
