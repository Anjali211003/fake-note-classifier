import os
import zipfile
import boto3
from pathlib import Path
from currencyClassifier import logger
from currencyClassifier.utils.common import get_size
from currencyClassifier.entity.config_entity import DataIngestionConfig

class DataIngestion():
    def __init__(self,config:DataIngestionConfig):
        self.config = config

    def get_file(self):
        if not os.path.exists(self.config.local_data_file):
            s3 = boto3.client('s3')
            s3 = boto3.resource(
                service_name='s3',
                region_name='us-east-1',
                aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            obj = s3.Bucket(self.config.source_bucket).Object(self.config.filename)
            filename = obj.key
            
            s3.Bucket(self.config.source_bucket).download_file(Key=self.config.filename, Filename=self.config.local_data_file)
            logger.info(f"{filename} downloaded from S3 bucket!")
        else:
            logger.info(f"File already exists of size: {get_size(Path(self.config.local_data_file))}")

    def extract_zip_file(self):
        """
        zip_file_path: str
        Extracts the zip file into the data directory
        Function returns None
        """
        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)
            