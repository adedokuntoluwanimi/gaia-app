# app/core/config.py

from pydantic import BaseSettings


class Settings(BaseSettings):
    AWS_REGION: str = "us-east-1"
    S3_BUCKET: str = "gaia-ml-dev"

    SAGEMAKER_MODEL_NAME: str
    SAGEMAKER_INSTANCE_TYPE: str = "ml.m5.large"


settings = Settings()
