from pydantic import BaseSettings


class Settings(BaseSettings):
    AWS_REGION: str = "us-east-1"
    S3_BUCKET: str = "gaia-ml-dev"

    # IMPORTANT CHANGE
    SAGEMAKER_MODEL_NAME: str | None = None

    class Config:
        env_file = ".env"


settings = Settings()
