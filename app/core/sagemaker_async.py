# app/core/sagemaker_async.py

import time
import boto3
from app.core.config import settings

sm = boto3.client("sagemaker", region_name=settings.AWS_REGION)


def trigger_inference(job_id: str) -> str:
    import traceback

    job_name = f"gaia-infer-{job_id}-{int(time.time())}"

    try:
        sm.create_transform_job(
            TransformJobName=job_name,
            ModelName=settings.SAGEMAKER_MODEL_NAME,
            TransformInput={
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": f"s3://{settings.S3_BUCKET}/jobs/{job_id}/input/",
                    }
                },
                "ContentType": "text/csv",
            },
            TransformOutput={
                "S3OutputPath": f"s3://{settings.S3_BUCKET}/jobs/{job_id}/output/",
                "AssembleWith": "Line",
            },
            TransformResources={
                "InstanceType": settings.SAGEMAKER_INSTANCE_TYPE,
                "InstanceCount": 1,
            },
        )

    except Exception as e:
        print("=== SAGEMAKER CREATE TRANSFORM JOB FAILED ===")
        print(str(e))
        traceback.print_exc()
        raise

    return job_name
