import boto3
import random
import string
import json
import time
from flask import current_app
from datetime import datetime


def get_aws_sessoin():
    return boto3.Session(aws_access_key_id=current_app.config.get(
        "AWS_ACCESS_KEY"), aws_secret_access_key=current_app.config.get("AWS_SECRET_KEY"),
        region_name=current_app.config.get("AWS_REGION"))


def get_rand_str():
    return "".join(random.choice(
        string.ascii_lowercase + string.digits) for _ in range(10))

def s3_upload_file(file):
    s3_client = get_aws_sessoin().client("s3")
    rand_str = get_rand_str()
    file_name = datetime.now().strftime("%H-%M-%S_%d-%m-%y") + str(rand_str)
    s3_client.put_object(Bucket=current_app.config.get(
        "S3_INPUT_BUCKET_NAME"), Body=file, Key=file_name)
    return file_name


def mediaconvert_create_job(filename):
    mc_client = get_aws_sessoin().client('mediaconvert')
    endpoint = mc_client.describe_endpoints()["Endpoints"][0]["Url"]
    mc_client = get_aws_sessoin().client('mediaconvert', endpoint_url=endpoint)
    with open("configs/media_convert_job.json", "r") as job_file:
        job_data = json.load(job_file)
        input_file_url = f"s3://{current_app.config.get('S3_INPUT_BUCKET_NAME')}/{filename}"
        output_bucket_url = f"s3://{current_app.config.get('S3_OUTPUT_BUCKET_NAME')}/"
        job_data['Settings']['OutputGroups'][0]['OutputGroupSettings']['FileGroupSettings']['Destination'] = output_bucket_url
        job_data['Settings']['Inputs'][0]['FileInput'] = input_file_url

    response = mc_client.create_job(**job_data)
    job_id = response["Job"]["Id"]
    for i in range(25):
        job_data = mc_client.get_job(Id=job_id)["Job"]
        if(job_data["Status"] == "COMPLETE"):
            break

        if i == 24:
            return job_data["ErrorMessage"]
        time.sleep(1)
    return True


def invalidate_cloudfront():
    cf_client = get_aws_sessoin().client('cloudfront')
    dist_id = current_app.config.get('CLOUDFRONT_DIST_ID')
    repsonse = cf_client.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={
            'Paths': {
                'Quantity': 1,
                'Items': [
                    '/*.mp4',
                ]
            },
            'CallerReference': get_rand_str()
        }
    )
    waiter = cf_client.get_waiter('invalidation_completed')
    domain = cf_client.get_distribution(Id=dist_id)["Distribution"]["DomainName"]
    return domain
