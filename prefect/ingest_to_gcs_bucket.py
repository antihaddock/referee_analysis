from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task()
def write_gcs(bucket: str) -> None:
    """Upload local csv file to GCS"""
    gcs_block = GcsBucket.load(bucket)
    path = '/home/antihaddock/Repos/data_engineering_camp_project/data/research_payments.csv'
    gcs_path = '/data/'
    gcs_block.upload_from_path(from_path=path, to_path=gcs_path, timeout=None)
    return


@flow()
def etl_web_to_gcs() -> None:
    """Upload data into a GCS bucket"""
    gcs_bucket_name = "engineering-camp"
    write_gcs(gcs_bucket_name)


if __name__ == "__main__":
    etl_web_to_gcs()