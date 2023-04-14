from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os

def get_files(path):
    """ A function to find all files in a directory and save the path into a list""" 
    files = []
    for r, d, f in os.walk(path):
        for file in f:
            files.append(os.path.join(r, file))
    return files


@task()
def write_gcs(bucket: str) -> None:
    """Upload files from a directory into GCS"""
    gcs_block = GcsBucket.load(bucket)
    path = '/home/antihaddock/Repos/ref_analysis/data/'
    files = get_files(path)
    # Loop over all files in the directory and subdirectory and upload to GCP
    for file in files:
         # Get a specific GCS path to store the file
        gcs_path = file.split('data/')[1]
        gcs_block.upload_from_path(from_path=file, to_path=gcs_path, timeout=None)
    return


@flow()
def etl_web_to_gcs() -> None:
    """Upload data into a GCS bucket"""
    gcs_bucket_name = "refereeing-bucket"
    write_gcs(gcs_bucket_name)


if __name__ == "__main__":
    etl_web_to_gcs()