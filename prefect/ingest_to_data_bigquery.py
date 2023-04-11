from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs() -> Path:
    """Download data from GCS to facilitate uploading into BigQuery"""
    gcs_path = "data/research_payments.csv"
    gcs_block = GcsBucket.load("engineering-camp")
    local_path = './data_engineering_camp_project/'
    gcs_block.get_directory(from_path=gcs_path, local_path=local_path)
    saved_path = f'{local_path}/{gcs_path}'
    return Path(saved_path)


@task(log_prints=True)
def write_to_bq(df: pd.DataFrame) -> None:
    """Writes a DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("data-engineering-creds")
    df.to_gbq(
        destination_table = "healthcare_payments_raw.ingest",
        project_id="datazoomcap2023",
        credentials=gcp_credentials_block.get_credentials_from_service_account()
    )
    

@flow()
def etl_gcs_to_bq() -> None:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs()
    df = pd.read_csv(path, low_memory=False)
    write_to_bq(df)

if __name__ == "__main__":
     etl_gcs_to_bq()