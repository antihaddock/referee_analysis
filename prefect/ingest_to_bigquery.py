from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import pandas_gbq
import os


# @task(retries=3)
# def extract_from_gcs() -> Path:
#     """Download data from GCS to facilitate uploading into BigQuery"""
#     gcs_path = "data/"
#     gcs_block = GcsBucket.load("engineering-camp")
#     local_path = './data_engineering_camp_project/'
#     gcs_block.get_directory(from_path=gcs_path, local_path=local_path)
#     saved_path = f'{local_path}/{gcs_path}'
#     return Path(saved_path)

def get_files(path):
    """ A function to find all files in a directory and save the path into a list""" 
    files = []
    for r, d, f in os.walk(path):
        for file in f:
            files.append(os.path.join(r, file))
    return files



@task(log_prints=True)
def write_to_bq() -> None:
    """Writes a DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("referee-credentials")        
   
    files = get_files('/home/antihaddock/Repos/ref_analysis/data/')
    for file in files:
        # take the path of the file and return just its name without spaces to come up 
        # with the Big Query Table name
        bq_table_name = os.path.splitext(os.path.basename(file))[0].replace(" ", "")   
       
        if file.endswith('.csv'):
            df = pd.read_csv(file, low_memory=False)
            # Pre processing of data   
            df = df.dropna()    
        elif file.endswith('.xlsx'):
            df = pd.read_excel(file)
            df = df.applymap(lambda x: x.encode('utf-8').strip() if isinstance(x, str) else x)
        else:
            print(f"{file} is not a CSV or XLSX file.")

        # Define a dictionary that maps the old column names to the new ones
        column_mapping = {
                      "Item/Acct" : 'Position',
                      "AUTO PLUS + (ADDITIONAL MATCHES)": "Suspension",
                      "PLAYER/TEAM OFFICIAL": "PLAYEROROFFICIAL"
                     }           

        # Loop through each old column name and its corresponding new column name
        for old_col, new_col in column_mapping.items():
        # If the old column exists in the DataFrame
            if old_col in df.columns:
                df.rename(columns={old_col: new_col}, inplace=True)
        
        # overcome issues with columns not being in the right format and case everything to bytes        
        for col in df.columns:
                df[col] = df[col].astype(bytes)
                        
        ## Upload file to BQ     
        table_name = f"appointments_data_raw.{bq_table_name}"
        print(f' table name is: {table_name}')
        
        df.to_gbq(
            destination_table = table_name,
            project_id="refereeinganalysis",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            if_exists='replace'
            )

@flow()
def etl_gcs_to_bq() -> None:
    """Main ETL flow to load data into Big Query"""
    write_to_bq()

if __name__ == "__main__":
     etl_gcs_to_bq()