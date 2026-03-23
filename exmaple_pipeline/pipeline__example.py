# IMPORTS
import dlt
from prefect import flow, task, get_run_logger
import pandas as pd
import warnings
from pathlib import Path
print("working")

LOCAL_DIR = Path("C:/Users/Adem/Projects/DE_projects/Data")
FILENAME = "data_test.csv"

def single_table_load(tablename: str, filepath: str):
    """ Creates a DLT pipeline and loads one csv into DuckDB 
        Note entire csv must load in memory                 """
    
    # Reading csv file into dataframe
    df = pd.read_csv(filepath)

    # Configure Pipeline
    pipeline = dlt.pipeline(
        pipeline_name = "example",
        destination = "duckdb",
        dataset_name = "example_schema",
        progress = "log"
    )

    # Run Pipeline
    pipeline.run(
        df,
        table_name = tablename,
        write_disposition =  "replace",
        refresh = "drop_resources"
    )

    return len(df)

@task(name="load data example")
def load_data_task(filepath: str):
    """ Run single table load function and create logs"""

    logger = get_run_logger()
    logger.info(f"Starting example file load from {filepath} ")
    try:
        rowcount = single_table_load(
            tablename = "example",
            filepath = filepath
        )
        logger.info(f"Successful load. Row count is {rowcount}")
        print(f"Data working {rowcount}")

    except Exception as e:
        logger.exception(f"Failed to load: {e}")
        raise
    
@flow(name="flow example")
def example_flow():
    """ Run flow datatask"""

    # Get path
    example_path = f"{LOCAL_DIR}/{FILENAME}"
    # 
    load_data_task(example_path)

if __name__ == "__main__":
    example_flow()