import logging
from config import SOURCE_DB, TARGET_DB, SELECTED_TABLES
from db_utils import get_engine
from migrate_utils import copy_all_tables, copy_selected_tables
from export_utils import export_to_csv, export_to_parquet, export_to_avro
from transform_utils import apply_transformations
import pandas as pd
import os

# Setup Logging
logging.basicConfig(filename="logs/pipeline.log", level=logging.INFO, format='%(asctime)s %(message)s')

def run_pipeline():
    logging.info("Starting data pipeline...")

    source_engine = get_engine(SOURCE_DB['url'])
    target_engine = get_engine(TARGET_DB['url'])

    for table, cols in SELECTED_TABLES.items():
        query = f"SELECT {', '.join(cols)} FROM {table}"
        df = pd.read_sql(query, con=source_engine)

        df = apply_transformations(df, table)
        df.to_sql(table, con=target_engine, index=False, if_exists='replace')

        export_to_csv(df, table)
        export_to_parquet(df, table)
        export_to_avro(df, table)

        logging.info(f"{table}: migrated and exported")

    logging.info("Data pipeline completed.")

if __name__ == "__main__":
    run_pipeline()
