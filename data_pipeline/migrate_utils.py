import pandas as pd
from config import SELECTED_TABLES

def copy_all_tables(source_engine, target_engine):
    tables = source_engine.table_names()
    for table in tables:
        df = pd.read_sql_table(table, con=source_engine)
        df.to_sql(table, con=target_engine, index=False, if_exists='replace')

def copy_selected_tables(source_engine, target_engine):
    for table, columns in SELECTED_TABLES.items():
        query = f"SELECT {', '.join(columns)} FROM {table}"
        df = pd.read_sql(query, con=source_engine)
        df.to_sql(table, con=target_engine, index=False, if_exists='replace')
