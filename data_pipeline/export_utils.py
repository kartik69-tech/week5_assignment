import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
from config import EXPORT_PATH

def export_to_csv(df, table):
    df.to_csv(f"{EXPORT_PATH}{table}.csv", index=False)

def export_to_parquet(df, table):
    table_arrow = pa.Table.from_pandas(df)
    pq.write_table(table_arrow, f"{EXPORT_PATH}{table}.parquet")

def export_to_avro(df, table):
    import fastavro
    import datetime
    import pandas as pd

    # Convert datetime/date columns to string for Avro compatibility
    for col in df.columns:
        if isinstance(df[col].iloc[0], (datetime.date, datetime.datetime)):
            df[col] = df[col].astype(str)

    records = df.to_dict(orient='records')

    def infer_avro_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return ["null", "int"]
        elif pd.api.types.is_float_dtype(dtype):
            return ["null", "float"]
        elif pd.api.types.is_bool_dtype(dtype):
            return ["null", "boolean"]
        else:
            return ["null", "string"]

    schema = {
        "doc": f"Exported {table} Table",
        "name": f"{table}_record",
        "namespace": "example.avro",
        "type": "record",
        "fields": [
            {"name": col, "type": infer_avro_type(dtype)}
            for col, dtype in df.dtypes.items()
        ]
    }

    with open(f"exports/{table}.avro", "wb") as out:
        fastavro.writer(out, schema, records)


