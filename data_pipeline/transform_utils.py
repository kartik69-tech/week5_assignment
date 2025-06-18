def apply_transformations(df, table_name):
    if table_name == "sales":
        df = df[df['amount'] > 1000]
    return df
