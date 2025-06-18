SOURCE_DB = {
    "url": "mysql+pymysql://root:Love300#kartik69@localhost/source_db"
}

TARGET_DB = {
    "url": "mysql+pymysql://root:Love300#kartik69@localhost/target_db"
}

EXPORT_PATH = "./exports/"

SELECTED_TABLES = {
    "employees": ["id", "name"],
    "sales": ["date", "amount"]
}
