from db import get_connection
from schema_utils import get_table_schema

def create_log_table(table_name):
    schema = get_table_schema(table_name)
    log_table = f"{table_name}_log"

    col_defs = ", ".join([f"{col} TEXT" for col, _ in schema])  # TEXT for simplicity
    extra_cols = "action TEXT, action_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {log_table} (
        {col_defs},
        {extra_cols}
    );
    """

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()
