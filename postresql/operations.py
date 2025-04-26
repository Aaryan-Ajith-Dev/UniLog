from db import get_connection
from schema_utils import get_primary_keys
from log_table_manager import create_log_table

def set_row(table_name, row_dict):
    create_log_table(table_name)
    pks = get_primary_keys(table_name)
    
    cols = list(row_dict.keys())
    values = [row_dict[col] for col in cols]
    update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols])
    
    insert_sql = f"""
    INSERT INTO {table_name} ({','.join(cols)})
    VALUES ({','.join(['%s'] * len(values))})
    ON CONFLICT ({','.join(pks)}) DO UPDATE SET
    {update_set};
    """

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(insert_sql, values)
    
    # Log the operation
    log_sql = f"""
    INSERT INTO {table_name}_log ({','.join(cols)}, action)
    VALUES ({','.join(['%s'] * len(values))}, 'SET')
    """
    cur.execute(log_sql, values)
    
    conn.commit()
    cur.close()
    conn.close()

def get_row(table_name, filters):
    create_log_table(table_name)
    where_clause = " AND ".join([f"{col} = %s" for col in filters])
    values = list(filters.values())

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name} WHERE {where_clause}", values)
    result = cur.fetchall()

    # Log the operation
    log_sql = f"""
    INSERT INTO {table_name}_log ({','.join(filters)}, action)
    VALUES ({','.join(['%s'] * len(values))}, 'GET')
    """
    cur.execute(log_sql, values)

    conn.commit()
    cur.close()
    conn.close()
    return result
