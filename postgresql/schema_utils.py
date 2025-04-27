from .db import get_connection

def get_table_schema(table_name):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
    """, (table_name,))
    schema = cur.fetchall()
    cur.close()
    conn.close()
    return schema

def get_primary_keys(table_name):
    conn = get_connection()
    cur = conn.cursor()
    print(f"Fetching primary keys for table: {table_name}")
    cur.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
    """, (table_name,))
    pks = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return pks
