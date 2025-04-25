import csv
import os
from pyhive import hive
import pandas as pd
import re
import ast
import tempfile

class HiveSystem:
    def __init__(self, host='localhost', port=10000, database='default'):
        """
        Initialize the Hive system with connection parameters and set up the operation log.
        
        Args:
            host (str): Hive server host
            port (int): Hive server port
            database (str): Database name
        """
        self.host = host
        self.port = port
        self.database = database
        self.connection = None
        self.cursor = None
        self.oplog_file = "oplog.hiveql"

        # In-memory cache of timestamps for each (student_id, course_id) pair
        self.timestamp_cache = {}

        # Initialize table-related attributes
        self.table_name = None
        self.all_columns = []


        
    def connect(self):
        """Establish connection to the Hive server"""
        try:
            self.connection = hive.Connection(
                host=self.host,
                port=self.port,
                database=self.database,
                username='',  # Add username if required
            )
            self.cursor = self.connection.cursor()
            print("Connected to Hive successfully")
        except Exception as e:
            print(f"Error connecting to Hive: {e}")
            raise
    
    def disconnect(self):
        """Close the connection to Hive"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Disconnected from Hive")

    def build_timestamp_cache(self, prime_attr):
        """
        Build the initial timestamp cache using dumped data.
        Assumes all dumped rows have custom_timestamp = 0.
        
        Args:
            prime_attr (list): List of attribute names (e.g., ['student_id', 'course_id'])
        """
        try:
            if not hasattr(self, "timestamp_cache"):
                self.timestamp_cache = {}

            # Compose the SELECT query to get distinct combinations of prime attributes
            group_by_cols = ", ".join(prime_attr+ ['custom_timestamp'])
            query = f"""
            SELECT {group_by_cols}
            FROM {self.table_name}
            """
            print(f"Executing timestamp cache init query: {query}")
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            print(results)
            
            for row in results:
                key = tuple(str(val) for val in row[:-1])
                time_stamp = row[-1]
                val = self.timestamp_cache.get(key, -1)
                if time_stamp > val:
                    self.timestamp_cache[key] = time_stamp

                
            print("✅ Timestamp cache initialized with dumped data.")
            print(f"Cached {len(self.timestamp_cache)} prime key combinations.")

        except Exception as e:
            print(f"❌ Failed to build timestamp cache: {e}")

    
    def set_timestamp_cache(self, key, timestamp):
        """
        Set the timestamp cache to map keys to their latest timestamps.
        If the key exists, the timestamp is updated; if not, the key is added with the provided timestamp.
        """
        try:
            self.timestamp_cache[key] = timestamp
            print(f"Assigned timestamp for key {key} with value {timestamp}.")
                
        except Exception as e:
            print(f"Error updating timestamp cache: {e}")


    
    def load_data_from_csv(self, csv_file, recreate=False):
        """
        Load data from CSV file into Hive table, adding a default timestamp of 0 for each row.
        """
        if not recreate:
            print("Data already loaded. Skipping...")
            return
        try:

            with open(csv_file, 'r') as f:
                lines = f.readlines()
                data_lines = lines[1:]  # skip header
                temp_csv = tempfile.NamedTemporaryFile(delete=False, mode='w')
                temp_csv.writelines(data_lines)
                temp_csv.close()

            self.cursor.execute("SET hive.auto.convert.join=false;")
            self.cursor.execute("SET hive.exec.mode.local.auto=true;")


            # Drop tables if they exist
            self.cursor.execute("DROP TABLE IF EXISTS student_course_grades")
            self.cursor.execute("DROP TABLE IF EXISTS student_course_grades_staging")

            # Create final table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS student_course_grades (
                student_id STRING,
                course_id STRING,
                roll_no STRING,
                email_id STRING,
                grade STRING,
                custom_timestamp INT
            )
            STORED AS TEXTFILE
            LOCATION '/home/sohith/Desktop/nosql/project/UniLog/hive/tmp/student_course_grades/'
            """
            self.cursor.execute(create_table_query)

            # Create staging table
            create_staging_table_query = """
            CREATE TABLE IF NOT EXISTS student_course_grades_staging (
                student_id STRING,
                course_id STRING,
                roll_no STRING,
                email_id STRING,
                grade STRING
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
                "separatorChar" = ","
            )
            STORED AS TEXTFILE
            LOCATION '/home/sohith/Desktop/nosql/project/UniLog/hive/tmp/student_course_grades_staging/'
            TBLPROPERTIES ("skip.header.line.count"="1")
            """
            self.cursor.execute(create_staging_table_query)

            # Load data into staging table
            load_data_query = f"""
            LOAD DATA LOCAL INPATH '{csv_file}'
            INTO TABLE student_course_grades_staging
            """
            self.cursor.execute(load_data_query)

            # Insert into final table with custom_timestamp = 0
            insert_query = """
            INSERT INTO TABLE student_course_grades
            SELECT student_id, course_id, roll_no, email_id, grade, 0
            FROM student_course_grades_staging
            """
            self.cursor.execute(insert_query)

            # Drop staging table
            self.cursor.execute("DROP TABLE IF EXISTS student_course_grades_staging")

            # self.build_timestamp_cache()
            print(f"✅ Successfully loaded data from {csv_file} into Hive table 'student_course_grades'")

            

        except Exception as e:
            print(f"❌ Error loading data: {e}")



    def set_table(self, table_name):
        """
        Set the table name and fetch its schema (column names).
        
        Args:
            table_name (str): Name of the table to work with
        """
        try:
            self.table_name = table_name

            # Fetch column names
            self.cursor.execute(f"SELECT * FROM {self.table_name} LIMIT 1")
            self.all_columns = [desc[0] for desc in self.cursor.description]

            print(f"Table '{self.table_name}' set with columns: {self.all_columns}")

        except Exception as e:
            print(f"Error setting table: {e}")
            raise



    
    def get(self, key_tuple, timestamp=None):
        """
        Generic GET operation for composite keys.

        Args:
            key_tuple (tuple): Composite key as tuple of values (e.g., (student_id, course_id, ...))
            timestamp (int, optional): Operation timestamp

        Returns:
            list: Retrieved values (rows) if found, [] otherwise
        """
        try:

            if not hasattr(self, "all_columns") or not hasattr(self, "table_name"):
                raise AttributeError("Table schema not set. Call set_table(table_name) first.")

            # Extract key_columns dynamically
            key_columns = self.all_columns[:len(key_tuple)]

            # Build dynamic WHERE clause based on key_columns and key_tuple values
            where_conditions = " AND ".join(
                f"{col} = '{value}'" for col, value in zip(key_columns, key_tuple)
            )

            query = f"SELECT * FROM {self.table_name} WHERE {where_conditions}"
            print(f"Executing query: {query}")
            self.cursor.execute(query)
            result = self.cursor.fetchall()

            # Log the GET operation
            if timestamp is not None:
                # Prepare the keys as key-value pairs for the log
                keys_dict = {col: value for col, value in zip(key_columns, key_tuple)}
                keys = [f"{key}: {value}" for key, value in keys_dict.items()]
                operation = f"GET ({', '.join(keys)})"
                
                # Call the function to log the operation into the oplog
                self.log_oplog_entry('GET', timestamp, key_tuple, None, None)

            if result:
                for row in result:
                    print(f"Retrieved: {row}")
                return result
            else:
                print(f"No entry found for key: {key_tuple}")
                return []

        except Exception as e:
            print(f"Error retrieving data: {e}")
            return []


    

            
    def set(self, key_tuple, values, set_attrs, timestamp=None, log_operation=True):
        """
        Set or insert values for multiple attributes of a composite key using INSERT only.

        Args:
            key_tuple (tuple): Composite key (e.g., (student_id, course_id))
            values (list or tuple): Values to set for corresponding attributes
            set_attrs (list): List of attribute names to be set
            timestamp (int, optional): Timestamp to insert (default = 0)
            log_operation (bool): Whether to log the operation
        """
        try:

            if not hasattr(self, "all_columns") or not hasattr(self, "table_name"):
                raise AttributeError("Table schema not set. Call set_table(table_name) first.")

            key_columns = self.all_columns[:len(key_tuple)]
            value_columns = self.all_columns[len(key_tuple):]
            key_columns = [col.split('.')[-1] for col in key_columns]
            value_columns = [col.split('.')[-1] for col in value_columns]
            all_columns = key_columns + value_columns + ['custom_timestamp']

            if timestamp is None:
                timestamp = 0

            # Build WHERE clause to check for existing row
            where_clause = " AND ".join(
                f"{col} = '{val}'" for col, val in zip(key_columns, key_tuple)
            )

            query = f"""
            SELECT * FROM {self.table_name}
            WHERE {where_clause}
            """
            self.cursor.execute(query)
            existing_row = self.cursor.fetchone()
            print(existing_row)

            # Initialize all values dict with key values
            all_values_dict = dict(zip(key_columns, key_tuple))

            # Set default NULLs for all value_columns
            for col in value_columns:
                all_values_dict[col] = None

            # If an existing row is found, preserve the existing values for the non-modified columns
            if existing_row:
                for col, val in zip(all_columns, existing_row):
                    if col not in key_columns and col != 'custom_timestamp':
                        all_values_dict[col] = val

                # If timestamp in cache is greater than the one provided, skip
                
                cache_time = self.timestamp_cache.get(key_tuple, -1)
                
                if timestamp <= cache_time:
                    print(f"⚠️ Skipping update. Timestamp {timestamp} is not newer than the cached timestamp {cache_time} for key {key_tuple}.")
                    return False

            # Set new values for the specified set_attrs
            for attr, val in zip(set_attrs, values):
                all_values_dict[attr] = val
            all_values_dict['custom_timestamp'] = timestamp

            insert_columns = all_values_dict.keys()
            insert_values = all_values_dict.values()

            insert_query = f"""
            INSERT INTO {self.table_name} ({", ".join(insert_columns)}) 
            VALUES ({", ".join("NULL" if v is None else f"'{v}'" for v in insert_values)})
            """
            print(f"Executing query: {insert_query}")
            self.cursor.execute(insert_query)

            # Log the operation if needed
            if log_operation:
                self.log_oplog_entry('SET', timestamp, key_tuple, set_attrs, values)

            # Change timestamp cache
            self.set_timestamp_cache(key_tuple, timestamp)

            print(f"✅ Successfully set {set_attrs} = {values} for key {key_tuple} with timestamp {timestamp}")
            return True

        except Exception as e:
            print(f"❌ Error setting data: {e}")
            return False


    
    def merge(self, system_name):
        """
        Merge the state with another system based on its operation log,
        applying SET operations if the other system's timestamp is newer.
        
        Args:
            system_name (str): Name of the system to merge with (e.g., SQL, MONGO)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:

            # Example: Load oplog from external source (replace with real loader)
            external_oplog = [{'timestamp': 1, 'operation': 'SET', 'table': 'student_course_grades', 'keys': {'student_id': 'SID103', 'course_id':'CSE016'}, 'item': {'grade': 'B'}}]# Expected to return list of dicts

            if not hasattr(self, "timestamp_cache"):
                self.timestamp_cache = {}

            applied_count = 0

            for entry in external_oplog:
                if entry["operation"] != "SET":
                    continue  # Only SET operations affect state

                keys = entry["keys"]
                item = entry["item"]
                timestamp = int(entry["timestamp"])
                table = entry.get("table", self.table_name)
                print(f'{keys},{item},{timestamp},{table}')
                # Validate target table
                if table != self.table_name:
                    continue    

                attribute_names = [col.split('.')[-1] for col in self.all_columns[:len(keys)]]
                # Convert keys to tuple for timestamp cache lookup
                key_tuple = tuple(keys[col] for col in attribute_names)
                cache_time = self.timestamp_cache.get(key_tuple, -1)
                print(f'{key_tuple}, {cache_time}')
                if timestamp > cache_time:
                    # Apply SET operation
                    set_attrs = list(item.keys())
                    values = list(item.values())

                    
                    success = self.set(key_tuple, values, set_attrs, timestamp=timestamp)
                    if success:
                        applied_count += 1

            print(f"✅ Merge complete. Applied {applied_count} newer SET operations from {system_name}.")
            return True

        except Exception as e:
            print(f"❌ Error merging with {system_name}: {e}")
            return False





    def parse_generic_hive_op(self, operation_str):
        """
        Parses generic HIVE.SET or HIVE.GET operation.

        - SET ((k1, k2, ..., kn), v1, v2, ..., vm)
        - GET (k1, k2, ..., kn)

        Returns:
            ("SET" or "GET", tuple of keys, tuple of values or None)
        """
        operation_str = operation_str.strip()

        # Match SET ((k1, k2, ...), v1, v2, ..., vm)
        set_match = re.match(r"SET\s*\(\(\s*(.*?)\s*\)\s*,\s*(.*)\)", operation_str)
        if set_match:
            keys = tuple(part.strip() for part in set_match.group(1).split(","))
            values = tuple(part.strip() for part in set_match.group(2).split(","))
            return ("SET", keys, values)

        # Match GET (k1, k2, ...)
        get_match = re.match(r"GET\s*\(\s*(.*?)\s*\)", operation_str)
        if get_match:
            keys = tuple(part.strip() for part in get_match.group(1).split(","))
            return ("GET", keys, None)

        return None
    
    def process_command(self, command, set_attr):
        try:
            command = command.strip()


            # Handle HIVE.MERGE
            if command.startswith("HIVE.MERGE"):
                system_name = command.split("(")[1].split(")")[0].strip()
                return self.merge(system_name)

            # Split timestamp and operation
            parts = command.split(",", 1)
            if len(parts) < 2:
                print(f"Invalid command format: {command}")
                return False

            timestamp = int(parts[0].strip())
            operation = parts[1].strip()

            if operation.startswith("HIVE."):
                parsed = self.parse_generic_hive_op(operation[5:])
                if not parsed:
                    print(f"Failed to parse HIVE operation: {operation}")
                    return False

                op_type, key_tuple, value_tuple = parsed
                print(f"Parsed operation: {op_type}, keys: {key_tuple}, values: {value_tuple}, timestamp: {timestamp}")

                if op_type == "SET":
                    return self.set(key_tuple, value_tuple,set_attr, timestamp=timestamp)
                elif op_type == "GET":
                    self.get(key_tuple, timestamp=timestamp)
                    return True

            return False

        except Exception as e:
            print(f"Error processing command: {command} - {e}")
            return False


    def create_oplog_table(self, recreate=False):
        """
        Creates the oplog table in Hive if it does not already exist.
        The table has the following schema:
            - timestamp: DOUBLE
            - operation: STRING
            - table_name: STRING
            - keys: ARRAY<STRING>
            - item: ARRAY<STRING>
        
        The data is stored as TEXTFILE format.
        """
        try:

            if recreate:
                # Drop the existing oplog table if it exists
                self.cursor.execute("DROP TABLE IF EXISTS oplog")
                print("Dropped existing oplog table.")

            create_table_query = """
            CREATE TABLE IF NOT EXISTS oplog (
                custom_timestamp INT,
                operation STRING,
                table_name STRING,
                keys ARRAY<STRING>,  -- Use array for keys
                item ARRAY<STRING>   -- Use array for item
            )
            STORED AS TEXTFILE
            LOCATION '/home/sohith/Desktop/nosql/project/UniLog/hive/tmp/oplog/'
            """

            print(f"Executing query: {create_table_query}")
            self.cursor.execute(create_table_query)

            print("✅ Successfully created the oplog table.")
        except Exception as e:
            print(f"❌ Error creating oplog table: {e}")


    def log_oplog_entry(self, operation, timestamp, key_tuple, set_attrs, values):
        """
        Log the operation into the oplog in the specified format for Hive using arrays for keys and item.

        Args:
            operation (str): The operation being logged (e.g., 'SET').
            timestamp (int): The timestamp of the operation.
            key_tuple (tuple): The composite key values.
            set_attrs (list): The attributes being set.
            values (list): The corresponding values for the attributes.
        """
        try:

            # Build the keys array (composite keys)
            keys_array = [f"{key}: {val}" for key, val in zip(self.all_columns[:len(key_tuple)], key_tuple)]

            # Build the item array (set attributes and values)
            if set_attrs is None and values is None:
                item_array = []
            else:
                item_array = [f"{attr}: {val}" for attr, val in zip(set_attrs, values)]

            # Hive insert query
            insert_query = f"""
            INSERT INTO oplog (custom_timestamp, operation, table_name, keys, item)
            VALUES ({timestamp}, '{operation}', '{self.table_name}', 
                    array({', '.join(f"'{k}'" for k in keys_array)}), 
                    array({', '.join(f"'{i}'" for i in item_array)}))
            """
            
            # Execute the insert query
            self.cursor.execute(insert_query)

            print(f"✅ Successfully logged operation to oplog: {keys_array}, {item_array}")
        except Exception as e:
            print(f"❌ Error logging operation: {e}")


    def load_timecache(self, recreate=False):
        """
        Load the timestamp cache from the database table into the timestamp_cache dictionary.
        """
        if recreate:
            hive_system.build_timestamp_cache(key,recreate)
            print("✅ Loaded timestamp cache from dumped data.")
            return
        try:
            load_query = "SELECT key_tuple, custom_timestamp FROM timestamp_cache_table"
            self.cursor.execute(load_query)
            rows = self.cursor.fetchall()

            for row in rows:
                key_str, timestamp = row
                # Convert key_str back to tuple using eval() (careful with eval)
                key_tuple = eval(key_str)
                self.timestamp_cache[key_tuple] = timestamp
                
            print("✅ Successfully loaded timecache from database.")

        except Exception as e:
            print(f"❌ Error loading timecache: {e}")

    def get_oplog(self):
        '''Sends the oplog data to other systems'''

        try:
            # Fetch oplog entries
            self.cursor.execute("SELECT * FROM oplog")
            rows = self.cursor.fetchall()

            oplog_data = []
            for row in rows:
                entry = {
                    "timestamp": row[0],
                    "operation": row[1],
                    "table_name": row[2],
                    "keys": {}
                }
                oplog_data.append(entry)

            return oplog_data

        except Exception as e:
            print(f"❌ Error fetching oplog data: {e}")
            return []
        
    # Convert '["k1: v1", "k2: v2"]' to dict: {'k1': 'v1', 'k2': 'v2'}
    def parse_key_value_list(self,kv_list_str):
        kv_dict = {}
        try:
            kv_list = ast.literal_eval(kv_list_str)
            for pair in kv_list:
                if ":" in pair:
                    key, value = pair.split(":", 1)
                    kv_dict[key.strip()] = value.strip()
        except Exception as e:
            print(f"❌ Error parsing keys/items: {e}")
        return kv_dict
        

    def get_oplog(self):
        '''Sends the oplog data to other systems'''
        try:
            # Fetch oplog entries
            self.cursor.execute("SELECT * FROM oplog")
            rows = self.cursor.fetchall()

            oplog_data = []
            for row in rows:
                timestamp, operation, table_name, keys_str, item_str = row

                

                keys_dict = self.parse_key_value_list(keys_str)
                items_dict = self.parse_key_value_list(item_str)

                entry = {
                    "timestamp": timestamp,
                    "operation": operation,
                    "table": table_name,
                    "keys": keys_dict,
                    "item": items_dict
                }

                oplog_data.append(entry)

            return oplog_data

        except Exception as e:
            print(f"❌ Error fetching oplog data: {e}")
            return []



if __name__ == "__main__":
    # Create a Hive system instance
    hive_system = HiveSystem()

    hive_system.connect()
    recreate = False
    key = ["student_id", "course_id"]
    set_attr = ["grade", "roll_no"]

    # Load data from CSV
    hive_system.load_data_from_csv("/home/sohith/Desktop/nosql/project/UniLog/dataset/student_course_grades.csv",recreate)
    
    # Set the table name and extract schema (this will populate self.table_name and self.all_columns)
    hive_system.set_table("student_course_grades")  # <-- Add this
    hive_system.create_oplog_table(recreate)

    
    hive_system.build_timestamp_cache(key)

    # Process commands from a test case file
    test_file = "/home/sohith/Desktop/nosql/project/UniLog/hive/testcase.in"
     
    
    try:
        with open(test_file, 'r') as f:
            commands = f.readlines()
            
        for command in commands:
            command = command.strip()
            if command:
                print(f"Processing command: {command}")
                hive_system.process_command(command, set_attr)   
    except Exception as e:
        print(f"Error processing test case file: {e}")
    finally:
        hive_system.disconnect()
    
