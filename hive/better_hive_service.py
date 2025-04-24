import csv
import os
import time
from pyhive import hive
import pandas as pd
from thrift.transport import TTransport
from datetime import datetime


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
        self.operation_counter = 0
        
        # In-memory cache of timestamps for each (student_id, course_id) pair
        # This keeps track of when each record was last updated
        self.timestamp_cache = {}
        
        # Initialize or load the operation log
        self._init_oplog()
        
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
    
    def _init_oplog(self):
        """Initialize or load the operation log file"""
        if not os.path.exists(self.oplog_file):
            with open(self.oplog_file, 'w') as f:
                pass  # Create an empty file
        else:
            # Count existing operations to set the counter correctly
            with open(self.oplog_file, 'r') as f:
                lines = f.readlines()
                if lines:
                    try:
                        last_counter = int(lines[-1].split(',')[0].strip())
                        self.operation_counter = last_counter
                    except (IndexError, ValueError):
                        self.operation_counter = 0
            
            # Also rebuild the timestamp cache from the oplog
            self._rebuild_timestamp_cache()
    
    def _rebuild_timestamp_cache(self):
        """
        Rebuild the timestamp cache from the operation log
        This is used when initializing the system to restore state
        """
        try:
            with open(self.oplog_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split(',', 1)
                    if len(parts) < 2:
                        continue
                    
                    timestamp = int(parts[0].strip())
                    operation = parts[1].strip()
                    
                    # Only process SET operations for timestamp cache
                    if operation.startswith("SET"):
                        parsed_op = self.parse_operation(operation)
                        if parsed_op and parsed_op[0] == "SET":
                            _, student_id, course_id, _ = parsed_op
                            key = (student_id, course_id)
                            self.timestamp_cache[key] = timestamp
        except Exception as e:
            print(f"Error rebuilding timestamp cache: {e}")
    
    def _log_operation(self, operation):
        """
        Append an operation to the log file
        
        Args:
            operation (str): The operation to log
        
        Returns:
            int: The operation counter (timestamp)
        """
        self.operation_counter += 1
        with open(self.oplog_file, 'a') as f:
            f.write(f"{self.operation_counter} , {operation}\n")
        return self.operation_counter
    
    def load_data_from_csv(self, csv_file):
        """
        Load data from CSV file into Hive table
        
        Args:
            csv_file (str): Path to the CSV file
        """
        try:
            self.connect()
            
            # Read CSV file to understand its structure
            df = pd.read_csv(csv_file)
            
            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS student_course_grades (
                student_id STRING,
                course_id STRING,
                grade STRING,
                PRIMARY KEY (student_id, course_id)
            )
            STORED AS TEXTFILE
            """
            
            self.cursor.execute("DROP TABLE IF EXISTS student_course_grades")
            self.cursor.execute(create_table_query)
            
            # Load data using Hive's LOAD DATA command
            # For simplicity, we'll use pandas to read and then insert data
            for _, row in df.iterrows():
                # Assuming the CSV has columns for student_id, course_id, grade
                student_id = str(row[0])  # Convert to string to ensure consistency
                course_id = str(row[1])
                grade = str(row[2])
                
                insert_query = f"""
                INSERT INTO student_course_grades 
                VALUES ('{student_id}', '{course_id}', '{grade}')
                """
                self.cursor.execute(insert_query)
                
                # Also update the timestamp cache for this entry
                # Initial load doesn't generate log entries but we set a timestamp of 0
                key = (student_id, course_id)
                self.timestamp_cache[key] = 0
            
            print(f"Successfully loaded data from {csv_file} into Hive table 'student_course_grades'")
        except Exception as e:
            print(f"Error loading data: {e}")
        finally:
            self.disconnect()
    
    def get(self, student_id, course_id):
        """
        Get the grade for a specific student_id and course_id
        
        Args:
            student_id (str): Student ID
            course_id (str): Course ID
            
        Returns:
            str: Grade value if found, None otherwise
        """
        try:
            self.connect()
            query = f"""
            SELECT grade FROM student_course_grades 
            WHERE student_id = '{student_id}' AND course_id = '{course_id}'
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            
            # Log the operation
            operation = f"GET ({student_id}, {course_id})"
            self._log_operation(operation)
            
            if result:
                grade = result[0]
                print(f"Grade for student {student_id} in course {course_id}: {grade}")
                return grade
            else:
                print(f"No grade found for student {student_id} in course {course_id}")
                return None
                
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return None
        finally:
            self.disconnect()
    
    def set(self, student_id, course_id, grade, update_timestamp=True):
        """
        Set or update the grade for a specific student_id and course_id
        
        Args:
            student_id (str): Student ID
            course_id (str): Course ID
            grade (str): New grade value
            update_timestamp (bool): Whether to update the timestamp cache
                                    (set to False when applying operations from other systems)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.connect()
            
            # Check if the record exists
            query = f"""
            SELECT COUNT(*) FROM student_course_grades 
            WHERE student_id = '{student_id}' AND course_id = '{course_id}'
            """
            self.cursor.execute(query)
            count = self.cursor.fetchone()[0]
            
            if count > 0:
                # Update existing record
                update_query = f"""
                UPDATE student_course_grades 
                SET grade = '{grade}' 
                WHERE student_id = '{student_id}' AND course_id = '{course_id}'
                """
                self.cursor.execute(update_query)
            else:
                # Insert new record
                insert_query = f"""
                INSERT INTO student_course_grades 
                VALUES ('{student_id}', '{course_id}', '{grade}')
                """
                self.cursor.execute(insert_query)
            
            # Log the operation only for operations initiated in this system
            # (not for operations being applied from other systems during merge)
            if update_timestamp:
                operation = f"SET (({student_id}, {course_id}), {grade})"
                timestamp = self._log_operation(operation)
                
                # Update timestamp cache
                key = (student_id, course_id)
                self.timestamp_cache[key] = timestamp
                
            print(f"Successfully set grade '{grade}' for student {student_id} in course {course_id}")
            return True
        
        except Exception as e:
            print(f"Error setting data: {e}")
            return False
        finally:
            self.disconnect()
    
    def read_oplog(self, system_name):
        """
        Read the operation log of another system
        
        Args:
            system_name (str): Name of the system (e.g., SQL, MONGO)
            
        Returns:
            list: List of operations from the specified system's oplog
        """
        oplog_file = f"oplog.{system_name.lower()}"
        operations = []
        
        try:
            with open(oplog_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        parts = line.split(',', 1)
                        if len(parts) == 2:
                            timestamp = int(parts[0].strip())
                            operation = parts[1].strip()
                            operations.append((timestamp, operation))
            return operations
        except Exception as e:
            print(f"Error reading oplog for {system_name}: {e}")
            return []
    
    def parse_operation(self, operation):
        """
        Parse an operation string into its components
        
        Args:
            operation (str): Operation string (e.g., "SET ((SID103, CSE016), A)")
            
        Returns:
            tuple: (operation_type, student_id, course_id, grade)
        """
        operation = operation.strip()
        
        if operation.startswith("SET"):
            # Extract parameters from SET operation
            params = operation[4:-1]  # Remove "SET(" and ")"
            
            # Extract student_id and course_id from a string like "((SID103, CSE016), A)"
            parts = params.split(',')
            
            # Handle the first part which is like "((SID103"
            student_id = parts[0].replace('(', '').strip()
            
            # Handle the second part which is like " CSE016)"
            course_id = parts[1].replace(')', '').strip()
            
            # The last part is the grade
            grade = parts[-1].strip()
            
            return ("SET", student_id, course_id, grade)
            
        elif operation.startswith("GET"):
            # Extract parameters from GET operation
            params = operation[4:-1]  # Remove "GET(" and ")"
            parts = params.split(',')
            
            student_id = parts[0].strip()
            course_id = parts[1].strip() if len(parts) > 1 else ""
            
            return ("GET", student_id, course_id, None)
        
        return None
    
    def merge(self, system_name):
        """
        Merge the state with another system based on its operation log,
        taking into account timestamps to ensure most recent updates are applied.
        
        Args:
            system_name (str): Name of the system to merge with (e.g., SQL, MONGO)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print(f"Starting merge with {system_name}...")
            operations = self.read_oplog(system_name)
            
            # Process only SET operations and consider timestamps
            for timestamp, operation in operations:
                parsed_op = self.parse_operation(operation)
                if parsed_op is None or parsed_op[0] != "SET":
                    continue
                
                _, student_id, course_id, grade = parsed_op
                key = (student_id, course_id)
                
                # Check if we should apply this operation based on timestamp
                should_apply = False
                
                # If we don't have this record or our version is older, apply the update
                if key not in self.timestamp_cache or self.timestamp_cache[key] < timestamp:
                    should_apply = True
                    print(f"Applying {system_name} operation: {operation} (timestamp: {timestamp})")
                    
                    # Apply the SET operation but don't update our oplog
                    # We're syncing data but not recording this as our own operation
                    self.set(student_id, course_id, grade, update_timestamp=False)
                    
                    # Update our timestamp cache with the other system's timestamp
                    self.timestamp_cache[key] = timestamp
                else:
                    print(f"Skipping {system_name} operation: {operation} (our timestamp: {self.timestamp_cache[key]} > {timestamp})")
            
            print(f"Successfully merged with {system_name}")
            return True
            
        except Exception as e:
            print(f"Error merging with {system_name}: {e}")
            return False


# Example usage
if __name__ == "__main__":
    # Create a Hive system instance
    hive_system = HiveSystem()
    
    # Load data from CSV
    hive_system.load_data_from_csv("student_course_grades.csv")
    
    # Example operations
    hive_system.get("SID103", "CSE016")
    hive_system.set("SID103", "CSE016", "A")
    
    # Merge with another system
    hive_system.merge("SQL")