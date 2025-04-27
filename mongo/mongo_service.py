from pymongo.errors import DuplicateKeyError
import pymongo
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import pandas as pd
import time


class MongoService:
    def __init__(self, db_name="project", oplog_name="oplog", table=None, recreate=False):
        load_dotenv()
        mongo_uri = os.environ.get("MONGO_URI")
        if not mongo_uri:
            raise EnvironmentError("MONGO_URI environment variable not set.")
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.oplog_name = oplog_name
        
        try:
            # Check if the collection already exists
            if recreate:
                if self.oplog_name in self.db.list_collection_names():
                    print(f"Collection '{self.oplog_name}' already exists. Deleting existing...")
                    self.drop_collection(table_name=self.oplog_name)


                if table in self.db.list_collection_names():
                    print(f"Collection '{table}' already exists. Deleting existing...")
                    self.drop_collection(table_name=table)
                    self.load_data(table_name=table)
            
            # Create the capped collection
            # if recreate is True:

                self.db.create_collection(
                    self.oplog_name,
                    capped=True,
                    size=1048576,  # Maximum size of the collection in bytes (e.g., 1MB)
                )
                # self.db[self.oplog_name].create_index({ "timestamp": 1, "_source": 1 }, unique=True)
                print(f"Capped collection '{self.oplog_name}' created successfully.")

        except Exception as e:
            print(f"An error occurred: {e}")

    def load_data(self, csv_file_path="./dataset/student_course_grades_head.csv", table_name="grades"):
        """
        Loads data from a CSV file into the specified MongoDB collection.
        Assumes the first row of the CSV contains headers.

        Return Value: Length of inserted entries
        """
        try:
            data = pd.read_csv(csv_file_path)
            collection = self.db[table_name]
            data_dict = data.to_dict('records')
            if data_dict:
                result = collection.insert_many(data_dict)
                return len(result.inserted_ids)
            else:
                return 0
        except FileNotFoundError:
            print(f"Error: CSV file not found at {csv_file_path}")
            return None
        except Exception as e:
            print(f"Error loading data into MongoDB: {e}")
            return None


    def _log_operation(self, log_entry_or_entries):
        log_collection = self.db[self.oplog_name]
        try:
            if isinstance(log_entry_or_entries, list):
                if log_entry_or_entries:
                    for entry in log_entry_or_entries:
                        try:
                            log_collection.insert_one(entry)
                        except DuplicateKeyError:
                            print(f"Skipped duplicate log entry with timestamp: {entry.get('timestamp')}")
            else:
                try:
                    log_collection.insert_one(log_entry_or_entries)
                    print("Logged one operation.")
                except DuplicateKeyError:
                    print(f"Skipped duplicate log entry with timestamp: {log_entry_or_entries.get('timestamp')}")
        except Exception as e:
            print(f"Error logging operation(s) to '{self.oplog_name}': {e}")


        
        
    
    def _get_timestamp(self):
        return time.time()


    def set_item(self, keys, item, table="grades", timestamp=None, log=True):
        """
        Sets or updates an item in the specified MongoDB collection based on the key.
        'keys' is a dictionary containing the set of keys used for search
        'item' is a dictionary containing the data to set or update.
        'table' is the name of the MongoDB collection.

        By default it adds an item if it cant find it.

        keys: {
            key1: val1,
            key2: val2,
            ...
        }

        item: {
            key: value,...
        }
        """
        collection = self.db[table]
        try:
            print("timeseat:",timestamp)
            log_entry = {"timestamp": timestamp if timestamp else self._get_timestamp(), "operation": "SET", "table": table, "keys": keys, "item": item, "_source": "local"}
            if log:
                self._log_operation(log_entry)
            result = collection.update_one(keys, {"$set": item}, upsert=True)
            return result.upserted_id if result.upserted_id else result.modified_count
        except Exception as e:
            print(f"Error setting item in '{table}': {e}")
            return None

    def get_item(self, keys, timestamp=None, table="grades", projection=None, log=True):
        """
        Retrieves a single item from the specified MongoDB collection based on the key.
        'keys' is a dictionary containing the set of keys used for search
        'table' is the name of the MongoDB collection.
        'key_field' is the field to use for the query (defaults to '_id').
        'projection' is an optional dictionary specifying which fields to return.
        """
        collection = self.db[table]
        try:
            log_entry = {"timestamp": timestamp if timestamp else self._get_timestamp(), "operation": "GET", "table": table, "keys": keys, "projection": projection, "_source": "local"}
            if log:
                self._log_operation(log_entry)
            return collection.find_one(keys, projection)
        except Exception as e:
            print(f"Error getting item from '{table}': {e}")
            return None
    
    def get_oplog(self, limit=None, query=None):
        """
        Retrieves entries from the MongoDB oplog (operation log).
        Requires connecting to a member of a replica set.

        Args:
            limit (int): The maximum number of oplog entries to retrieve (default: 10).
            query (dict, optional): A query to filter oplog entries. Defaults to None.

        Returns:
            pymongo.cursor.Cursor or None: A cursor iterating over the oplog entries,
                                          or None if an error occurred or not connected
                                          to a replica set member.
        """
        try:
            oplog = self.db[self.oplog_name]
            oplog_query = query if query is not None else {}
            if limit is not None:
                return list(oplog.find(oplog_query).sort('timestamp', pymongo.ASCENDING).limit(limit))
            else:
                return list(oplog.find(oplog_query).sort('timestamp', pymongo.ASCENDING))
        except Exception as e:
            print(f"Error accessing oplog: {e}")
            print("Ensure you are connected to a member of a MongoDB replica set.")
            return None



    def merge(self,system_name, other_oplog: list):
        """
        Merge the custom MongoDB oplog with the operation log from another system (assumed to only contain SET).
        Executes each SET instruction from both logs starting from the timestamp of the
        first instruction in the other oplog.

        Args:
            other_oplog (list): A list of dictionaries representing the operation log from
                                the other system with "timestamp", "operation" ("SET"),
                                "table", "keys", and "item".
                                Sorted in order of timestamps???
        """
        def filter_latest_oplog_entries(oplog_entries):
            latest_entries = {}

            for entry in oplog_entries:
                # Create a tuple (keys, table) as the unique identifier
                key = (tuple(sorted(entry['keys'].items())), entry['table'])

                # If not seen before, or if current timestamp is newer, update
                if key not in latest_entries or entry['timestamp'] > latest_entries[key]['timestamp']:
                    latest_entries[key] = entry

            # Return just the latest oplog entries as a list
            return list(latest_entries.values())


        # Find the timestamp of the first instruction in the other oplog
        other_oplog = list(filter(lambda x: x.get('operation') == 'SET', other_oplog))
        for oplog in other_oplog:
            oplog["_source"] = system_name

        if len(other_oplog) == 0:
            print("No operations found in the other oplog. Exiting.")
            return True
    
        start_timestamp = sorted(other_oplog, key=lambda x: x.get('timestamp'))[0].get("timestamp")
        oplog = []

        oplog = self.get_oplog(query={"operation": "SET", 'timestamp': {'$gte': start_timestamp}})

        if oplog is None:
            print(f"Could not retrieve MongoDB custom oplog '{self.oplog_name}' for merging.")
            return False
        
        oplog.extend(other_oplog)

        oplog = filter_latest_oplog_entries(oplog)
        print("Filtered: ", oplog)

        # Execute SET operations starting from the timestamp of the first other oplog instruction
        for operation in oplog:
            if operation.get('_source') != 'local':
                print(f"Executing: {operation}")
                self.set_item(operation['keys'], operation['item'], operation['table'], operation['timestamp'],)

        print(f"Merge operation completed with {system_name} system.")
        return True


    def drop_collection(self, table_name="grades"):
        """
        Drops the specified MongoDB collection. USE WITH CAUTION!
        """
        try:
            if self.db.name and table_name:
                self.client[self.db.name].drop_collection(table_name)
                print(f"Collection '{table_name}' dropped from database '{self.db.name}'.")
                return True
            else:
                print("Database or table name not configured for drop operation.")
                return False
        except Exception as e:
            print(f"Error dropping collection '{table_name}': {e}")
            return False

    def close(self):
        """
        Closes the MongoDB connection.
        """
        if self.client:
            self.client.close()
            print("MongoService connection closed.")

    def __del__(self):
        """
        Ensures the connection is closed when the object is garbage collected.
        """
        self.close()

# Example Usage (in a separate script):
if __name__ == "__main__":
    mongo_service = MongoService(table="student_course_grades",recreate=True)  # You can change the default db name

    # # Load data from a CSV
    # loaded_count = mongo_service.load_data(table_name="student_course_grades")
    # if loaded_count is not None:
    #     print(f"Loaded {loaded_count} records into 'student_course_grades'.")

    # print(mongo_service.db[mongo_service.oplog_name].index_information())

    # oplog_entries = mongo_service.get_oplog(limit=10)
    # if oplog_entries:
    #     print("\nLatest Oplog Entries:")
    #     for entry in oplog_entries:
    #         print(entry)

    keys = {
        "student_id": "SID101",
        "course_id": "CSE026"
    }

    '''
    SET (( SID101 , CSE026 ) , B )
    MONGO.GET ( SID403 , CSE013 )
    SET (( SID101 , CSE026 ) , A )
    '''

    # Set a single item
    set_result = mongo_service.set_item(keys, {"grade": "B"}, table="student_course_grades",timestamp=1)
    print(f"Set result for student SID1033: {set_result}")

    keys['student_id'] = "SID403"
    keys['course_id'] = "CSE013"

    # Get a single item
    grade_1033 = mongo_service.get_item(keys, table="student_course_grades",timestamp=2)
    print(f"Grade for student: {grade_1033}")

    keys['student_id'] = "SID101"
    keys['course_id'] = "CSE026"

    # Set a single item
    set_result = mongo_service.set_item(keys, {"grade": "B"}, table="student_course_grades",timestamp=3)
    print(f"Set result for student SID1033: {set_result}")

    # Merge operations
    merge_operations = [
        {
            "timestamp": 1,
            "operation": "SET",
            "table": "student_course_grades",
            "keys": {
                "student_id": "SID103",
                "course_id": "CSE016"
            },
            "item": {
                "grade": "A"
            }
        },
        {
            "timestamp": 2,
            "operation": "GET",
            "table": "student_course_grades",
            "keys": {
                "student_id": "SID103",
                "course_id": "CSE016"
            },
            "item": {}
        },
        {
            "timestamp": 3,
            "operation": "SET",
            "table": "student_course_grades",
            "keys": {
                "student_id": "SID103",
                "course_id": "CSE016"
            },
            "item": {
                "roll_no": "None",
                "email_id": "None",
                "grade": "B"
            }
            }
    ]

    merge_results = mongo_service.merge("Stupidity", merge_operations)
    print(f"Merge status: {merge_results}")

    mongo_service.close()