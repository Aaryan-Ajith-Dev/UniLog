import pymongo
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import pandas as pd
import time


class MongoService:
    def __init__(self, db_name="project", oplog_name="oplog"):
        load_dotenv()
        mongo_uri = os.environ.get("MONGO_URI")
        if not mongo_uri:
            raise EnvironmentError("MONGO_URI environment variable not set.")
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.oplog_name = oplog_name
        try:
            # Check if the collection already exists
            if self.oplog_name in self.db.list_collection_names():
                print(f"Collection '{self.oplog_name}' already exists.")
            else:
                # Create the capped collection
                self.db.create_collection(
                    self.oplog_name,
                    capped=True,
                    size=1048576,  # Maximum size of the collection in bytes (e.g., 1MB)
                )
                print(f"Capped collection '{self.oplog_name}' created successfully.")

        except Exception as e:
            print(f"An error occurred: {e}")

    def load_data(self, csv_file_path="cleaned_grades.csv", table_name="grades"):
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
                    log_collection.insert_many(log_entry_or_entries)
                    print(f"Logged {len(log_entry_or_entries)} operations.")
            else:
                log_collection.insert_one(log_entry_or_entries)
                print("Logged one operation.")
        except Exception as e:
            print(f"Error logging operation(s) to '{self.oplog_name}': {e}")

    
    
    
    def _get_timestamp(self):
        return time.time()


    def set_item(self, keys, item, table="grades"):
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
            log_entry = {"timestamp": self._get_timestamp(), "operation": "GET", "table": table, "keys": keys, "item": item}
            self._log_operation(log_entry)
            result = collection.update_one(keys, {"$set": item}, upsert=True)
            return result.upserted_id if result.upserted_id else result.modified_count
        except Exception as e:
            print(f"Error setting item in '{table}': {e}")
            return None

    def get_item(self, keys, table="grades", projection=None):
        """
        Retrieves a single item from the specified MongoDB collection based on the key.
        'keys' is a dictionary containing the set of keys used for search
        'table' is the name of the MongoDB collection.
        'key_field' is the field to use for the query (defaults to '_id').
        'projection' is an optional dictionary specifying which fields to return.
        """
        collection = self.db[table]
        try:
            log_entry = {"timestamp": self._get_timestamp(), "operation": "GET", "table": table, "keys": keys, "projection": projection}
            self._log_operation(log_entry)
            return collection.find_one(keys, projection)
        except Exception as e:
            print(f"Error getting item from '{table}': {e}")
            return None
    
    def get_oplog(self, limit=10, query=None):
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
            return list(oplog.find(oplog_query).sort('timestamp', pymongo.ASCENDING).limit(limit))
        except Exception as e:
            print(f"Error accessing oplog: {e}")
            print("Ensure you are connected to a member of a MongoDB replica set.")
            return None



    def merge(self, other_oplog):
        """
        Merge the custom MongoDB oplog with the operation log from another system (assumed to only contain SET).
        Executes each SET instruction from both logs starting from the timestamp of the
        first instruction in the other oplog.

        Args:
            other_oplog (list): A list of dictionaries representing the operation log from
                                the other system with "timestamp", "operation" ("SET"),
                                "table", "keys", and "item".
        """

        self._log_operation(other_oplog)
            
        oplog = self.get_oplog(query={"operation": "SET"})

        if oplog is None:
            print(f"Could not retrieve MongoDB custom oplog '{self.oplog_name}' for merging.")
            return False


        # Find the timestamp of the first instruction in the other oplog
        first_other_timestamp = float('inf')
        for operation in other_oplog:
            if operation.get('operation') == 'SET':
                first_other_timestamp = operation.get('timestamp', float('inf'))
                if first_other_timestamp != float('inf'):
                    break

        if first_other_timestamp == float('inf'):
            print("No operations found in the other oplog. Exiting.")
            return True

        print(f"Starting SET execution from timestamp: {first_other_timestamp}")

        # Execute SET operations starting from the timestamp of the first other oplog instruction
        for operation in oplog:
            if operation.get('operation') == 'SET' and operation.get('timestamp', 0) >= first_other_timestamp:
                print(f"Executing: {operation}")
                self.set_item(operation['keys'], operation['item'], operation['table'])

        print("Merge operation completed.")
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
    mongo_service = MongoService()  # You can change the default db name
    # mongo_service.drop_collection()

    # # Load data from a CSV
    # loaded_count = mongo_service.load_data("cleaned_grades.csv", table_name="grades")
    # if loaded_count is not None:
    #     print(f"Loaded {loaded_count} records into 'grades'.")

    # oplog_entries = mongo_service.get_oplog(limit=5)
    # if oplog_entries:
    #     print("\nLatest Oplog Entries:")
    #     for entry in oplog_entries:
    #         print(entry)

    # keys = {
    #     "Student ID": "8bc6bb96e00acb88ee954fc1702afb48a2d1ea05309d90f3dfedb3a155a6d06e",
    #     "Subject Code / Name": "IT 989/20 / Thesis / Research hours"
    # }
    # # Set a single item
    # set_result = mongo_service.set_item(keys, {"Obtained Marks/Grade": "B+"}, table="grades")
    # print(f"Set result for student 105: {set_result}")

    # # Get a single item
    # grade_101 = mongo_service.get_item(keys, table="grades")
    # print(f"Grade for student: {grade_101}")

    # # Merge operations
    # merge_operations = [
    #     {"timestamp": mongo_service._get_timestamp(), "operation": "SET", "table": "grades", "keys": keys, "item": {"Obtained Marks/Grade": "C"}}
    # ]
    # merge_results = mongo_service.merge(merge_operations)
    # print(f"Merge status: {merge_results}")

    mongo_service.close()