from mongo.mongo_service import MongoService
from hive.better_hive_service import HiveSystem

def main():
    """Main function to run the Hive system"""
    # Create a Hive system instance
    hive_system = HiveSystem()
    mongo_system = MongoService()

    key = ["student_id", "course_id"]
    
    try:
        hive_system.connect()
        hive_system.set_table("student_course_grades")
        hive_system.build_timestamp_cache(key)
        print("Connected to Hive and MongoDB systems.")

        elog = hive_system.oplog_manager.get_oplog()
        print("Oplog fetched successfully.")
 
        status = mongo_system.merge(elog)
        print("Merge status:", status)

            
    except Exception as e:
        print(f"System error: {e}")
    finally:
        hive_system.disconnect()


if __name__ == "__main__":
    main()

