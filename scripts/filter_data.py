from bigdata2.tools.filter import DataFilter
from bigdata2.tools.data_processor import MongoDataProcessor

if __name__ == "__main__":
    filter = DataFilter()
    data_processor = MongoDataProcessor()

    # Step 1: Insert Data into MongoDB
    # This will insert the data into MongoDB in parallel using multiple workers
    filter.run_parallel_processing()

    # Step 2: Delete Data from MongoDB
    # Uncomment the line below to delete all records after insertion
    #data_processor.delete_all_documents(collection_name='filtered_data')


