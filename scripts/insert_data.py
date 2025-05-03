from bigdata2.tools.data_inserter import MongoDataProcessor
import os

if __name__ == '__main__':
    project_path = os.path.dirname(os.path.abspath(__file__))
    parent_path = os.path.dirname(project_path)
    data_path = os.path.join(parent_path, 'aisdk-2024-05-01.csv')

    data_processor = MongoDataProcessor(data_path=data_path, chunk_size=50000)

    # Step 1: Insert Data into MongoDB
    # This will insert the data into MongoDB in parallel using multiple workers
    data_processor.process_data_in_parallel(limit=200000)

    # Step 2: Delete Data from MongoDB
    # Uncomment the line below to delete all records after insertion
    #data_processor.delete_all_documents()

