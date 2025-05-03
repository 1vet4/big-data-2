import pandas as pd
from pymongo import MongoClient
from concurrent.futures import ProcessPoolExecutor
import time


class MongoDataProcessor:
    def __init__(self, data_path, chunk_size=100, overlap_size=0, num_workers=4):
        self.data_path = data_path
        self.chunk_size = chunk_size
        self.overlap_size = overlap_size
        self.num_workers = num_workers


    def insert_chunk(self, chunk):
        try:
            client = MongoClient("mongodb://localhost:27020")  # mongos port
            db = client["vessel_data"]
            collection = db["original_data"]
            records = chunk.to_dict(orient='records')
            if records:
                collection.insert_many(records)
                return f"Inserted {len(records)} records"
        except Exception as e:
            return f"Error inserting chunk: {e}"

    def delete_specific_documents(self, filter_condition):
        """Delete documents from the collection based on the provided filter."""
        try:
            client = MongoClient("mongodb://localhost:27020")  # mongos port
            db = client["vessel_data"]
            collection = db["original_data"]
            delete_result = collection.delete_many(filter_condition)
            return f"Deleted {delete_result.deleted_count} documents"
        except Exception as e:
            return f"Error deleting documents: {e}"

    def delete_all_documents(self):
        """Delete all documents from the collection."""
        try:
            client = MongoClient("mongodb://localhost:27020")  # mongos port
            db = client["vessel_data"]
            collection = db["original_data"]
            collection.delete_many({})
            print("All records have been deleted.")
        except Exception as e:
            print(f"Error deleting all records: {e}")

    def process_data_in_parallel(self, limit):
        start_time = time.time()
        total_records_inserted = 0
        futures = []

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            # Read the CSV and insert in chunks in parallel
            with pd.read_csv(self.data_path, chunksize=self.chunk_size) as reader:
                for chunk in reader:
                    # Check if we've reached the limit
                    if total_records_inserted >= limit:
                        break

                    # Calculate how many records to process in this chunk
                    remaining_records = limit - total_records_inserted
                    chunk_to_process = chunk.head(remaining_records)  # Only process the remaining number of records

                    total_records_inserted += len(chunk_to_process)
                    futures.append(executor.submit(self.insert_chunk, chunk_to_process))

            # Wait for all futures to complete
            for idx, future in enumerate(futures, 1):
                try:
                    result = future.result()
                    print(f"Chunk {idx}: {result}")
                except Exception as e:
                    print(f"Error in chunk {idx}: {e}")

        end_time = time.time()
        print(f"Inserted {total_records_inserted} records in {end_time - start_time:.2f} seconds.")
