from pymongo import MongoClient
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import time
from bigdata2.tools.data_processor import MongoDataProcessor

class DataFilter:
    def __init__(self):
        self.required_columns = ["Navigational status", "MMSI", "Latitude", "Longitude", "ROT", "SOG", "COG", "Heading"]
        self.source_collection = 'original_data'
        self.target_collection = 'filtered_data'
        self.db_name = 'vessel_data'
        self.connection_string = "mongodb://localhost:27020"
        self.num_workers = 4
        self.data_processor = MongoDataProcessor()

    def query_data(self, skip, limit):
        client = MongoClient(self.connection_string)
        db = client[self.db_name]
        collection = db[self.source_collection]
        cursor = collection.find().skip(skip).limit(limit)
        data = pd.DataFrame(list(cursor))
        return data

    def filter_data(self, data):
        vessel_counts = data['MMSI'].value_counts()
        valid_vessels = vessel_counts[vessel_counts >= 100].index
        filtered_data = data[data['MMSI'].isin(valid_vessels)]

        filtered_data = filtered_data[filtered_data.apply(self.is_valid, axis=1)]

        filtered_data = filtered_data[(filtered_data['Latitude'].between(-90, 90)) & (filtered_data['Longitude'].between(-180, 180))]

        return filtered_data

    def is_valid(self, doc):
        return all(field in doc and doc[field] not in [None, "", "NaN", -1] for field in self.required_columns)

    def save_filtered_data(self, filtered_data):
        if filtered_data.empty:
            print("No valid data to insert.")
            return

        client = MongoClient(self.connection_string)
        db = client[self.db_name]
        collection = db[self.target_collection]

        records = filtered_data.to_dict(orient='records')
        collection.insert_many(records)
        print(f"Inserted {len(records)} filtered records.")

    def process_chunk(self, skip, limit):
        data = self.query_data(skip, limit)
        filtered_data = self.filter_data(data)
        if filtered_data.empty:
            print("No valid data to insert.")
            return
        result = self.data_processor.insert_chunk(self.target_collection, filtered_data)

        return result

    def run_parallel_processing(self):
        client = MongoClient(self.connection_string)
        total_docs = client[self.db_name][self.source_collection].count_documents({})
        chunk_size = total_docs // self.num_workers

        start_time = time.time()
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            for i in range(self.num_workers):
                skip = i * chunk_size

                limit = chunk_size if i < self.num_workers - 1 else total_docs - skip
                futures.append(executor.submit(self.process_chunk, skip, limit))

            for idx, future in enumerate(futures, 1):
                try:
                    result = future.result()
                    print(f"Chunk {idx}: {result}")
                except Exception as e:
                    print(f"Error in chunk {idx}: {e}")

        end_time = time.time()
        print(f"Processing completed in {end_time - start_time:.2f} seconds.")
