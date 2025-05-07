import json
from pymongo import MongoClient # type: ignore

@staticmethod
def upload_json_to_mongodb(uri, database_name, collection_name, file_path):
    """
        Upload data from a JSON file to MongoDB Atlas.
        :param uri: MongoDB connection URI
        :param database_name: Name of the database
        :param collection_name: Name of the collection
        :param file_path: Path to the JSON file
    """
    try:
        # Connect to MongoDB Atlas
        client = MongoClient(uri)
        db = client[database_name]
        collection = db[collection_name]
        
        # Read JSON file
        with open(file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
        
        # Insert data into the collection
        if isinstance(data, list):
            collection.insert_many(data)
        else:
            collection.insert_one(data)
        
        print("Data uploaded successfully to MongoDB Atlas.")
    except Exception as e:
        print(f"Error uploading data to MongoDB: {e}")

@staticmethod
def get_data_from_mongodb(uri, database_name, collection_name, query={}):
    """
    Fetches data from a MongoDB collection and returns it as a list of dictionaries.

    :param uri: MongoDB connection URI (e.g., "mongodb://localhost:27017/")
    :param database_name: Name of the database to connect to.
    :param collection_name: Name of the collection to fetch data from.
    :param query: MongoDB query filter (default: {} to fetch all documents).
    :return: List of dictionaries containing the fetched data.
    """
    try:
        # Connect to MongoDB
        client = MongoClient(uri)
        db = client[database_name]
        collection = db[collection_name]

        # Fetch data from MongoDB
        data = list(collection.find(query))

        # Convert ObjectId to string for JSON compatibility
        for document in data:
            document["_id"] = str(document["_id"])

        return data

    except Exception as e:
        print(f"Error fetching data from MongoDB: {e}")
        return []
