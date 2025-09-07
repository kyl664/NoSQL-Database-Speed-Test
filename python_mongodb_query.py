from pymongo import MongoClient
import time

# timing the program
start_time = time.perf_counter()

def query_mongodb():
    # connect to MongoDB
    client = MongoClient("mongodb://127.0.0.1:27017/")
    
    # select database
    db = client["python_test"]
    
    # select collection
    collection = db["test_collection"]

    ## queries: ##
    
    # find all names in "test_collection"
    #query = {"Firstname": {"$exists": True}}
    
    # find all names in "test_collection" that have 'a' in it
    #query = {"Firstname": {"$regex": "a", "$options": "i"}}
    
    # find all ages that are 22
    #query = {"Age": 22}
    
    # find all names that have 'a' and are 22. the "i" is an option that find "a" and "A" 
    #query = {"Age": 22, "Firstname": {"$regex": "a", "$options": "i"}}
    
    # find all people that are 27, live in a city that has "a" in the name, and have a first and last name that contains "a".
    #query = {"Age": 27, "Firstname": {"$regex": "a", "$options": "i"}, "Lastname": {"$regex": "a", "$options": "i"}, "City": {"$regex": "a", "$options": "i"}}

    # find all people that are 27, live in a city that has "a" and "f" in the name, and have a first and last name that contains "a" and "f".
    #query = {"Age": 27, "Firstname": {"$regex": "a.*f|f.*a", "$options": "i"}, "Lastname": {"$regex": "a.*f|f.*a", "$options": "i"}, "City": {"$regex": "a.*f|f.*a", "$options": "i"}}

    
    
    # execute query
    results = collection.find(query).limit(1000000)
    
    # print results. printing takes the majority of the time. 
    for document in results:
        document
        #print(document)
    
    # close connection
    client.close()

if __name__ == "__main__":
    query_mongodb()

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.4f} seconds")
