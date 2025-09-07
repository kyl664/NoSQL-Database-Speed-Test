from pymongo import MongoClient
import time
import random
import string

#sudo systemctl start mongod
#sudo systemctl stop mongod
#sudo systemctl status mongod


# timing the program
start_time = time.perf_counter()

# function that will randomly make a string
def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

def create_mongo_database():
    
    # connect to MongoDB server 
    client = MongoClient("mongodb://127.0.0.1:27017/")

    db_name = "python_test"
    
    # deletes a database if it already exist. if not it creates it
    existing_database = client.list_database_names()
    if db_name in existing_database:
        client.drop_database(db_name)
        
    # create or access a database
    db = client[db_name]
    
    # create or access a collection
    collection = db["test_collection"]
    
    # sample data to insert
    '''test_data = []
    for i in range(1000):
        test_data.append ({"Firstname": f"first name {i}","Lastname": f"last name {i}", "City": f"city {i}", "Age": 20 + i % 10})'''

    # sample data to insert. inserts randomly mad string for fname, lname, and city
    test_data = []
    for i in range(1000000):
        test_data.append({"Firstname": random_string(7),
                          "Lastname": random_string(7),
                          "City": random_string(12),
                          "Age": 20 + i % 10})
    
    # insert data into collection
    collection.insert_many(test_data)
    
    print("Database created successfully with sample data!")

if __name__ == "__main__":
    create_mongo_database()

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.4f} seconds")
