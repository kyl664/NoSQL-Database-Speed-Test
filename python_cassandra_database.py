from cassandra.cluster import Cluster
import time
import random
import string

start_time = time.perf_counter()

def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

def create_cassandra_database():
    # connect to Cassandra server 
    cluster = Cluster(['127.0.0.1'])  
    session = cluster.connect()

    keyspace_name = "python_test"

    # check if the keyspace exists, and create if not
    try:
        session.set_keyspace(keyspace_name)
    except:
        # create the keyspace if it doesn't exist
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name} 
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        session.set_keyspace(keyspace_name)
    
    # create table (if it doesn't exist)
    session.execute("""
        CREATE TABLE IF NOT EXISTS test_table (
            id UUID PRIMARY KEY,
            Firstname TEXT,
            Lastname TEXT,
            City TEXT,
            Age INT
        )
    """)

    # sample data to insert
    test_data = []
    for i in range(1000000):
        test_data.append((
            random_string(7),    
            random_string(7),    
            random_string(12),   
            20 + i % 10          
        ))

    # insert data into table
    insert_stmt = session.prepare("""
        INSERT INTO test_table (id, Firstname, Lastname, City, Age)
        VALUES (uuid(), ?, ?, ?, ?)
    """)
    
    # execute the insert for each piece of data
    for data in test_data:
        session.execute(insert_stmt, data)
    
    print("Database created successfully with sample data!")

if __name__ == "__main__":
    create_cassandra_database()

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.4f} seconds")
