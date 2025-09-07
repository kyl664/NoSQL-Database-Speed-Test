from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel
import time
import random
import string

def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

def create_cassandra_database():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    keyspace_name = "python_test"

    # Create keyspace if it doesn't exist
    try:
        session.set_keyspace(keyspace_name)
    except:
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name} 
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        session.set_keyspace(keyspace_name)

    # Create table
    session.execute("""
        CREATE TABLE IF NOT EXISTS test_table (
            id UUID PRIMARY KEY,
            Firstname TEXT,
            Lastname TEXT,
            City TEXT,
            Age INT
        )
    """)

    insert_stmt = session.prepare("""
        INSERT INTO test_table (id, Firstname, Lastname, City, Age)
        VALUES (uuid(), ?, ?, ?, ?)
    """)

    batch_size = 100
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    
    for i in range(1_000_000):
        data = (
            random_string(7),
            random_string(7),
            random_string(12),
            20 + i % 10
        )
        batch.add(insert_stmt, data)

        if (i + 1) % batch_size == 0:
            session.execute(batch)
            batch.clear()
            if (i + 1) % 10000 == 0:
                print(f"{i + 1} rows inserted...")

    # Insert any remaining rows
    if batch:
        session.execute(batch)

    print("Batch insert complete.")

if __name__ == "__main__":
    start_time = time.perf_counter()
    create_cassandra_database()
    end_time = time.perf_counter()
    print(f"Elapsed time: {end_time - start_time:.2f} seconds")
