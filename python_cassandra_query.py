from cassandra.cluster import Cluster
import time

# timing the program
start_time = time.perf_counter()

def query_cassandra():
    # connect to Cassandra
    cluster = Cluster(["127.0.0.1"])  # Adjust if needed
    session = cluster.connect("python_test")  # Your keyspace

    # set table name
    table = "test_table"
    
    #find all names in "test_table"
    #cql_query = session.execute(f"SELECT Firstname from {table};")

    # find all ages that are 22
    #cql_query = session.execute(f"SELECT * From {table} WHERE Age = 22 ALLOW FILTERING;")

    #find all first and last names
    #cql_query = session.execute(f"SELECT Firstname, Lastname FROM {table};")
    
    #find last names and cities where age is 28
    cql_query = session.execute(f"SELECT City, Lastname FROM {table} WHERE Age = 28 ALLOW FILTERING;")

    # limit to 1 million 
    limited_results = cql_query[:1000000]

    # loop through
    for row in limited_results:
        row
        print(row)

    # close the connection
    cluster.shutdown()

if __name__ == "__main__":
    query_cassandra()

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.4f} seconds")
