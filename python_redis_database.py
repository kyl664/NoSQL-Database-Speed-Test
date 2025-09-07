import redis
import time
import random
import string

#sudo systemctl enable redis-server
#sudo systemctl start redis-server
#redis-cli

# timing the program 
start_time = time.perf_counter()

# function that will randomly make a string
def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

def create_redis_database():
    # connect to Redis server
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    # clear the database 
    client.flushdb()
    
    # sample data insertion
    pipeline = client.pipeline()
    for i in range(1000000):
        key = f"user:{i}"
        pipeline.hset(key, mapping={
            "Firstname": random_string(7),
            "Lastname": random_string(7),
            "City": random_string(12),
            "Age": 20 + i % 10
        })
    pipeline.execute()
    
    print("Database created successfully with sample data!")

if __name__ == "__main__":
    create_redis_database()
    
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.4f} seconds")
