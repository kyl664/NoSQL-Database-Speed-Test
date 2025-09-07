import redis
import time

client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
def query_redis():
    
    key = 'user:0'

    first = client.hget(key, 'Firstname')
    last = client.hget(key, 'Lastname')
    city = client.hget(key, 'City')
    age = client.hget(key, 'Age')
   

    #print(f"Firstname: {first}\n", f"Lastname: {last}\n", f"City: {city}\n", f"Age: {age}")

def query_redis2():
    for i in range(0, 10000):
        key = f"user:{i}"

        first = client.hget(key, 'Firstname')
        last = client.hget(key, 'Lastname')
        city = client.hget(key, 'City')
        age = client.hget(key, 'Age')
        print(f"Firstname: {first}\n", f"Lastname: {last}\n", f"City: {city}\n", f"Age: {age}")
        
    

if __name__ == "__main__":
    start_time = time.perf_counter()

    #query_redis()
    query_redis2()

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.4f} seconds")
