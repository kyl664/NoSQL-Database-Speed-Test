import redis
import time

# timing the program
start_time = time.perf_counter()

def query_redis_1():
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    # find all users with a "Firstname" 
    user_keys = client.keys('user:*')  # get all user keys
    results_1 = []
    for key in user_keys:
        if client.hexists(key, "Firstname"):  # check if the "Firstname" field exists
            results_1.append(client.hgetall(key))  # add the user data if field exists
    print(results_1)
            
    client.close()

def query_redis_2():
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # find all users with a in their firstname
    user_keys = client.keys('user:*')  
    results_2 = []
    for key in user_keys:
        firstname = client.hget(key, "Firstname")
        if firstname and 'a' in firstname.lower():  # check for 'a' in the Firstname field (case-insensitive)
            results_2.append(client.hgetall(key))
    print(results_2)
    
    client.close()

def query_redis_3():
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # find all users whos age is 22
    user_keys = client.keys('user:*')  
    results_3 = []
    for key in user_keys:
        age = client.hget(key, "Age")
        if age and int(age) == 22:
            results_3.append(client.hgetall(key))
    #print(results_3)
    
    client.close()

def query_redis_4():
    client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    user_keys = client.keys('user:*')  
    results_4 = []
    for key in user_keys:
        firstname = client.hget(key, "Firstname")
        lastname = client.hget(key, "Lastname")
        age = client.hget(key, "Age")
        if age and int(age) == 22 and 'a' in (firstname or '').lower() and 'a' in (lastname or '').lower():
            results_4.append(client.hgetall(key))
    print(results_4)
    
    client.close()


if __name__ == "__main__":
    #query_redis_1()
    #query_redis_2()
    #query_redis_3()
    query_redis_4()
    

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.4f} seconds")
