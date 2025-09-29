from codecs import encode, decode
import http.client
import aiohttp
import asyncio
import json
import random
import time

random.seed(42)



# function async
def send_post_request_config(host='load_balancer', port=5000, path='/init'):
    print("/init")
    payload = {
        "N":6,
        "schema":{
            "columns":["Stud_id","Stud_name","Stud_marks"],
            "dtypes":["Number","String","String"]
        },
        "shards":[
            {"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096},
            {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096},
            {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size":4096},
            {"Stud_id_low":12288, "Shard_id": "sh4", "Shard_size":4096}
        ],
        "servers":{
            "Server0":["sh1","sh2"],
            "Server1":["sh3","sh4"],
            "Server3":["sh1","sh3"],
            "Server4":["sh4","sh2"],
            "Server5":["sh1","sh4"],
            "Server6":["sh3","sh2"]
        }
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('POST', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()




# async read function
async def send_post_request_read_async(low, high, host='load_balancer', port=5000, path='/read'):
    payload = {
        "low":low,
        "high":high
    }
    headers = {'Content-type': 'application/json'}
    # json_payload = json.dumps(payload)
    
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{host}:{port}{path}', json=payload, headers=headers) as response:
            response_text = await response.text()
            # print(response.status)
            return response_text


async def send_read_requests(num_requests=10000):
    tasks = []
    for _ in range(num_requests):
        low = random.randint(0, 16000)
        high = min(low+100, 16000)
        # high = random.randint(low, 16000)
        response_text=send_post_request_read_async(low, high)
        tasks.append(response_text)
    await asyncio.gather(*tasks)



async def send_post_request_write_async(sem, session, index=0, host='load_balancer', port=5000, path='/write'):
    payload = {
        "data": [
            {"Stud_id": str(index), "Stud_name": "GHI"+str(index), "Stud_marks": "27"}
        ]
    }
    headers = {'Content-type': 'application/json'}
    async with sem:
        async with session.post(f'http://{host}:{port}{path}', json=payload, headers=headers) as response:
            response_text = await response.text()
            # print(response_text)
            return response_text

async def send_write_requests(num_requests=10000):
    sem = asyncio.Semaphore(1000)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _ in range(num_requests):
            r_int = random.randint(0, 16000)
            task = send_post_request_write_async(sem, session, r_int)
            tasks.append(task)
        await asyncio.gather(*tasks)




async def main():

    num_requests = 500
    num_iterations = 20
    start_time = time.time()
    for i in range(0,num_iterations):
        # print(i)
        await send_write_requests(num_requests)
        write_time = time.time() - start_time
        print(f"{num_requests} Write requests took {write_time} seconds")
        time.sleep(1)
    
    write_time = time.time() - start_time

    for i in range(0,num_iterations):
        # print(i)
        # start_time = time.time()
        await send_read_requests(num_requests)
        read_time = time.time() - start_time-write_time
        print(f"{num_requests} Read requests took {read_time} seconds")
        time.sleep(1)
    
    read_time = time.time() - start_time
    print(f"Total time taken: {read_time} seconds")
    print(f"Write time: {write_time} seconds")
    print(f"Read time: {read_time - write_time} seconds")








if __name__ == '__main__':

    send_post_request_config()
    time.sleep(10)
    asyncio.run(main())



