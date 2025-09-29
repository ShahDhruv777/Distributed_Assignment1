from codecs import encode, decode
import http.client
import json
import random
import time

def send_post_request_config(host='load_balancer', port=5000, path='/init'):
    print("/init")
    payload = {
        "N":6,
        "schema":{"columns":["Stud_id","Stud_name","Stud_marks"],
        "dtypes":["Number","String","String"]},
        "shards":[{"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096},
        {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096},
        {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size":4096},
        {"Stud_id_low":12288, "Shard_id": "sh4", "Shard_size":4096}],
        "servers":{"Server0":["sh1","sh2"],
        "Server1":["sh3","sh4"],
        "Server3":["sh1","sh3"],
        "Server4":["sh4","sh2"],
        "Server5":["sh1","sh4"],
        "Server6":["sh3","sh2"]}
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


def send_post_request_read(low, high, host='load_balancer', port=5000, path='/read'):
    # print("/read")
    payload = {
        "low":low,
        "high":high
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('POST', path, json_payload, headers)

    response = connection.getresponse()
    response_data = response.read().decode('utf-8')
    connection.close()

def send_post_request_write(index=0,host='load_balancer', port=5000, path='/write'):
    # print("/write")
    payload = {
        "data": [
            {"Stud_id": str(index), "Stud_name": "GHI"+str(index), "Stud_marks": "27"}
            # Add more entries as needed
        ]
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('POST', path, json_payload, headers)

    response = connection.getresponse()
    # print(f'Status: {response.status}')
    # print('Response:')
    response_data = response.read().decode('utf-8')
    # json_response = json.loads(response_data)
    # print(json.dumps(json_response, indent=4))
    # print()
    connection.close()





def send_get_request_status(host='load_balancer', port=5000, path='/status'):
    print("/status")
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()




if __name__ == '__main__':
    random.seed(42)

    send_post_request_config()
    send_get_request_status()

    s1 = time.time()

    for i in range(0, 10000):
        send_post_request_write(random.randint(0, 16000))
        if i % 100 == 0:
            print(f"W: {i} and time {time.time()-s1}")

    s2 = time.time()

    for i in range(0, 10000):
        low = random.randint(0, 16000)
        high = low + 100
        send_post_request_read(low, high)
        if i % 100 == 0:
            print(f"R: {i} and time {time.time()-s2}")

    s3 = time.time()

    print("Write time: ", s2-s1)
    print("Read time: ", s3-s2)

