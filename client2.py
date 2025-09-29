from codecs import encode, decode
import http.client
import json

def send_post_request_config(host='load_balancer', port=5000, path='/config'):
    print("/config")
    # payload = {
    #     "N":3,
    #     "schema":{
    #         "columns":["Stud_id","Stud_name","Stud_marks"],
    #         "dtypes":["Number","String","String"]
    #     },
    #     "shards":[{"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096},
    #     {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096},
    #     {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size":4096},],
    #     "servers":{
    #         "Server0":["sh1","sh2"],
    #         "Server1":["sh2","sh3"],
    #         "Server2":["sh1","sh3"]
    #     }
    # }
    payload = {
        "schema":{"columns":["Stud_id","Stud_name","Stud_marks"],
        "dtypes":["Number","String","String"]},
        "shards":["sh1","sh2"]
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

def send_get_request_heartbeat(host='load_balancer', port=5000, path='/heartbeat'):
    print("/heartbeat")
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()

# Define other functions...
def send_get_request_copy(host='load_balancer', port=5000, path='/copy'):
    print("/copy")
    payload = {
        "shards": ["sh1", "sh2"]
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    # print(json.dumps(json_response, indent=4))
    print(json_response)
    print()
    connection.close()

def send_post_request_read(host='load_balancer', port=5000, path='/read'):
    print("/read")
    payload = {
        "shard":'sh1',
        'Stud_id':{"low":0,"high":15}
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('POST', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    # response_data = decode(encode(response_data, 'latin-1', 'backslashreplace'), 'unicode-escape')
    response_data = response.read().decode('utf-8')
    # print(response_data)
    json_response = json.loads(response_data)
    print(json_response)
    # print(json.dumps(json_response, indent=4))
    print()
    connection.close()

def send_post_request_write(index=0,host='load_balancer', port=5000, path='/write'):
    print("/write")
    payload = {
        "shard":"sh1",
        "curr_idx":2,
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
    print(f'Status: {response.status}')
    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    print(json.dumps(json_response, indent=4))
    print()
    connection.close()

def send_put_request_update(index=0,host='load_balancer', port=5000, path='/update'):
    print("/update")
    payload = {
        "shard":"sh1",
        "Stud_id": index,
        "data": {"Stud_id": 28, "Stud_name": "GHI", "Stud_marks": "28"}
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('PUT', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    print(json.dumps(json_response, indent=4))
    print()
    connection.close()

import http.client
import json

def send_delete_request_del(index=0,host='load_balancer', port=5000, path='/del'):
    print("/del")
    payload = {
        "shard":"sh1",
        "Stud_id": index
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('DELETE', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    print(json.dumps(json_response, indent=4))
    print()
    connection.close()

def send_updateidx(index = 2, host='load_balancer', port=5000, path='/updateid'):
    print("/updateid")
    payload = {
        "shard":"sh1",
        "update_idx":index
    }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    connection.request('POST', path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    print(json.dumps(json_response, indent=4))
    print()
    connection.close()

if __name__ == '__main__':
    send_post_request_config()
    # send_get_request_heartbeat()
    send_get_request_copy()
    send_post_request_write(4)
    send_updateidx(10)
    send_post_request_write(2)
    send_post_request_write(10)
    send_post_request_write(3)
    send_post_request_write(19)
    send_post_request_write(16)
    send_post_request_write(100)

    send_post_request_read()
    send_put_request_update(4)
    send_post_request_read()

    send_get_request_copy()
    send_post_request_read()
    send_delete_request_del(19)
    send_post_request_read()
    send_get_request_copy()
    # send_get_request_copy()


