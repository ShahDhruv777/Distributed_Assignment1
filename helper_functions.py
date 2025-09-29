from packages import *


def send_request(host='server', port=5000, path='/config',payload={},method='POST'):
    # print(path)
    # payload = {
    #     "schema": {"columns": ["Stud_id", "Stud_name", "Stud_marks"], "dtypes": ["Number", "String", "String"]},
    #     "shards": ["sh1", "sh2"]
    # }
    headers = {'Content-type': 'application/json'}
    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)

    connection.request(method, path, json_payload, headers)

    response = connection.getresponse()
    print(f'Status: {response.status}')
    print('Response:')
    print(response.read().decode('utf-8'))
    print()
    connection.close()
    return response




def client_request_sender(shard_id, path, payload, method):
    rid = random.randrange(99999, 1000000, 1)
    shard_obj = shard_id_object_mapping[shard_id]
    with shard_obj.mutex:
        serv_id, index = shard_obj.client_hash(rid)
    server_name = "server" + str(serv_id)
    response = send_request(server_name, 5000, path, payload, method)
    with shard_obj.mutex:
        shard_obj.cont_hash[index][1] = None
    return response





def configure_server(server_id, shard_list):
    global global_schema
    Payload_Json= {
    "schema": global_schema,
    "shards": shard_list,
    }
    resp=send_request('server'+str(server_id), 5000, '/config',Payload_Json,'POST')
    return resp






def configure_and_setup(server_id, shard_list):
    configure_server(server_id, shard_list)
    # rid = random.randrange(99999, 1000000, 1)
    for shard in shard_list:
        rid = random.randrange(99999, 1000000, 1)
        shard_obj = shard_id_object_mapping[shard]
        with shard_obj.mutex:
            out = shard_obj.client_hash(rid)
        if out == None:
            return 
        serv_id = out[0]
        server_name = "server" + str(serv_id)
        shard_name = "sh" + str(shard)
        payload_shard_list = []
        payload_shard_list.append(shard_name)
        payload = {
            "shards": payload_shard_list
        }
        response = send_request(server_name, 5000, '/copy', payload, 'GET')
        with shard_obj.mutex:
            shard_obj.cont_hash[out[1]] = None
        shard_id_object_mapping[shard].add_server(server_id)
        payload = {
            "shard": shard_name,
            "curr_idx": 0,
            "data": response['data']
        }
        server_name = "server" + str(server_id)   
        response = send_request(server_name, 5000, '/write', payload, 'POST')
        shard_id_object_mapping[shard].serv_dict[server_id][1] = int(response['current_idx']) 
    return        
    
    
    
        







# ports = {"server1": 8001, "server2": 8002}
def send_get_request(host='localhost', port=5000, path='/'):
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)
    
    response = connection.getresponse()
    # print(f'Status: {response.status}')
    # print('Response:')
    # print(response.read().decode('utf-8'))
    
    connection.close()
    return response



def send_get_request_with_timeout(host_name='localhost', port=5000, path='/'):
    global metadata_obj
    global servers_obj
    try:
        connection = http.client.HTTPConnection(host_name, port, timeout=5)    
        print("Sending heartbeat request to " + host_name)
        connection.request('GET', path)
        response = connection.getresponse()
        response.read()
        connection.close()
    except Exception as e:
        with servers_obj.mutex:
            server_id = host_name[6:]
            shard_list = metadata_obj.get_shards(server_id)
            servers_obj.remove_server(server_id)
            servers_obj.add_server(server_id, shard_list)
        print(e)
        print("ERROR!! Heartbeat response not received from " + host_name)
    return

def thread_heartbeat():
    global servers_obj
    print("Heartbeat thread started")
    print(servers_obj)
    while(1):
        with servers_obj.mutex:
            host_list = []
            for server_id in servers_obj.server_to_docker_container_map.keys():
                host_list.append("server" + str(server_id))
            # host_list = copy.deepcopy(shared_data.serv_dict)
        for host_name in host_list:
                send_get_request_with_timeout(host_name, 5000, '/heartbeat')
        time.sleep(5)














def server_copy(shard_list, server_id):
    payload = {
        "shards": shard_list
    }
    server_name = "server" + str(server_id)   
    response = send_request(server_name, 5000, '/copy', payload, 'GET')
    return response

def server_read(shard, Stud_id_low, Stud_id_high, server_id):
    Stud_id_range = {"low": Stud_id_low, "high": Stud_id_high}
    payload = {
        "shard": shard,
        "Stud_id": Stud_id_range
    }
    server_name = "server" + str(server_id)   
    response = send_request(server_name, 5000, '/read', payload, 'POST')
    # if(response['status'] != "Success"):
    #     return None
    return response

def server_write(shard, curr_idx, data, server_id):
    payload = {
        "shard": shard,
        "curr_idx": curr_idx,
        "data": data
    }
    server_name = "server" + str(server_id)   
    response = send_request(server_name, 5000, '/write', payload, 'POST')
    return response

def server_update(shard, Stud_id, sid, sname, smarks, server_id):
    data = {"Stud_id": sid, "Stud_name": sname, "Stud_marks": smarks}
    payload = {
        "shard": shard,
        "Stud_id": Stud_id,
        "data": data
    }
    server_name = "server" + str(server_id)   
    response = send_request(server_name, 5000, '/update', payload, 'PUT')
    return response

def server_delete(shard, Stud_id, server_id):
    payload = {
        "shard": shard,
        "Stud_id": Stud_id
    }
    server_name = "server" + str(server_id)   
    response = send_request(server_name, 5000, '/delete', payload, 'DELETE')
    return response    

def server_updateid(server_id, shard_id, update_idx):
    shard = "sh" + str(shard_id)
    payload = {
        "shard": shard,
        "update_idx": update_idx
    }
    server_name = "server" + str(server_id)   
    response = send_request(server_name, 5000, '/updateid', payload, 'POST')
    return response