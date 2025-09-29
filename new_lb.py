import http.client
import time
import asyncio
import random
from sortedcontainers import SortedDict
import docker
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import threading
import json
import math
import threading
import copy
import os
import mysql.connector
import mysql.connector.pooling
import asyncio
from aiohttp import web


global global_schema
num_retries = 3
client = docker.from_env()
shard_id_object_mapping = {}
initialized = False



# # Create a connection pool
# connection_pool = mysql.connector.pooling.MySQLConnectionPool(
#     pool_name="mypool",
#     pool_size=32,
#     host="lb_database",
#     user="root",
#     password="password"
# )

connection = mysql.connector.connect(
    host="lb_database",  # Container name of MySQL
    user="root",
    password="password"
)

cursor = connection.cursor()

# import subprocess

# def execute_mysql_query(query):
#     # Command to execute MySQL query using the mysql client
#     command = f"mysql -h lb_database -u root -ppassword -e \"{query}\""

#     # Execute the command and capture the output
#     process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     output, error = process.communicate()

#     return output.decode('utf-8')





class Metadata:
    def __init__(self):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS Metadata")
        cursor.execute("USE Metadata")

        # print("Creating tables")
        table_name = "ShardT"
        create_table_query = f"CREATE TABLE {table_name} ( Stud_id_low INT, Shard_id INT, Shard_size INT, Valid_idx INT, Update_idx INT);"
        # print(create_table_query)
        cursor.execute(create_table_query)

        table_name = "MapT"
        create_table_query = f"CREATE TABLE {table_name} ( Shard_id INT, Server_id INT);"
        # print(create_table_query)
        cursor.execute(create_table_query)
        connection.commit()
        # cursor.close()
        # connection.close()

    def add_shard(self, shard_id, shard_size,shard_id_low):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Inside add_shard function. adding {shard_id} info to ShardT")
        insert_query = f"INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size, Valid_idx, Update_idx) VALUES ({shard_id_low}, {shard_id}, {shard_size}, 0, -1);"
        cursor.execute(insert_query)
        connection.commit()    
        # cursor.close()
        # connection.close()

    def remove_shard(self, shard_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Removing shard:{shard_id} from ShardT")
        delete_query = f"DELETE FROM ShardT WHERE Shard_id = {shard_id};"
        cursor.execute(delete_query)
        connection.commit()
        # cursor.close()
        # connection.close()
    
    def add_server(self, server_id, shard_list):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"adding server:{server_id} having shard_list: {shard_list} in MapT")
        for shard in shard_list:
            insert_query = f"INSERT INTO MapT (Shard_id, Server_id) VALUES ({shard}, {server_id});"
            cursor.execute(insert_query)
        connection.commit()
        # cursor.close()
        # connection.close()

    def remove_server(self, server_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Removing server:{server_id} from MapT")
        delete_query = f"DELETE FROM MapT WHERE Server_id = {server_id};"
        cursor.execute(delete_query)
        connection.commit()
        # cursor.close()
        # connection.close()

    def get_shards(self, server_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Getting shards for server:{server_id}")
        select_query = f"SELECT Shard_id FROM MapT WHERE Server_id = {server_id};"
        cursor.execute(select_query)
        shard_list = cursor.fetchall()
        # cursor.close()
        # connection.close()
        return shard_list

    def get_all_shards(self):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Getting all shards")
        select_query = f"SELECT Stud_id_low, Shard_id, Shard_size FROM ShardT;"
        cursor.execute(select_query)
        shard_list = cursor.fetchall()
        # cursor.close()
        # connection.close()
        return shard_list
    
    def get_shard_id(self, stud_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Getting shard_id for stud_id:{stud_id}")
        select_query = f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {stud_id} AND Stud_id_low + Shard_size > {stud_id};"
        cursor.execute(select_query)
        shard_id = cursor.fetchall()
        # cursor.close()
        # connection.close()
        if len(shard_id) == 0:
            return None
        return shard_id[0][0]
    
    def get_server_id(self, shard_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Getting server_id for shard_id:{shard_id}")
        select_query = f"SELECT Server_id FROM MapT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        server_ids = cursor.fetchall()
        # cursor.close()
        # connection.close()
        return server_ids
    
    def get_valid_idx(self, shard_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Getting valid_idx for shard_id:{shard_id}")
        select_query = f"SELECT Valid_idx FROM ShardT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        valid_idx = cursor.fetchall()
        # cursor.close()
        # connection.close()
        return valid_idx[0][0]
    
    def get_update_idx(self, shard_id):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Getting update_idx for shard_id:{shard_id}")
        select_query = f"SELECT Update_idx FROM ShardT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        update_idx = cursor.fetchall()
        # cursor.close()
        # connection.close()
        return update_idx[0][0]
    
    def update_valid_idx(self, shard_id, valid_idx):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Updating valid_idx for shard_id:{shard_id} to {valid_idx}")
        update_query = f"UPDATE ShardT SET Valid_idx = {valid_idx} WHERE Shard_id = {shard_id};"
        cursor.execute(update_query)
        connection.commit()
        # cursor.close()
        # connection.close()

    def update_update_idx(self, shard_id, update_idx):
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        # print(f"Updating update_idx for shard_id:{shard_id} to {update_idx}")
        update_query = f"UPDATE ShardT SET Update_idx = {update_idx} WHERE Shard_id = {shard_id};"
        cursor.execute(update_query)
        connection.commit()
        # cursor.close()
        # connection.close()







     




class Shards:
    def __init__(self):
        # print("Creating shard object")
        self.num_serv = 0
        self.counter = 0
        self.serv_dict = {}
        self.buf_size = 512
        self.num_vservs = int(math.log2(self.buf_size))
        self.cont_hash = [[None, None] for _ in range(self.buf_size)]
        self.serv_id_dict = SortedDict()
        self.mutex = threading.Lock()


    def get_hash(self,host_name):
        # print("At get hash")
        li=[]
        for j in range(self.num_vservs):
            prime_multiplier = 37
            magic_number = 0x5F3759DF
            constant_addition = 11
            random_number=random.randint(0,100000)

            nindex = ((random_number*random_number + j*j + 2 * j + 25) * prime_multiplier) ^ magic_number
            nindex = (nindex + constant_addition) % self.buf_size
            nindex = (nindex ^ (nindex & (nindex ^ (nindex - 1)))) % self.buf_size
            nindex+=self.buf_size
            nindex%=self.buf_size
            jp=0
            while(jp<self.buf_size):
                if self.cont_hash[nindex][0]!=None:
                    nindex+=1
                    nindex%=self.buf_size
                else:
                    self.serv_id_dict[nindex]= None
                    self.cont_hash[nindex][0]=host_name
                    li.append(nindex)
                    break
                jp+=1
        # print(f"Returning from get hash function and the list is {li}")
        return li

    def client_hash(self,r_id):
        # print("At client hash")
        prime_multiplier = 31
        magic_number = 0x5F3759DF
        constant_addition = 7

        nindex = ((r_id + 2 * r_id + 17) * prime_multiplier) ^ magic_number
        nindex = (nindex + constant_addition) % self.buf_size
        nindex = (nindex ^ (nindex & (nindex ^ (nindex - 1)))) % self.buf_size  
        nindex+=self.buf_size
        nindex%=self.buf_size
        nindex += 1
        nindex %= self.buf_size
        if(len(self.serv_id_dict) != 0):
            lower_bound_key = self.serv_id_dict.bisect_left(nindex)
            if lower_bound_key == len(self.serv_id_dict):
                # print(f"Returning from client hash function and the server is {self.cont_hash[self.serv_id_dict.iloc[0]][0]}")
                return self.cont_hash[self.serv_id_dict.iloc[0]][0], ((nindex-1)+self.buf_size)%self.buf_size
            else:
                # print(f"Returning from client hash function and the server is {self.cont_hash[self.serv_id_dict.iloc[lower_bound_key]][0]}")
                return self.cont_hash[self.serv_id_dict.iloc[lower_bound_key]][0], ((nindex-1)+self.buf_size)%self.buf_size
        else:
            return None
        

    def rm_server(self,host_name):
        # print(f"Removing server {host_name}")

        indexes=self.serv_dict[host_name][0]
        # print(f"Indexes are {indexes} and the server is {host_name}")
        # with self.mutex:
        # print(f"Inside mutex for removing server {host_name}")
        for ind in indexes:
            self.cont_hash[ind][0]=None
            del self.serv_id_dict[ind]
        self.num_serv-=1

        del self.serv_dict[host_name]
        # print(f"Removed server {host_name}")

    def add_server(self,serv_id):
        # print(f"Adding server {serv_id}")
        self.num_serv += 1
        self.counter += 1
        
        serv_listid = self.get_hash(serv_id)

        self.serv_dict[serv_id] = [serv_listid,0]
        # print(f"Added server {serv_id} with indexes {serv_listid}")
        return








class Servers: 
    def __init__(self):
        self.mutex = threading.Lock()
    server_to_docker_container_map = {}        

    def add_server(self, server_id, shard_list):
        # print(f"Inside add_server function of servers_obj for server:{server_id} having shard list:{shard_list}")
        global metadata_obj
        global client

        server_name="server"+str(server_id)

        # print(f"adding server {server_id} in metadata_obj")
        metadata_obj.add_server(server_id, shard_list)
        environment_vars = {'ID': server_id}
        # print("making server container")
        container = client.containers.run("server_image", detach=True, hostname = server_name, name = server_name, network ="my_network", environment=environment_vars)
        time.sleep(5)
        self.server_to_docker_container_map[server_id] = container
        # print(f"calling configure_and_setup for {server_id}")
        configure_and_setup(server_id, shard_list)
        return
    
    def remove_server(self, server_id):
        # print(f"Inside remove_server function of servers_obj for server:{server_id}")
        if server_id not in self.server_to_docker_container_map.keys():
            # print(f"Server {server_id} not found")
            # print(self.server_to_docker_container_map.keys())
            return -1
        global metadata_obj
        shard_list = metadata_obj.get_shards(server_id)
        for i in range(len(shard_list)):
            shard_list[i]=int(shard_list[i][0])
        # print(f"Removing server {server_id} from metadata_obj")
        for shard in shard_list:
            # print(f"removing server {server_id} from shard {shard}")
            shard_obj = shard_id_object_mapping[shard]
            with shard_obj.mutex:
                shard_id_object_mapping[shard].rm_server(server_id)
                if(len(shard_id_object_mapping[shard].serv_dict)==0):
                    # print(f"deleting shard object for {shard}")
                    shard_id_object_mapping.pop(shard)
                    metadata_obj.remove_shard(shard)

        metadata_obj.remove_server(server_id)
        try:
            # print(f"Stopping and removing container of server:{server_id}")
            container = self.server_to_docker_container_map[server_id]
            container.stop()
            container.remove()
            time.sleep(5)
        except:
            # print("No such container found!!")
            pass
        # print("Server" + str(server_id) + " removed")
        self.server_to_docker_container_map.pop(server_id)
        return 1





def send_request(host='server', port=5000, path='/config',payload={},method='POST'):
    headers = {'Content-type': 'application/json'}

    json_payload = json.dumps(payload)
    
    connection = http.client.HTTPConnection(host, port)
    # print(f'Sending {method} request to {host}:{port}{path} with payload: {json_payload}')
    connection.request(method, path, json_payload, headers)

    response = connection.getresponse()
    # print(f'Status: {response.status}')

    # print('Response:')
    response_data = response.read().decode('utf-8')
    json_response = json.loads(response_data)
    connection.close()
    # print(json_response)
    return json_response, response.status




def client_request_sender(shard_id, path, payload, method):
    # print(f"Sending request to shard:{shard_id} with path:{path} and payload:{payload} and method:{method}")
    shard_obj = shard_id_object_mapping[shard_id]
    # with shard_obj.mutex:
    while(1):
        try:
            rid = random.randrange(99999, 1000000, 1)
            out = shard_obj.client_hash(rid)
            if out==None:
                return None
            # print(f"Sending request to server:{out[0]}")
            server_name = "server" + str(out[0])
            response, _ = send_request(server_name, 5000, path, payload, method)
            return response
        except:
            continue
    # with shard_obj.mutex:
    #     shard_obj.cont_hash[out[1]][1] = None
    # return response





def configure_server(server_id, shard_list):
    # print(f"Configuring server:{server_id} with shard_list:{shard_list}")

    shards = []
    for e in shard_list:
        shards.append("sh"+str(e))
    
    global global_schema
    Payload_Json= {
    "schema": global_schema,
    "shards": shards,
    }
    resp, _=send_request('server'+str(server_id), 5000, '/config',Payload_Json,'POST')
    return resp






def configure_and_setup(server_id, shard_list):
    # print(f"calling configure_server for {server_id} having {shard_list}") 
    configure_server(server_id, shard_list)
    for shard in shard_list:
        # print(f"Inside for loop for shard:{shard}")
        shard_obj = shard_id_object_mapping[shard]
        with shard_obj.mutex:
            if len(shard_obj.serv_dict.keys()) == 0:
                # print(f"adding server:{server_id} to object of {shard}")
                shard_id_object_mapping[shard].add_server(server_id)
                continue
        shard_name = "sh" + str(shard)
        response = None
        while(1):
            try:
                rid = random.randrange(99999, 1000000, 1)
                with shard_obj.mutex:
                    out = shard_obj.client_hash(rid)
                serv_id = out[0]
                server_name = "server" + str(serv_id)
                payload_shard_list = []
                payload_shard_list.append(shard_name)
                payload = {
                    "shards": payload_shard_list
                }
                # print(f"Copying shard:{shard} to server:{server_id}")
                response, _ = send_request(server_name, 5000, '/copy', payload, 'GET')
                # with shard_obj.mutex:
                #     shard_obj.cont_hash[out[1]][1]=None
                # print(f"adding server:{server_id} to object of {shard}")
                break
            except:
                # print(f"Error in copying shard:{shard} to server:{server_id}")
                continue
        
        payload = {
            "shard": shard_name,
            "curr_idx": 0,
            "data": response['sh'+str(shard)]
        }
        server_name = "server" + str(server_id)   
        response, _ = send_request(server_name, 5000, '/write', payload, 'POST')
        cur_valid_idx = metadata_obj.get_valid_idx(shard)
        response2 = server_updateid(server_id, shard, cur_valid_idx)
        with shard_obj.mutex:
            shard_id_object_mapping[shard].add_server(server_id)
            shard_id_object_mapping[shard].serv_dict[server_id][1] = int(response['current_idx']) 
    return        
    
    
    
        






def send_get_request(host='localhost', port=5000, path='/'):
    # print(f"Sending GET request to {host}:{port}{path}")
    connection = http.client.HTTPConnection(host, port)
    connection.request('GET', path)
    
    response = connection.getresponse()
    # print(f'Status: {response.status} Response: {response.read().decode("utf-8")}')
    
    connection.close()
    return response



def send_get_request_with_timeout(host_name='localhost', port=5000, path='/'):
    global metadata_obj
    global servers_obj
    try:
        connection = http.client.HTTPConnection(host_name, port, timeout=5)    
        # print(f"Sending heartbeat request to  {host_name}")
        connection.request('GET', path)
        response = connection.getresponse()
        response.read()
        connection.close()
    except Exception as e:
        # print(e)
        # print(f"ERROR!! Heartbeat response not received from {host_name}")
        with servers_obj.mutex:
            server_id = int(host_name[6:])
            shard_list = metadata_obj.get_shards(server_id)
            for i in range(len(shard_list)):
                shard_list[i]=int(shard_list[i][0])
            status = servers_obj.remove_server(server_id)
            if status == -1:
                return
            servers_obj.add_server(server_id, shard_list)
    return

def thread_heartbeat():
    global servers_obj
    # print("Heartbeat thread started")
    while(1):
        with servers_obj.mutex:
            host_list = []
            for server_id in servers_obj.server_to_docker_container_map.keys():
                host_list.append("server" + str(server_id))
        for host_name in host_list:
                send_get_request_with_timeout(host_name, 5000, '/heartbeat')
        time.sleep(5)














def server_copy(shard_list, server_id):
    # print(f"Copying shards: {shard_list} to server: {server_id}")
    payload = {
        "shards": shard_list
    }
    server_name = "server" + str(server_id)   
    response, _ = send_request(server_name, 5000, '/copy', payload, 'GET')
    return response

def server_read(shard, Stud_id_low, Stud_id_high, server_id):
    # print(f"Reading from shard: {shard} in server: {server_id} with Stud_id_low: {Stud_id_low} and Stud_id_high: {Stud_id_high}")
    Stud_id_range = {"low": Stud_id_low, "high": Stud_id_high}
    payload = {
        "shard": shard,
        "Stud_id": Stud_id_range
    }
    server_name = "server" + str(server_id)   
    response, _ = send_request(server_name, 5000, '/read', payload, 'POST')
    return response

def server_write(shard, curr_idx, data, server_id):
    # print(f"Writing to shard: {shard} in server: {server_id} with curr_idx: {curr_idx} and data: {data}")
    payload = {
        "shard": shard,
        "curr_idx": curr_idx,
        "data": data
    }
    server_name = "server" + str(server_id) 
    # print("Payload")
    # print(payload)  
    response, _ = send_request(server_name, 5000, '/write', payload, 'POST')
    return response

def server_update(shard, Stud_id, sname, smarks, server_id):
    # print(f"Updating the student with Stud_id: {Stud_id} in shard:  {shard} in server: {server_id} with Stud_name: {sname} and Stud_marks: {smarks}")
    sid=Stud_id
    data = {"Stud_id": sid, "Stud_name": sname, "Stud_marks": smarks}
    payload = {
        "shard": shard,
        "Stud_id": Stud_id,
        "data": data
    }
    server_name = "server" + str(server_id)   
    response, status = send_request(server_name, 5000, '/update', payload, 'PUT')
    return response, status

def remove_entry(shard, Stud_id, server_id):
    # print(f"Deleting the student with Stud_id: {Stud_id} from shard:  {shard} in server: {server_id}")
    payload = {
        "shard": shard,
        "Stud_id": Stud_id
    }
    server_name = "server" + str(server_id)   
    response, status_code = send_request(server_name, 5000, '/del', payload, 'DELETE')
    return response, status_code 

def server_updateid(server_id, shard_id, update_idx):
    # print(f"Updating update_idx in shard: {shard_id}  in server: {server_id}  to {update_idx}")
    shard = "sh" + str(shard_id)
    payload = {
        "shard": shard,
        "update_idx": update_idx
    }
    server_name = "server" + str(server_id)   
    response, _ = send_request(server_name, 5000, '/updateid', payload, 'POST')
    return response




metadata_obj=Metadata()
servers_obj=Servers()


async def generate_random_id():
    # print("Generating random id")
    rand_int = random.randint(500000, 1000000)
    while rand_int in servers_obj.server_to_docker_container_map.keys():
        rand_int = random.randint(500000, 1000000)
    return rand_int















class Post_handler:
    async def init_handler(self, request):
        global initialized
        global servers_obj
        global metadata_obj
        global global_schema
        # print("Inside \"/init\" endpoint")
        data = await request.json()
        num_servers = int(data["N"])
        schema = data["schema"]
        shards_info = data["shards"]
        shard_server_mapping = data["servers"]
        global_schema = schema
        for shard in shards_info:
            shard_id = shard["Shard_id"]
            shard_id = int(shard_id[2:])
            # print(f"Creating shard object for {shard}")
            shard_id_object_mapping[shard_id] = Shards()
            # print(f"adding details of {shard} in metadata object")
            metadata_obj.add_shard(shard_id, int(shard["Shard_size"]), int(shard["Stud_id_low"]))
        for server_name, shard_list in shard_server_mapping.items():
            server_id = int(server_name[6:])
            for i in range(len(shard_list)):
                shard_list[i] = int(shard_list[i][2:])
            with servers_obj.mutex:
                # print(f"adding {server_id} to server_obj")
                servers_obj.add_server(server_id, shard_list)
        initialized = True
        response_data = {'message': 'Configured Database', 'status': 'success'}
        return web.json_response(response_data, status=200)


    async def write_handler(self, request):
        global initialized
        global servers_obj
        global metadata_obj
        global global_schema
        if initialized == False:
            return web.json_response({"message": "<Error> Database not initialized", "status": "failure"}, status=400)
        # print("Inside \"/write\" endpoint")
        # check if content is correct json object or not
        content = await request.json()
        # print(type(content))
        data = content["data"]
        shard_grp = {}
        for entry in data:
            stud_id = int(entry["Stud_id"])
            shard_id = metadata_obj.get_shard_id(stud_id)
            if shard_id is None:
                server_response = {"message": "<Error> Shard not found", "status": "failure"}
                return web.json_response(server_response, status=400)
            if shard_id not in shard_grp.keys():
                shard_grp[shard_id] = []
            shard_grp[shard_id].append(entry)
        # print(shard_grp)
        # # print("write ke under hu")
        for shard_id, entries_list in shard_grp.items():
            shard_obj = shard_id_object_mapping[shard_id]
            with shard_obj.mutex:
                # # print("Mutex ke under hu")
                last_idx=0
                cur_valid_idx = metadata_obj.get_valid_idx(shard_id)
                for server_id in shard_obj.serv_dict.keys():
                    try:
                        response = server_write('sh'+str(shard_id), cur_valid_idx, entries_list, server_id)
                        shard_obj.serv_dict[server_id][1] = int(response["current_idx"])
                        last_idx = int(response["current_idx"])
                    except:
                        continue
                metadata_obj.update_valid_idx(shard_id, last_idx)
                for server_id in shard_obj.serv_dict.keys():
                    try:
                        response = server_updateid(server_id, shard_id, metadata_obj.get_valid_idx(shard_id))
                    except:
                        # print(f"Error in updating update_idx on server{server_id}")
                        continue
                    
        server_response = {"message": f"{len(data)} entries added", "status": "success"} 
        return web.json_response(server_response, status=200)







    async def read_handler(self, request):
        global initialized
        global servers_obj
        global metadata_obj
        global global_schema
        if initialized == False:
            return web.json_response({"message": "<Error> Database not initialized", "status": "failure"}, status=400)
        # print("In read endpoint")
        content = await request.json()
        low=content["low"]
        high=content["high"]
        response_payload={}
        response_payload['shards_queried']=[]
        response_payload['data']=[]
        # connection = connection_pool.get_connection()
        # cursor = connection.cursor()
        cursor.execute("USE Metadata")
        #check if the update_index is not in this range
        query=f"SELECT Shard_id FROM ShardT WHERE Update_idx>={low} AND Update_idx<={high};"
        cursor.execute(query)
        res=cursor.fetchall()
        if len(res)>0:
            server_response = {"message": "<Error> Update_Index mein load hai ", "status": "failure"}
            # cursor.close()
            # connection.close()
            return web.json_response(server_response, status=400)
        
        # now select whichever shards is in the range and then query them, also select their low and sizes too
        query=f"SELECT Shard_id,Stud_id_low,Shard_size FROM ShardT WHERE Stud_id_low<={high} AND Stud_id_low+Shard_size>={low};"
        cursor.execute(query)
        res=cursor.fetchall()
        # cursor.close()
        # connection.close()
        for shard in res:
            # print(shard)
            shard_id=shard[0]
            sh_low=shard[1]
            size=shard[2]
            payload={}
            payload['shard']='sh'+str(shard_id)
            payload['Stud_id']={}
            payload['Stud_id']['low']=max(low,sh_low)
            payload['Stud_id']['high']=min(high,sh_low+size)
            response=client_request_sender(shard_id, '/read',payload,'POST')
            if response==None:
                continue
            response_payload['shards_queried'].append('sh'+str(shard))
            for entries in response['data']:
                response_payload['data'].append(entries)

        response_payload['status']='success'
        return web.json_response(response_payload, status=200)
        

    
    async def add_handler(self, request):
        global initialized
        global servers_obj
        global metadata_obj
        global global_schema
        if initialized == False:
            return web.json_response({"message": "<Error> Database not initialized", "status": "failure"}, status=400)
        # print("In add endpoint")
        data = await request.json()
        num_servers = int(data["n"])
        new_shards = data["new_shards"]
        server_list = data["servers"]

        if num_servers != len(server_list):
            return web.json_response({"message": "<Error> Number of new servers (n) is invalid", "status": "failure"}, status=400)

        for shard in new_shards:
            shard_id = shard["Shard_id"]
            if not shard_id.startswith("sh"):
                return web.json_response({"message": "<Error> Shard ID should start with 'sh'", "status": "failure"}, status=400)
            try:
                shard_id = int(shard_id[2:])
            except:
                return web.json_response({"message": "<Error> Shard ID should be an integer", "status": "failure"}, status=400)
            shard_id_object_mapping[shard_id] = Shards()
            metadata_obj.add_shard(shard_id, int(shard["Shard_size"]), int(shard["Stud_id_low"]))
        
        serv_id_list = []
        for server_name, shard_list in server_list.items():
            if not server_name.startswith("Server"):
                server_id = generate_random_id()
            else:
                try:
                    server_id = int(server_name[6:])
                except:
                    server_id = generate_random_id()
            
            for i in range(len(shard_list)):
                shard_list[i] = int(shard_list[i][2:])
            with servers_obj.mutex:
                servers_obj.add_server(server_id, shard_list)
            serv_id_list.append(server_id)

        serv_id_list = [f"Server:{serv_id}" for serv_id in serv_id_list]
        return web.json_response({"N": len(servers_obj.server_to_docker_container_map), "message": f"Added Server: {serv_id_list}", "status": "success"}, status=200)

        



class Put_handler:
    async def update_handler(self,request):
        global initialized
        global servers_obj
        global metadata_obj
        global global_schema
        if initialized == False:
            return web.json_response({"message": "<Error> Database not initialized", "status": "failure"}, status=400)
        # print("In update endpoint")
        content = await request.json()
        
        Stud_id = int(content["Stud_id"])
        data = content["data"]
        
        sid = int(data["Stud_id"])
        sname = data["Stud_name"]
        smarks = data["Stud_marks"]
        
        shard_id = metadata_obj.get_shard_id(Stud_id)
        if shard_id is None:
            return web.json_response({"message": "<Error> Shard not found", "status": "failure"}, status=400)
        
        shard_obj = shard_id_object_mapping[shard_id]

        with shard_obj.mutex:
            metadata_obj.update_update_idx(shard_id, sid)
            for server_id in shard_obj.serv_dict.keys():
                try:
                    response, status = await server_update('sh'+str(shard_id), sid, sname, smarks, server_id)
                    if status == 400:
                        return web.json_response({"message": "<Error> Entry not found", "status": "failure"}, status=400)
                except:
                    continue
            metadata_obj.update_update_idx(shard_id, -1)
                
        return web.json_response({"message": f"Data entry for Stud_id: {Stud_id} updated", "status": "success"}, status=200)


class Get_handler:
    async def status_handler(self,request):
        global initialized
        global servers_obj
        global metadata_obj
        global global_schema

        if initialized == False:
            return web.json_response({"message": "<Error> Database not initialized", "status": "failure"}, status=400)
        
        # print("In status endpoint")
        payload = {}
        payload['N'] = len(servers_obj.server_to_docker_container_map)
        payload['schema'] = global_schema
        shard_info = metadata_obj.get_all_shards()
        out_list = []
        for i_shard_info in shard_info:
            temp_dict = {}
            temp_dict['Stud_id_low'] = i_shard_info[0]
            temp_dict['Shard_id'] = "sh" + str(i_shard_info[1])
            temp_dict['Shard_size'] = i_shard_info[2]
            out_list.append(temp_dict)

        payload['shards'] = out_list
        payload['servers'] = {}
        for server_id in servers_obj.server_to_docker_container_map.keys():
            shard_list = metadata_obj.get_shards(server_id)
            shard_list = ['sh' + str(shard[0]) for shard in shard_list]
            payload['servers']['Server'+str(server_id)] = shard_list

        return web.json_response(payload, status=200)
    
class Delete_handler:
    async def remove_handler(self, request):
        global initialized
        global servers_obj
        global metadata_obj
        global global_schema

        if initialized == False:
            return web.json_response({"message": "<Error> Database not initialized", "status": "failure"}, status=400)
        
        # print("Inside remove handler")
        data = await request.json()
        num_servs = int(data["n"])
        server_list = data["servers"]
        rm_servs = []
        if num_servs > len(servers_obj.server_to_docker_container_map) or num_servs < len(server_list):
            return web.json_response({"message": "<Error> Length of server list is invalid", "status": "failure"}, status=400)
        
        cur = 0
        for server in server_list:
            server_id = int(server[6:])
            with servers_obj.mutex:
                status = servers_obj.remove_server(server_id)
            if status == -1:
                rm_servs.append(server + " not found")
            else:
                rm_servs.append(server)
            cur += 1
        
        while cur < num_servs:
            leng = len(servers_obj.server_to_docker_container_map)
            random_idx = random.randint(0, leng-1)
            server_id = list(servers_obj.server_to_docker_container_map.keys())[random_idx]
            with servers_obj.mutex:
                servers_obj.remove_server(server_id)
            rm_servs.append("Server:"+str(server_id))
            cur += 1

        message = {
            'N': len(servers_obj.server_to_docker_container_map),
            'servers': rm_servs
        }

        return web.json_response({"message": message, "status": "successful"}, status=200)
        

    async def delete_handler(self, request):
        global initialized
        global servers_obj
        global metadata_obj
        global global_schema
        if initialized == False:
            return web.json_response({"message": "<Error> Database not initialized", "status": "failure"}, status=400)
        # print("Inside delete handler")
        data = await request.json()
        Stud_id = int(data["Stud_id"])
        shard_id = metadata_obj.get_shard_id(Stud_id)
        if shard_id is None:
            return web.json_response({"message": "<Error> Shard not found", "status": "failure"}, status=400)

        
        shard_obj = shard_id_object_mapping[shard_id]
        with shard_obj.mutex:
            metadata_obj.update_update_idx(shard_id, Stud_id)
            for server_id in shard_obj.serv_dict.keys():
                try:
                    response, status_code = await remove_entry('sh'+str(shard_id), Stud_id, server_id)
                    if status_code != 200:
                        return web.json_response({"message": "<Error> Entry not found", "status": "failure"}, status=400)
                except:
                    continue
            
            metadata_obj.update_update_idx(shard_id, -1)
            metadata_obj.update_valid_idx(shard_id, metadata_obj.get_valid_idx(shard_id)-1)
            
        
        return web.json_response({"message": "Entry deleted", "status": "success"}, status=200)

handle_get = Get_handler()
handle_post = Post_handler()
handle_put = Put_handler()
handle_delete = Delete_handler()

app = web.Application()
app.router.add_post('/init', handle_post.init_handler)
app.router.add_post('/add', handle_post.add_handler)
app.router.add_post('/read', handle_post.read_handler)
app.router.add_post('/write', handle_post.write_handler)

app.router.add_get('/status', handle_get.status_handler)

app.router.add_put('/update', handle_put.update_handler)

app.router.add_delete('/rm', handle_delete.remove_handler)
app.router.add_delete('/del', handle_delete.delete_handler)
            

if __name__ == '__main__':
    # print('Starting server on port 5000...')
        
    try:
        heart_beat_thread = threading.Thread(target=thread_heartbeat)
        heart_beat_thread.start()
        web.run_app(app, port=5000)
        # httpd.serve_forever()
        heart_beat_thread.join()

    except KeyboardInterrupt:
        # print('Server is shutting down...')
        # httpd.shutdown()
        exit()
    
    