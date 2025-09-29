from packages import *



from Datastructures import *
metadata_obj=Metadata()
servers_obj=Servers()
from helper_functions import *




# Extend SimpleHTTPRequestHandler to use shared data
class SimpleHandlerWithMutex(SimpleHTTPRequestHandler):
    global metadata_obj
    global servers_obj
    global global_schema
    def do_POST(self):
        if(self.path == '/init'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            content = json.loads(content)
            num_servers = int(content["N"])
            schema = content["schema"]
            shards_info = content["shards"]
            shard_server_mapping = content["servers"]
            global_schema = schema
            for shard in shards_info:
                shard_id = shard["Shard_id"]
                # if(shard_id[0:2]!="sh"):
                #     self.send_response(400)
                #     self.send_header('Content-type', 'application/json')
                #     self.end_headers()
                #     server_response = {"message": "<Error> Shard ID should start with 'sh'", "status": "failure"}
                #     response_str = json.dumps(server_response)
                #     self.wfile.write(response_str.encode('utf-8'))
                #     return
                # try:
                shard_id = int(shard_id[2:])
                shard_id_object_mapping[shard_id] = Shards()
                # except:
                #     self.send_response(400)
                #     self.send_header('Content-type', 'application/json')
                #     self.end_headers()
                #     server_response = {"message": "<Error> Shard ID should be an integer", "status": "failure"}
                #     response_str = json.dumps(server_response)
                #     self.wfile.write(response_str.encode('utf-8'))
                #     return
                metadata_obj.add_shard(shard_id, int(shard["Shard_size"]), int(shard["Stud_id_low"]))
            for server_name, shard_list in shard_server_mapping.items():
                server_id = int(server_name[6:])
                for i in range(len(shard_list)):
                    shard_list[i] = int(shard_list[i][2:])
                with servers_obj.mutex:
                    servers_obj.add_server(server_id, shard_list)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            server_response = {"message": "Configured Database", "status": "success"}
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return
                
        elif(self.path == '/add'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            content = json.loads(content)
            num_servers = int(content["n"])
            new_shards = content["new_shards"]
            server_list = content["servers"]

            if num_servers != len(server_list):
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                server_response = {"message": "<Error> Number of new servers (n) is greater than newly added instances", "status": "failure"}
                response_str = json.dumps(server_response)
                self.wfile.write(response_str.encode('utf-8'))
                return
            
            for shard in new_shards:
                shard_id = shard["Shard_id"]
                if(shard_id[0:2]!="sh"):
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    server_response = {"message": "<Error> Shard ID should start with 'sh'", "status": "failure"}
                    response_str = json.dumps(server_response)
                    self.wfile.write(response_str.encode('utf-8'))
                    return
                try:
                    shard_id = int(shard_id[2:])
                    shard_id_object_mapping[shard_id] = Shards()
                except:
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    server_response = {"message": "<Error> Shard ID should be an integer", "status": "failure"}
                    response_str = json.dumps(server_response)
                    self.wfile.write(response_str.encode('utf-8'))
                    return
                metadata_obj.add_shard(shard_id, int(shard["Shard_size"]), int(shard["Stud_id_low"]))

            serv_id_list = []
            for server_name, shard_list in server_list.items():
                if(server_name[0:6]!="Server"):
                    rand_int = random.randint(500000, 1000000)
                    while rand_int in servers_obj.server_to_docker_container_map:
                        rand_int = random.randint(500000, 1000000)
                    server_id = rand_int
                    # server_name = "Server" + str(rand_int)
                try:
                    server_id = int(server_name[6:])
                except:
                    rand_int = random.randint(500000, 1000000)
                    while rand_int in servers_obj.server_to_docker_container_map:
                        rand_int = random.randint(500000, 1000000)
                    server_id = rand_int
                
                for i in range(len(shard_list)):
                    shard_list[i] = int(shard_list[i][2:])
                with servers_obj.mutex:
                    servers_obj.add_server(server_id, shard_list)
                serv_id_list.append(server_id)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            for i in range(len(serv_id_list)):
                serv_id_list[i] = "Server:"+str(serv_id_list[i])

            server_response = {
                "N": len(servers_obj.server_to_docker_container_map),
                "message": f"Added Server: {serv_id_list}", 
                "status": "success"
            }
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return
        

    def do_GET(self):
        # with shared_data.mutex:
        #     shared_data.counter += 1
        #     counter_value = shared_data.counter
        if(self.path == '/status'):
            payload={}
            payload['N']=len(servers_obj.server_to_docker_container_map)
            payload['schema']=global_schema
            payload['shards']=metadata_obj.get_all_shards()
            payload['servers']={}
            for server_id, shard_list in servers_obj.server_to_shard_map.items():
                for i in range(len(shard_list)):
                    shard_list[i]='sh'+str(shard_list[i])
                payload['servers']['Server'+str(server_id)]=shard_list
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_str = json.dumps(payload)
            self.wfile.write(response_str.encode('utf-8'))
            return
        
        if(self.path == '/read'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            content = json.loads(content)
            low=content["low"]
            high=content["high"]
            response_payload={}
            response_payload['shards_queried']=[]
            response_payload['data']=[]
            for shard in shard_id_object_mapping.keys():
                select_query = f"SELECT Stud_id_low,Shard_size,Update_idx FROM ShardT WHERE Shard_id == {shard};"
                cursor.execute(select_query)
                sh_low,size,up_index = cursor.fetchall()
                sh_low=int(sh_low)
                size=int(size)
                up_index=int(up_index)
                if low >= sh_low+size or high < sh_low:
                    continue
                elif up_index >=low and up_index<=high:
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    server_response = {"message": "<Error> No entries found ", "status": "failure"}
                    response_str = json.dumps(server_response)
                    self.wfile.write(response_str.encode('utf-8'))
                    return

                else:
                    payload={}
                    payload['shard']='sh'+str(shard)
                    payload['Stud_id']={}
                    payload['Stud_id']['low']=max(low,sh_low)
                    payload['Stud_id']['high']=min(high,sh_low+size)
                    response=client_request_sender(shard, '/read',payload,'POST')
                    response_payload['shards_queried'].append('sh'+str(shard))
                    for entries in response['data']:
                        response_payload['data'].append(entries)

            # if len(response_payload)>0:
            response_payload['status']='success'
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_str = json.dumps(response_payload)
            self.wfile.write(response_str.encode('utf-8'))
            return
            # else:
            #     self.send_response(400)
            #     self.send_header('Content-type', 'application/json')
            #     self.end_headers()
            #     server_response = {"message": "<Error> No entries found ", "status": "failure"}
            #     response_str = json.dumps(server_response)
            #     self.wfile.write(response_str.encode('utf-8'))
            #     return

                    


        # if(self.path == '/rep'):
        #     self.send_response(200)
        #     self.send_header('Content-type', 'application/json')
        #     self.end_headers()
        #     server_response = {"message": {"N": shared_data.num_serv, "replicas": [hostname for hostname in shared_data.serv_dict.keys()]}, "status": "successful"}
        #     response_str = json.dumps(server_response)
        #     self.wfile.write(response_str.encode('utf-8'))
        #     return
        # elif(self.path == '/info'):
        #     self.send_response(200)
        #     self.send_header('Content-type', 'application/json')
        #     self.end_headers()
        #     counter_dict = {key: value[2] for key, value in shared_data.serv_dict.items()}
        #     # server_response = {"message": {"N": shared_data.num_serv, "replicas": [hostname for hostname in shared_data.serv_dict.keys()]}, "status": "successful"}
        #     response_str = json.dumps(counter_dict)
        #     self.wfile.write(response_str.encode('utf-8'))
        #     return

        # else:
        #     rid = random.randrange(99999, 1000000, 1)
        #     cnt = num_retries
        #     while(1):
        #         cnt -= 1
        #         with shared_data.mutex:
        #             serv_id, index = shared_data.client_hash(rid)
        #         if(cnt == 0):
        #             self.send_response(500)
        #             self.send_header('Content-type', 'application/json')
        #             self.end_headers()
        #             server_response = {"message": "<Error> Server not found", "status": "failure"}
        #             response_str = json.dumps(server_response)
        #             self.wfile.write(response_str.encode('utf-8'))
        #             with shared_data.mutex:
        #                 shared_data.cont_hash[index][1] = None
        #             return
        #         if serv_id == None:
        #             with shared_data.mutex:
        #                 shared_data.cont_hash[index][1] = None
        #             continue
        #         else:
        #             # port = ports[serv_id]
        #             shared_data.serv_dict[serv_id][2] += 1
        #             response = send_get_request(serv_id, 5000, self.path)
        #             # response = send_get_request('localhost', port, self.path)
        #             self.send_response(response.status)
        #             self.send_header('Content-type', response.getheader('Content-type'))
        #             self.end_headers()
        #             self.wfile.write(response.read())
        #             with shared_data.mutex:
        #                 shared_data.cont_hash[index][1] = None
        #             return

    def do_PUT(self):
        if(self.path == '/write'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            content = json.loads(content)
            data = content["data"]
            shard_grp = {}
            for entry in data:
                stud_id = int(entry["Stud_id"])
                shard_id = metadata_obj.get_shard_id(stud_id)
                if shard_id is None:
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    server_response = {"message": "<Error> Shard not found", "status": "failure"}
                    response_str = json.dumps(server_response)
                    self.wfile.write(response_str.encode('utf-8'))
                    return
                shard_grp[shard_id].append(entry)

            for shard_id, entries_list in shard_grp.items():
                shard_obj = shard_id_object_mapping[shard_id]
                with shard_obj.update_mutex:
                    for server_id in shard_obj.serv_list.keys():
                        response = server_write('sh'+str(shard_id), shard_obj.serv_dict[server_id][1], entries_list, server_id)
                        shard_obj.serv_dict[server_id][1] = int(response["current_idx"])
                    metadata_obj.update_valid_idx(shard_id, int(response["current_idx"]))
                    for server_id in shard_obj.serv_list.keys():
                        response = server_updateid(server_id, shard_id, metadata_obj.get_valid_idx(shard_id))
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            server_response = {"message": f"{len(data)} entries added", "status": "success"} 
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return
    
    
    
    
    
    
        elif (self.path == '/update'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            content = json.loads(content)
            
            Stud_id = int(content["Stud_id"])
            data = content["data"]
            
            sid = int(data["Stud_id"])
            sname = data["Stud_name"]
            smarks = data["Stud_marks"]
            
            shard_id = metadata_obj.get_shard_id(Stud_id)
            if shard_id is None:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                server_response = {"message": "<Error> Shard not found", "status": "failure"}
                response_str = json.dumps(server_response)
                self.wfile.write(response_str.encode('utf-8'))
                return
            
            shard_obj = shard_id_object_mapping[shard_id]
            with shard_obj.update_mutex:
                metadata_obj.update_update_idx(shard_id, sid)
                for server_id in shard_obj.serv_list.keys():
                    response = server_update('sh'+str(shard_id), sid, sname, smarks, server_id)
                metadata_obj.update_valid_idx(shard_id, -1)
                    
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            
           
           
           
         
        
    def do_DELETE(self):
        if(self.path == '/rm'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            content = json.loads(content)
            num_servs = int(content["n"])
            server_list = content["servers"]
            if(num_servs<len(server_list)):
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                server_response = {"message": "<Error> Length of hostname list is more than newly added instances", "status": "failure"}
                response_str = json.dumps(server_response)
                self.wfile.write(response_str.encode('utf-8'))
                return
            cur = 0
            for server in server_list:
                server_id = int(server[6:])
                with servers_obj.mutex:
                    servers_obj.remove_server(server_id)
                cur += 1
            while cur < num_servs:
                len = len(servers_obj.server_to_docker_container_map)
                random_idx = random.randint(0, len-1)
                server_id = list(servers_obj.server_to_docker_container_map.keys())[random_idx]
                with servers_obj.mutex:
                    servers_obj.remove_server(server_id)
                cur += 1

        # if(self.path == '/rm'):
        #     content_length = int(self.headers['Content-Length'])
        #     content = self.rfile.read(content_length).decode('utf-8')
        #     content = json.loads(content)
        #     num_servs = int(content["n"])
        #     name_servs = content["hostnames"]
        #     if(len(name_servs) > num_servs):
        #         self.send_response(400)
        #         self.send_header('Content-type', 'application/json')
        #         self.end_headers()
        #         server_response = {"message": "<Error> Length of hostname list is more than newly added instances", "status": "failure"}
        #         response_str = json.dumps(server_response)
        #         self.wfile.write(response_str.encode('utf-8'))
        #         return
        #     with shared_data.mutex:
        #         for i in range(len(name_servs)):
        #             if(name_servs[i] not in shared_data.serv_dict):
        #                 self.send_response(400)
        #                 self.send_header('Content-type', 'application/json')
        #                 self.end_headers()
        #                 server_response = {"message": "<Error> Server not found", "status": "failure"}
        #                 response_str = json.dumps(server_response)
        #                 self.wfile.write(response_str.encode('utf-8'))
        #                 return
        #         for i in range(num_servs):
        #             if(i>=len(name_servs)):
        #                 first_key, first_value = next(iter(shared_data.serv_dict.items()))
        #                 shared_data.rm_server(first_key)
        #             else:
        #                 shared_data.rm_server(name_servs[i])
        #     self.send_response(200)
        #     self.send_header('Content-type', 'application/json')
        #     self.end_headers()
        #     server_response = {"message": {"N": shared_data.num_serv, "replicas": [hostname for hostname in shared_data.serv_dict.keys()]}, "status": "successful"}
        #     response_str = json.dumps(server_response)
        #     self.wfile.write(response_str.encode('utf-8'))
        #     return
        
        
        elif (self.path == '/del'):
            content_length = int(self.headers['Content-Length'])
            content = self.rfile.read(content_length).decode('utf-8')
            content = json.loads(content)
            Stud_id = int(content["Stud_id"])
            shard_id = metadata_obj.get_shard_id(Stud_id)
            if shard_id is None:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                server_response = {"message": "<Error> Shard not found", "status": "failure"}
                response_str = json.dumps(server_response)
                self.wfile.write(response_str.encode('utf-8'))
                return
            shard_obj = shard_id_object_mapping[shard_id]
            with shard_obj.update_mutex:
                metadata_obj.update_update_idx(shard_id, Stud_id)
                for server_id in shard_obj.serv_list.keys():
                    response = server_delete('sh'+str(shard_id), Stud_id, server_id)
                metadata_obj.update_valid_idx(shard_id, -1)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            server_response = {"message": "Entry deleted", "status": "success"}
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return
            

if __name__ == '__main__':
    server_address = ('', 5000)
    httpd = ThreadingHTTPServer(server_address, SimpleHandlerWithMutex)
    # n = int(os.environ.get('NUM_INIT_SERVER'))
    # with shared_data.mutex:
    #     for _ in range(n):
    #         shared_data.add_server()
        
    print('Starting server on port 5000...')
    try:
        heart_beat_thread = threading.Thread(target=thread_heartbeat)
        heart_beat_thread.start()
        httpd.serve_forever()
        heart_beat_thread.join()

    except KeyboardInterrupt:
        print('Server is shutting down...')
        httpd.shutdown()
        exit()
    
    

# async