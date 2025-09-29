from packages import *
from helper_functions import *


connection = mysql.connector.connect(
    host="localhost",
    user="myuser",
    password="mypass",
    database="Metadata"
)

cursor = connection.cursor()






class Metadata:
    def __init__(self):
#         ShardT (Stud id low: Number, Shard id: Number, Shard size:Number, valid idx:Number)
#        MapT (Shard id: Number, Server id: Number)
        
        # for shard in shards:
        table_name = "ShardT"
        create_table_query = f"CREATE TABLE {table_name} ( Stud_id_low INT, Shard_id INT, Shard_size INT, Valid_idx INT, Update_idx INT);"
        print(create_table_query)
        cursor.execute(create_table_query)

        table_name = "MapT"
        create_table_query = f"CREATE TABLE {table_name} ( Shard_id INT, Server_id INT);"
        print(create_table_query)
        cursor.execute(create_table_query)
        connection.commit()

    def add_shard(self, shard_id, shard_size,shard_id_low):
        insert_query = f"INSERT INTO ShardT (Stud_id_low, Shard_id, Shard_size, Valid_idx, Update_idx) VALUES ({shard_id_low}, {shard_id}, {shard_size}, 0, 0);"
        cursor.execute(insert_query)
        connection.commit()    
    
    def add_server(self, server_id, shard_list):
        for shard in shard_list:
            insert_query = f"INSERT INTO MapT (Shard_id, Server_id) VALUES ({shard}, {server_id});"
            cursor.execute(insert_query)
        connection.commit()

    def remove_server(self, server_id):
        delete_query = f"DELETE FROM MapT WHERE Server_id = {server_id};"
        cursor.execute(delete_query)
        connection.commit()

    def get_shards(self, server_id):
        select_query = f"SELECT Shard_id FROM MapT WHERE Server_id = {server_id};"
        cursor.execute(select_query)
        shard_list = cursor.fetchall()
        return shard_list

    def get_all_shards(self):
        select_query = f"SELECT Stud_id_low, Shard_id, Shard_size FROM ShardT;"
        cursor.execute(select_query)
        shard_list = cursor.fetchall()
        return shard_list
    
    def get_shard_id(self, stud_id):
        # also check if stud_id is within the range of the shard
        select_query = f"SELECT Shard_id FROM ShardT WHERE Stud_id_low <= {stud_id} AND Stud_id_low + Shard_size > {stud_id};"
        cursor.execute(select_query)
        shard_id = cursor.fetchall()
        if len(shard_id) == 0:
            return None
        return shard_id[0][0]
    
    def get_server_id(self, shard_id):
        select_query = f"SELECT Server_id FROM MapT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        server_ids = cursor.fetchall()
        return server_ids
    
    def get_valid_idx(self, shard_id):
        select_query = f"SELECT Valid_idx FROM ShardT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        valid_idx = cursor.fetchall()
        return valid_idx[0][0]
    
    def get_update_idx(self, shard_id):
        select_query = f"SELECT Update_idx FROM ShardT WHERE Shard_id = {shard_id};"
        cursor.execute(select_query)
        update_idx = cursor.fetchall()
        return update_idx[0][0]
    
    def update_valid_idx(self, shard_id, valid_idx):
        update_query = f"UPDATE ShardT SET Valid_idx = {valid_idx} WHERE Shard_id = {shard_id};"
        cursor.execute(update_query)
        connection.commit()

    def update_update_idx(self, shard_id, update_idx):
        update_query = f"UPDATE ShardT SET Update_idx = {update_idx} WHERE Shard_id = {shard_id};"
        cursor.execute(update_query)
        connection.commit()







     




class Shards:
    def __init__(self):
        self.num_serv = 0
        self.counter = 0
        self.serv_dict = {}
        self.buf_size = 512
        self.num_vservs = int(math.log2(self.buf_size))
        self.cont_hash = [[None, None] for _ in range(self.buf_size)]
        self.serv_id_dict = SortedDict()
        self.update_mutex = threading.Lock()
        self.mutex = threading.Lock()


    def get_hash(self,host_name):
        li=[]
        for j in range(self.num_vservs):
            prime_multiplier = 37
            magic_number = 0x5F3759DF
            constant_addition = 11

            nindex = ((self.counter*self.counter + j*j + 2 * j + 25) * prime_multiplier) ^ magic_number
            nindex = (nindex + constant_addition) % self.buf_sizeresponse["current_idx"]
            nindex = (nindex ^ (nindex & (nindex ^ (nindex - 1)))) % self.buf_size  # Bitwise AND operation
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
        # print(li)
        return li

    def client_hash(self,r_id):
        prime_multiplier = 31
        magic_number = 0x5F3759DF
        constant_addition = 7

        nindex = ((r_id + 2 * r_id + 17) * prime_multiplier) ^ magic_number
        nindex = (nindex + constant_addition) % self.buf_size
        nindex = (nindex ^ (nindex & (nindex ^ (nindex - 1)))) % self.buf_size  # Bitwise AND operation
        nindex+=self.buf_size
        nindex%=self.buf_size
        jp=0
        while(jp<self.buf_size):
            if self.cont_hash[nindex][1]!=None:
                nindex+=1
                nindex%=self.buf_size
            else:
                self.cont_hash[nindex][1]=r_id
                break
            jp+=1
        nindex += 1
        nindex %= self.buf_size
        # print(self.serv_id_dict)
        if(jp!=512 and len(self.serv_id_dict) != 0):
            lower_bound_key = self.serv_id_dict.bisect_left(nindex)
            if lower_bound_key == len(self.serv_id_dict):
                return self.cont_hash[self.serv_id_dict.iloc[0]][0], ((nindex-1)+self.buf_size)%self.buf_size
            else:
                return self.cont_hash[self.serv_id_dict.iloc[lower_bound_key]][0], ((nindex-1)+self.buf_size)%self.buf_size
        else:
            return None
        

    def rm_server(self,host_name):
        indexes=self.serv_dict[host_name][0]
        for ind in indexes:
            self.cont_hash[ind][0]=None
            del self.serv_id_dict[ind]
        self.num_serv-=1
        # container = self.serv_dict[host_name][1]
        del self.serv_dict[host_name]
        # time.sleep(5)
        # try:
        #     container.stop()
        #     container.remove()
        # except:
        #     print("No such container found!!")

    def add_server(self,serv_id):
        self.num_serv += 1
        self.counter += 1
        
        # if self.buf_size - self.num_vservs*self.num_serv <self.num_vservs:
        #     self.counter-=1
        #     self.num_serv-=1
        #     print("ERROR!! Buffer size exceeded")
        #     return 0
        serv_listid = self.get_hash(serv_id)
        self.serv_dict[serv_id] = [serv_listid,0]
        return








class Servers: 
    global metadata_obj
    def __init__(self):
        self.mutex = threading.Lock()
    server_to_docker_container_map = {}        

    def add_server(self, server_id, shard_list):
        server_name="server"+str(server_id)
        # for shard in shard_list:
        #     shard_id_object_mapping[shard].add_server(server_id)
        metadata_obj.add_server(server_id, shard_list)
        environment_vars = {'ID': server_id}
        container = client.containers.run("server_image", detach=True, hostname = server_name, name = server_name, network ="my_network", environment=environment_vars)
        self.server_to_docker_container_map[server_id] = container
        configure_and_setup(server_id, shard_list)
        return
    
    def remove_server(self, server_id):
        shard_list = metadata_obj.get_shards(server_id)
        for shard in shard_list:
            shard_id_object_mapping[shard].rm_server(server_id)
        metadata_obj.remove_server(server_id)
        time.sleep(5)
        try:
            container = self.server_to_docker_container_map[server_id]
            container.stop()
            container.remove()
        except:
            print("No such container found!!")
        self.server_to_docker_container_map.pop(server_id)
        return shard_list
