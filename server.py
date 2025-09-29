import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import mysql.connector
import copy
import asyncio
from aiohttp import web

server_id = os.environ.get('ID')

connection = mysql.connector.connect(
    host="localhost",
    user="myuser",
    password="mypass",
    database="Student_info"
)

cursor = connection.cursor()

update_idx_dict = {}

        
class Get_handler:
    async def heartbeat_handler(self, request):
        print("Received heartbeat request\n")
        return web.json_response({}, status=200)
    
    async def copy_handler(self, request):
        print("Received copy request\n")
        
        payload = await request.json()
        
        shards = payload.get('shards')


        response_data = {}

        for shard in shards:
            query = f"SELECT * FROM {shard} LIMIT {update_idx_dict[shard]};"
            cursor.execute(query)
            rows = cursor.fetchall()
            res = ""

            out_list = []
            res = {}
            for row in rows:
                res["Stud_id"] = str(row[0])
                res["Stud_name"] = row[1]
                res["Stud_marks"] = row[2]

                out_list.append(copy.deepcopy(res))
                res = {}

            response_data[shard] = out_list

        response_data["status"] = "success"
        print(response_data)
        return web.json_response(response_data, status=200)




    
class Post_handler:
    async def config_handler(self, request):
        print("Received config request\n")
        payload=await request.json()

        schema = payload.get('schema')
        shards = payload.get('shards')

        tables_created = ""
        dict={}
        dict['Number'] = 'INT'
        dict['String'] = 'VARCHAR(255)'
        for shard in shards:
            update_idx_dict[shard] = 0
            table_name = shard
            columns = ', '.join([f"{col} {dict[dtype]}" for col, dtype in zip(schema['columns'], schema['dtypes'])])
            # print(columns)
            create_table_query = f"CREATE TABLE {table_name} ({columns});"

            # print(create_table_query)
            cursor.execute(create_table_query)
            connection.commit()
            tables_created += (f"{server_id}:{table_name}, ")
        tables_created =  tables_created[:-2]
        tables_created += (" configured")

        response_data = {'message': tables_created, 'status': 'success'}
        return web.json_response(response_data, status=200)
    
    async def read_handler(self, request):
        
        payload = await request.json()

        shard = payload.get('shard')
        stud_id_range = payload.get('Stud_id')


        low = int(stud_id_range.get('low'))
        high = int(stud_id_range.get('high'))


        response_data = {}

        query = f"SELECT * FROM (SELECT * FROM {shard} LIMIT {update_idx_dict[shard]}) AS subquery WHERE Stud_id BETWEEN {low} AND {high};"
        cursor.execute(query)
        rows = cursor.fetchall()


        out_list = []
        res = {}
        for row in rows:
            res["Stud_id"] = str(row[0])
            res["Stud_name"] = row[1]
            res["Stud_marks"] = row[2]

            out_list.append(copy.deepcopy(res))
            res = {}

        response_data["data"] = out_list

        response_data["status"] = "success"
        return web.json_response(response_data, status=200)
    
    async def write_handler(self, request):        
        print("Received write request\n")
        payload = await request.json()

        shard = payload.get('shard')
        curr_idx = int(payload.get('curr_idx'))
        studs_data = payload.get('data')
        
        
        for stud in studs_data:
            print(stud)
            sid = int(stud.get('Stud_id'))
            sname = '\"' + stud.get('Stud_name') + '\"'
            smarks = '\"' + stud.get('Stud_marks') + '\"'
            
            check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
            cursor.execute(check_query)
            check = cursor.fetchone()[0]
            
            if(check > 0):
                update_query = f"UPDATE {shard} SET Stud_id = {sid}, Stud_name = {sname}, Stud_marks = {smarks} WHERE Stud_id = {sid};"
                cursor.execute(update_query)    
            else:
                write_query = f"INSERT INTO {shard}(Stud_id, Stud_name, Stud_marks) VALUES ({sid}, {sname}, {smarks});"
                cursor.execute(write_query)
                curr_idx = curr_idx + 1
        connection.commit()
        response_json = {
            "message": 'Data entries added',
            "current_idx": curr_idx,
            "status": "success"
        }
        return web.json_response(response_json, status=200)
        
        
    async def updateid_handler(self, request):
        print("Received update index request\n")
        payload = await request.json()  
        shard = payload.get('shard')
        update_idx_dict[shard] = int(payload.get('update_idx'))
        response_json = {
            "message": 'Update index updated',
            "status": "success"
        }
        return web.json_response(response_json, status=200)           
    


class Put_handler:
    async def update_handler(self, request):
        print("Received update request\n")
        payload = await request.json()
        
        shard = payload.get('shard')
        stud_id = int(payload.get('Stud_id'))
        data = payload.get('data')
        sid = int(data.get('Stud_id'))
        sname ='\"'+ data.get('Stud_name')+'\"'
        smarks = '\"'+data.get('Stud_marks')+'\"'
            
        
        check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
        cursor.execute(check_query)
        check = cursor.fetchone()[0]
        
        if check <= 0:
            return web.json_response({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}, status=400)
        
        update_query = f"UPDATE {shard} SET Stud_id = {sid}, Stud_name = {sname}, Stud_marks = {smarks} WHERE Stud_id = {stud_id};"
        cursor.execute(update_query)
        connection.commit()
        response_json = {
            "message": f'Data entry for Stud_id:{stud_id} updated',
            "status": "success"
        }
        return web.json_response(response_json, status=200)
        
        

class Delete_handler:
    async def del_handler(self, request):
        print("Received delete request\n")
        payload = await request.json()
        shard = payload.get('shard')
        sid = int(payload.get('Stud_id'))
        
        check_query = f"SELECT COUNT(*) FROM {shard} WHERE Stud_id = {sid};"
        cursor.execute(check_query)
        check = cursor.fetchone()[0]
        
        if check is None:
            return web.json_response({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}, status=400)
        
        if check <= 0:
            return web.json_response({"error": f"Entry with Stud_id:{sid} does not exist in the given shard"}, status=400)
        
        delete_query = f"DELETE FROM {shard} WHERE Stud_id = {sid};"
        cursor.execute(delete_query)
        connection.commit()
        update_idx_dict[shard] = update_idx_dict[shard] - 1
        response_json = {
            "message": f'Data entry with Stud_id:{sid} removed',
            "status": "success"
        }
        return web.json_response(response_json, status=200)
        
            
        


handle_get = Get_handler()
handle_post = Post_handler()
handle_put = Put_handler()
handle_delete = Delete_handler()

app = web.Application()
app.router.add_post('/config', handle_post.config_handler)
app.router.add_post('/read', handle_post.read_handler)
app.router.add_post('/write', handle_post.write_handler)
app.router.add_post('/updateid', handle_post.updateid_handler)

app.router.add_get('/heartbeat', handle_get.heartbeat_handler)
app.router.add_get('/copy', handle_get.copy_handler)

app.router.add_put('/update', handle_put.update_handler)

app.router.add_delete('/del', handle_delete.del_handler)
            

if __name__ == '__main__':
    print('Starting server on port 5000...')
    web.run_app(app, port=5000)