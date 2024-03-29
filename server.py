# server.py
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

server_id = os.environ.get('ID')

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if(self.path == '/home'):
            print("Received home request\n")
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            server_response = {"text": "Hello from server " + server_id + "!"}
            response_str = json.dumps(server_response)
            self.wfile.write(response_str.encode('utf-8'))
            return
        elif(self.path == '/heartbeat'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            # server_response = {}
            # response_str = json.dumps(server_response)
            # self.wfile.write(response_str.encode('utf-8'))
            return
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'404 Not Found')
            return
        

def run(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler, port=5000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting server on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run()