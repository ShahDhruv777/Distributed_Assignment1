from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import time

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(10)
        print("Server done sleeping")
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Hello, World!")

server_address = ('', 5000)
httpd = ThreadingHTTPServer(server_address, SimpleHandler)
httpd.serve_forever()
