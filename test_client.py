import requests
import threading

def send_requests():
    for _ in range(1):
        # Send a GET request to the server
        response = requests.get('http://localhost:5000')
        print(response.text)

# Create multiple threads to simulate concurrent client requests
threads = []
for _ in range(5):
    print("Creating thread")
    thread = threading.Thread(target=send_requests)
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()
