import socket

def start_debug_server():
    # Listen on all IPs at port 8080
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 8080))
    server.listen(1)
    print("--- PC SERVER IS LIVE ---")
    print("Listening on 172.21.177.76:8080 (USB Tethering)")
    
    while True:
        print("\nWaiting for phone to connect...")
        client, addr = server.accept()
        print(f"!!! SUCCESS !!! Connected by {addr}")
        while True:
            data = client.recv(1024)
            if not data: break
            print(f"Received: {data.decode('utf-8').strip()}")
        client.close()

start_debug_server()