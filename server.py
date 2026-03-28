import socket
import json

# Use '0.0.0.0' to listen to ALL network interfaces (WiFi, USB, etc.)
HOST = '0.0.0.0' 
PORT = 9000

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"Server is listening on port {PORT}...")
        print("Waiting for phone to connect...")

        while True:
            conn, addr = s.accept()
            with conn:
                print(f"Connected by {addr}")
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    
                    try:
                        # Decode the JSON packet sent by Flutter
                        message = data.decode('utf-8').strip()
                        decoded_data = json.loads(message)
                        print("\n--- New Data Received ---")
                        print(f"Phone: {decoded_data.get('phone')}")
                        print(f"Location: {decoded_data.get('location')}")
                        print(f"BLE Data: {decoded_data.get('ble_payload')}")
                    except Exception as e:
                        print(f"Raw Data: {data}")

if __name__ == "__main__":
    start_server()