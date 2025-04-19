import socket
import time
import json
import os
import pandas as pd

def handle_date(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)

def tail_jsonl(file_path):
    """Generator that yields new JSON lines as they are appended to the file."""
    with open(file_path, 'r') as f:
        f.seek(0, os.SEEK_END)  # Start at end of file
        while True:
            line = f.readline()
            if not line:
                time.sleep(1)
                continue
            try:
                yield json.loads(line.strip())
            except json.JSONDecodeError:
                print(f"[WARN] Skipping invalid JSON: {line.strip()}")

def send_data_over_socket(file_path, host='0.0.0.0', port=9999):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print(f"[Socket] Waiting for Spark to connect on {host}:{port}...")

    while True:
        conn, addr = s.accept()
        print(f"[Socket] Connected to {addr}")
        try:
            for record in tail_jsonl(file_path):
                try:
                    serialized = json.dumps(record, default=handle_date).encode('utf-8')
                    conn.send(serialized + b'\n')
                    print(f"[Socket] Sent: {serialized.decode('utf-8')}")
                    time.sleep(1)  # Optional: control streaming speed
                except (BrokenPipeError, ConnectionResetError):
                    print("[Socket] Connection lost while sending. Waiting for reconnect...")
                    break
        finally:
            conn.close()
            print("[Socket] Connection closed.")

if __name__ == "__main__":
    send_data_over_socket("datasets/reviews.jsonl")
