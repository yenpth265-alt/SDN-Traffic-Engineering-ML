
import socket
import json
import sys
import time

def receive_traffic(listen_ip="0.0.0.0", listen_port=5001, flow_group_id="0x105"):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((listen_ip, listen_port))
    
    log_filename = f"receiver_{flow_group_id}.jsonl"
    print(f"[*] Receiver đang lắng nghe tại {listen_ip}:{listen_port} cho nhóm {flow_group_id}...")
    
    with open(log_filename, "w", encoding="utf-8") as log_file:
        try:
            while True:
                data, addr = server_socket.recvfrom(1024)
                receive_timestamp = time.time()
                available_at_timestamp = receive_timestamp + 0.005
                
                try:
                    packet_info = json.loads(data.decode('utf-8'))
                    seq = packet_info.get("seq")
                except Exception:
                    seq = -1
                
                log_entry = {
                    "run_id": "run_001",
                    "flow_group_id": flow_group_id,
                    "seq": seq,
                    "receive_timestamp": receive_timestamp,
                    "available_at": available_at_timestamp,
                    "payload_size_bytes": len(data),
                    "sender_ip": addr[0]
                }
                log_file.write(json.dumps(log_entry) + "\n")
                log_file.flush()
                
        except KeyboardInterrupt:
            print(f"\n[+] Dừng lắng nghe. Log đã được lưu tại {log_filename}")
    server_socket.close()

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5001
    group_id = sys.argv[2] if len(sys.argv) > 2 else "0x105"
    receive_traffic("0.0.0.0", port, group_id)