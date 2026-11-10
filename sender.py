
import socket
import time
import json
import sys

def send_traffic(flow_group_id, dst_ip, dst_port, warmup_seconds=70, measurement_seconds=360):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    log_filename = f"sender_{flow_group_id}.jsonl"
    print(f"[*] Bắt đầu tiến trình phát cho nhóm: {flow_group_id} tới {dst_ip}:{dst_port}")
    print(f"[*] Pha Warm-up: {warmup_seconds}s | Pha Measurement: {measurement_seconds}s")
    
    seq = 0
    total_duration = warmup_seconds + measurement_seconds
    
    with open(log_filename, "w", encoding="utf-8") as log_file:
        for current_second in range(total_duration):
            loop_start = time.time()
            is_warmup = current_second < warmup_seconds
            
            # Tốc độ gói tin mỗi giây (có thể điều chỉnh theo lịch nhu cầu)
            packets_per_second = 10  
            payload_size = 1024      
            
            for _ in range(packets_per_second):
                seq += 1
                send_timestamp = time.time()
                # Thiết lập mốc available_at cho chế độ online (tránh rò rỉ tương lai)
                available_at_timestamp = send_timestamp + 0.005 
                
                payload = json.dumps({
                    "seq": seq,
                    "flow_group_id": flow_group_id,
                    "send_time": send_timestamp
                }).encode('utf-8')
                
                try:
                    client_socket.sendto(payload, (dst_ip, dst_port))
                    
                    # Chỉ ghi log chi tiết vào file trong pha measurement để tối ưu dữ liệu
                    if not is_warmup:
                        log_entry = {
                            "run_id": "run_001",
                            "flow_group_id": flow_group_id,
                            "seq": seq,
                            "timestamp": send_timestamp,
                            "available_at": available_at_timestamp,
                            "payload_size_bytes": len(payload),
                            "status": "sent"
                        }
                        log_file.write(json.dumps(log_entry) + "\n")
                except Exception as e:
                    print(f"[!] Lỗi khi gửi gói {seq}: {e}")
            
            # Đảm bảo đúng nhịp 1 giây cho mỗi bước
            elapsed = time.time() - loop_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
                
    client_socket.close()
    print(f"[+] Hoàn thành phát luồng cho nhóm: {flow_group_id}. Log đã lưu tại {log_filename}")

if __name__ == "__main__":
    target_ip = sys.argv[1] if len(sys.argv) > 1 else "10.0.0.3"
    target_port = int(sys.argv[2]) if len(sys.argv) > 2 else 5001
    group_id = sys.argv[3] if len(sys.argv) > 3 else "0x105"
    
    # Chạy với 70s warm-up và 360s measurement như thiết kế chuẩn
    send_traffic(group_id, target_ip, target_port, warmup_seconds=70, measurement_seconds=360)