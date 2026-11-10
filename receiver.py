import json
import os
import socket
import struct
import sys
import time
from datetime import datetime, timezone

def load_manifest(manifest_path="runner_manifest.json"):
    """Đọc tệp manifest để lấy thông tin run_id và tổng thời gian chạy."""
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"Không tìm thấy file manifest tại: {manifest_path}")
    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)

def run_receiver(manifest_path, bind_ip, bind_port, flow_group_id):
    # 1. Đọc manifest để tính tổng thời gian chạy (warmup + measurement + xả pha phụ trội nếu có)
    manifest = load_manifest(manifest_path)
    run_id = manifest.get("run_id", "run_001")
    
    phases = manifest.get("phases", {})
    warmup_seconds = phases.get("warmup_seconds", 70)
    measurement_seconds = phases.get("measurement_seconds", 360)
    drain_seconds = phases.get("drain_seconds", 10) # Thời gian xả pha sau đo đạc
    
    total_duration = warmup_seconds + measurement_seconds + drain_seconds

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((bind_ip, bind_port))
    server_socket.settimeout(1.0) # Timeout 1 giây để kiểm tra điều kiện thoát vòng lặp thời gian

    receiver_log_filename = f"receiver_{flow_group_id}.jsonl"

    print(f"[*] Bắt đầu lắng nghe traffic cho nhóm: {flow_group_id} tại {bind_ip}:{bind_port} (Run ID: {run_id})")
    print(f"[*] Thời gian chạy dự kiến: {total_duration} giây")

    start_time = time.time()
    
    with open(receiver_log_filename, "w", encoding="utf-8") as log_file:
        while time.time() - start_time < total_duration:
            try:
                data, addr = server_socket.recvfrom(2048)
                recv_timestamp = time.time()
                
                # Giải mã gói tin nhị phân theo chuẩn struct đã gửi (Q: sequence id, d: send_timestamp)
                header_size = struct.calcsize("Qd")
                if len(data) >= header_size:
                    seq, send_timestamp = struct.unpack("Qd", data[:header_size])
                    
                    # Tính độ trễ một chiều (latency) tính bằng mili-giây (ms)
                    latency_ms = (recv_timestamp - send_timestamp) * 1000.0
                    
                    # Ghi log chi tiết thô phía nhận
                    log_entry = {
                        "run_id": run_id,
                        "flow_group_id": flow_group_id,
                        "seq": seq,
                        "sender_ip": addr[0],
                        "send_time": send_timestamp,
                        "receive_time": recv_timestamp,
                        "latency_ms": round(latency_ms, 3),
                        "received_bytes": len(data),
                        "status": "received"
                    }
                    
                    log_file.write(json.dumps(log_entry) + "\n")
                else:
                    print(f"[!] Nhận được gói tin quá ngắn từ {addr[0]}: {len(data)} bytes")

            except socket.timeout:
                # Hết timeout 1 giây mà không có gói tin, tiếp tục kiểm tra thời gian thoát
                continue
            except Exception as e:
                print(f"[!] Lỗi khi nhận gói tin: {e}")

    server_socket.close()
    print(f"[+] Hoàn thành thu thập luồng {flow_group_id}. Log nhận đã lưu tại {receiver_log_filename}")

if __name__ == "__main__":
    # Tham số truyền vào: manifest_path, bind_ip, bind_port, flow_group_id
    m_path = sys.argv[1] if len(sys.argv) > 1 else "runner_manifest.json"
    ip = sys.argv[2] if len(sys.argv) > 2 else "0.0.0.0"
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 5005
    group_id = sys.argv[4] if len(sys.argv) > 4 else "h1_h5"

    run_receiver(m_path, ip, port, group_id)
