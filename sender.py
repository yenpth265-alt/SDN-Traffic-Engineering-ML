import csv
import json
import os
import socket
import struct
import sys
import time
from datetime import datetime, timezone

def load_manifest(manifest_path="runner_manifest.json"):
    """Đọc tệp manifest để lấy thông tin run_id, thời gian bắt đầu và các pha."""
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"Không tìm thấy file manifest tại: {manifest_path}")
    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_demand_plan(demand_csv_path, target_flow_group):
    """Đọc file lịch nhu cầu (demand_plan) cho flow_group_id cụ thể."""
    demands = {} # {time_offset_s: requested_mbps}
    if not os.path.exists(demand_csv_path):
        raise FileNotFoundError(f"Không tìm thấy file lịch tại: {demand_csv_path}")
    
    with open(demand_csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=';') # Hoặc đổi thành ',' tùy file CSV của bạn
        for row in reader:
            if row.get("flow_group_id") == target_flow_group:
                t = int(row["time_offset_s"])
                mbps = float(row["requested_mbps"])
                demands[t] = mbps
    return demands

def run_sender(manifest_path, flow_group_id, dst_ip, dst_port, demand_csv_path):
    # 1. Đọc manifest
    manifest = load_manifest(manifest_path)
    run_id = manifest.get("run_id", "run_001")
    run_start_utc_str = manifest.get("run_start_utc", datetime.now(timezone.utc).isoformat())
    
    # Chuyển đổi run_start_utc sang timestamp (seconds) để tính available_at chuẩn xác
    # Giả định định dạng ISO 8601
    run_start_dt = datetime.fromisoformat(run_start_utc_str.replace("Z", "+00:00"))
    run_start_timestamp = run_start_dt.timestamp()

    warmup_seconds = manifest.get("phases", {}).get("warmup_seconds", 70)
    measurement_seconds = manifest.get("phases", {}).get("measurement_seconds", 360)
    total_duration = warmup_seconds + measurement_seconds

    # 2. Đọc lịch nhu cầu
    demands = load_demand_plan(demand_csv_path, flow_group_id)

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    requested_log_filename = f"requested_log_{flow_group_id}.csv"
    sender_log_filename = f"sender_{flow_group_id}.jsonl"

    print(f"[*] Bắt đầu phát luồng {flow_group_id} tới {dst_ip}:{dst_port} (Run ID: {run_id})")
    print(f"[*] Warm-up: {warmup_seconds}s | Measurement: {measurement_seconds}s")

    seq = 0
    payload_size = 1000 # Kích thước gói cố định chuẩn (bytes)

    # Mở file ghi requested_log.csv
    with open(requested_log_filename, "w", encoding="utf-8", newline="") as req_file, \
         open(sender_log_filename, "w", encoding="utf-8") as sender_log_file:
        
        req_writer = csv.writer(req_file, delimiter=',')
        # Ghi header theo chuẩn yêu cầu
        req_writer.writerow(["time_offset_s", "flow_group_id", "requested_mbps", "available_at_utc"])

        # Vòng lặp theo từng giây của kịch bản
        for current_second in range(total_duration):
            loop_start = time.time()
            is_warmup = current_second < warmup_seconds

            # Lấy requested_mbps từ lịch, mặc định 0 nếu không có
            requested_mbps = demands.get(current_second, 0.0)

            # Tính toán số lượng gói cần gửi trong giây này
            # Công thức: packets = (mbps * 10^6) / (8 * payload_size)
            if requested_mbps > 0:
                total_packets_this_sec = int((requested_mbps * 1_000_000) / (8 * payload_size))
            else:
                total_packets_this_sec = 0

            # Tính available_at_utc cho bin giây này: run_start + (t + 1) giây
            available_at_epoch = run_start_timestamp + current_second + 1
            available_at_utc_str = datetime.fromtimestamp(available_at_epoch, timezone.utc).isoformat()

            # Ghi nhận vào requested_log.csv (bao gồm cả pha warm-up để ML đủ lịch sử)
            req_writer.writerow([current_second, flow_group_id, requested_mbps, available_at_utc_str])

            # Rải đều các gói tin trong khoảng 1 giây
            if total_packets_this_sec > 0:
                interval = 1.0 / total_packets_this_sec
                for i in range(total_packets_this_sec):
                    packet_start_time = time.time()
                    seq += 1
                    send_timestamp = time.time()

                    # Đóng gói nhị phân (struct) để tối ưu CPU thay vì dùng json.dumps toàn bộ
                    # Cấu trúc: sequence id (Q - unsigned long long), timestamp (d - double)
                    packet_header = struct.pack("Qd", seq, send_timestamp)
                    padding = b"X" * (payload_size - len(packet_header))
                    payload = packet_header + padding

                    try:
                        client_socket.sendto(payload, (dst_ip, dst_port))
                        
                        # Ghi log chi tiết gói tin phía sender (chỉ ghi trong pha measurement hoặc ghi toàn bộ tùy yêu cầu)
                        if not is_warmup:
                            log_entry = {
                                "run_id": run_id,
                                "flow_group_id": flow_group_id,
                                "seq": seq,
                                "timestamp": send_timestamp,
                                "payload_size_bytes": len(payload),
                                "status": "sent"
                            }
                            sender_log_file.write(json.dumps(log_entry) + "\n")

                    except Exception as e:
                        print(f"[!] Lỗi gửi gói {seq}: {e}")

                    # Kiểm soát nhịp rải gói trong giây
                    elapsed_pkt = time.time() - packet_start_time
                    if elapsed_pkt < interval:
                        time.sleep(interval - elapsed_pkt)
            else:
                # Nếu không có tải trong giây này, sleep đủ 1 giây
                time.sleep(1.0)

            # Đồng hồ hóa nhịp giây tổng thể
            elapsed_sec = time.time() - loop_start
            if elapsed_sec < 1.0:
                time.sleep(1.0 - elapsed_sec)

    client_socket.close()
    print(f"[+] Hoàn thành phát luồng {flow_group_id}. Đã xuất file {requested_log_filename} và {sender_log_filename}")

if __name__ == "__main__":
    # Tham số truyền vào: manifest_path, flow_group_id, dst_ip, dst_port, demand_csv_path
    m_path = sys.argv[1] if len(sys.argv) > 1 else "runner_manifest.json"
    g_id = sys.argv[2] if len(sys.argv) > 2 else "h1_h5"
    ip = sys.argv[3] if len(sys.argv) > 3 else "10.0.0.5"
    port = int(sys.argv[4]) if len(sys.argv) > 4 else 5005
    csv_path = sys.argv[5] if len(sys.argv) > 5 else "demand_plan.csv"

    run_sender(m_path, g_id, ip, port, csv_path)
