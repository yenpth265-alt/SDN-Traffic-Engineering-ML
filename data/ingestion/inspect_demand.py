
import zipfile
import io
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
zip_path = ROOT / "network/traffic_schedules/demand_plan.zip"

with zipfile.ZipFile(zip_path, "r") as z:
    files = [f for f in z.infolist() if not f.is_dir()]

    print("Tổng số file:", len(files))
    print("\nDanh sách 10 file đầu tiên:")

    for f in files[:10]:
        print(f.filename, "|", f.file_size, "bytes")

    print("\nKiểm tra file CSV đầu tiên:")

    csv_files = [
        f for f in files
        if f.filename.lower().endswith(".csv")
    ]

    if csv_files:
        with z.open(csv_files[0]) as raw:
            reader = csv.DictReader(
                io.TextIOWrapper(raw, encoding="utf-8-sig")
            )

            print("Tên file:", csv_files[0].filename)
            print("Các cột:", reader.fieldnames)

            for i, row in enumerate(reader):
                if i >= 3:
                    break
                print(row)
    else:
        print("Không tìm thấy file CSV.")
