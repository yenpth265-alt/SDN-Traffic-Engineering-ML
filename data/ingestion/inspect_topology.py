import json
from pathlib import Path

path = Path("configs/network/topology_bundle.json")

with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

print("Các trường cấp cao nhất:")
print(list(data.keys()))

for key, value in data.items():
    print(f"{key}: {type(value).__name__}")

    if isinstance(value, list) and value:
        print("  Phần tử đầu:", value[0])