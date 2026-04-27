$ErrorActionPreference = "Stop"

# 同じWi-Fi/LAN上のスマホや別PCからアクセスできるように、全ネットワークIFで待ち受けます。
# 起動後、スマホから http://<このPCのIPv4アドレス>:8020/login を開いてください。
python -m uvicorn app:app --host 0.0.0.0 --port 8020
