import asyncio
import datetime
import json
import os
import sys
import time
import urllib.request
import websockets

WS_URL = "wss://ws.sstock.vn/api/v1/ws"
APPS_SCRIPT_URL = os.environ.get("APPS_SCRIPT_URL", "").strip()
MIN_VOLUME = 400_000
FLUSH_INTERVAL = 3.0

HEADERS = {
    "Origin": "https://sstock.vn",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

VOLUME_FIELDS = {
    "Bid1Vol": ("MUA (Bid1)", "Bid1", "Kê lệnh lớn"),
    "Bid2Vol": ("MUA (Bid2)", "Bid2", "Kê lệnh lớn"),
    "Bid3Vol": ("MUA (Bid3)", "Bid3", "Kê lệnh lớn"),
    "Ask1Vol": ("BÁN (Ask1)", "Ask1", "Kê lệnh lớn"),
    "Ask2Vol": ("BÁN (Ask2)", "Ask2", "Kê lệnh lớn"),
    "Ask3Vol": ("BÁN (Ask3)", "Ask3", "Kê lệnh lớn"),
    "LastVol": ("KHỚP LỆNH", "LastPrice", "Khớp lệnh lớn"),
    "MatchVol": ("KHỚP LỆNH", "MatchPrice", "Khớp lệnh lớn"),
}

VN_TZ = datetime.timezone(datetime.timedelta(hours=7))


def get_vn_now() -> datetime.datetime:
    return datetime.datetime.now(VN_TZ)


def is_session_over() -> bool:
    now = get_vn_now()
    hm = now.hour * 60 + now.minute
    # Kết thúc phiên sáng sau 11:35 (695) hoặc phiên chiều sau 15:05 (905)
    if 695 <= hm < 775:
        return True
    if hm >= 905:
        return True
    return False


def post_to_apps_script(rows: list[list]) -> dict:
    data = json.dumps({"rows": rows}).encode("utf-8")
    req = urllib.request.Request(
        APPS_SCRIPT_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def extract_whale_rows(payload: dict) -> list[list]:
    if payload.get("dataType") != "stock":
        return []
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    symbol = payload.get("identifier") or data.get("Symbol", "")
    if not symbol or len(symbol) != 3:
        return []

    timestamp = data.get("TradingTime") or payload.get("timestamp", "")
    msg_id = payload.get("id", "")
    rows = []

    for vol_key, (side_label, price_key, event_type) in VOLUME_FIELDS.items():
        vol = data.get(vol_key)
        if isinstance(vol, (int, float)) and vol > MIN_VOLUME:
            price = data.get(price_key) or data.get("Price") or data.get("RefPrice") or 0
            if isinstance(price, (int, float)) and price > 0:
                price_vnd = price * 1000 if price < 1000 else price
                value_billion = round((vol * price_vnd) / 1_000_000_000, 2)
            else:
                value_billion = ""

            rows.append([
                str(timestamp),
                symbol,
                side_label,
                price if price else "N/A",
                int(vol),
                value_billion,
                event_type,
                str(msg_id),
            ])
    return rows


async def stream_and_push():
    if not APPS_SCRIPT_URL:
        print("[!] Thiếu biến môi trường APPS_SCRIPT_URL.")
        sys.exit(1)

    row_buffer = []
    last_flush = time.monotonic()
    seen_cache = set()

    while not is_session_over():
        try:
            print(f"[*] [{get_vn_now().strftime('%H:%M:%S')}] Đang kết nối tới {WS_URL}...")
            async with websockets.connect(
                WS_URL,
                additional_headers=HEADERS,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                print(f"[+] Đã thông luồng! Quét lệnh > {MIN_VOLUME:,} cp toàn thị trường...")
                while not is_session_over():
                    try:
                        raw_msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        payload = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue

                    matched_rows = extract_whale_rows(payload)
                    for row in matched_rows:
                        dedup_key = (row[1], row[2], row[4])
                        if dedup_key in seen_cache:
                            continue
                        seen_cache.add(dedup_key)
                        if len(seen_cache) > 5000:
                            seen_cache.clear()

                        row_buffer.append(row)
                        print(f"[WHALE] {row[0]} | {row[1]} | {row[2]} | KL: {row[4]:,} cp | {row[5]} tỷ")

                    now = time.monotonic()
                    if row_buffer and (now - last_flush >= FLUSH_INTERVAL):
                        batch = row_buffer[:]
                        row_buffer.clear()
                        last_flush = now
                        try:
                            res = await asyncio.to_thread(post_to_apps_script, batch)
                            if res.get("status") == "ok":
                                print(f"    -> Đã ghi {res.get('inserted')} dòng vào Google Sheet.")
                        except Exception as err:
                            print(f"[!] Lỗi gửi dữ liệu: {err}")
                            row_buffer.extend(batch)

        except (websockets.ConnectionClosed, OSError) as e:
            print(f"[!] Mất kết nối ({e}). Thử lại sau 3s...")
            await asyncio.sleep(3)

    if row_buffer:
        post_to_apps_script(row_buffer)
    print(f"[*] [{get_vn_now().strftime('%H:%M:%S')}] Hết phiên giao dịch. Dừng bot an toàn.")


if __name__ == "__main__":
    asyncio.run(stream_and_push())
