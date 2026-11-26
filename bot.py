import asyncio
import datetime
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import websockets

BASE_HISTORY_URL = "https://api-feature.sstock.vn/api/v1/ta/orderbook-history"
WS_URL = "wss://ws.sstock.vn/api/v1/ws"

VN100_SYMBOLS = [
    "ACB", "ANV", "BCM", "BID", "BMP", "BSI", "BVH", "BWE", "CII", "CMG",
    "CTD", "CTG", "CTR", "CTS", "DBC", "DCM", "DGC", "DGW", "DIG", "DPM",
    "DSE", "DXG", "DXS", "EIB", "EVF", "FPT", "FRT", "FTS", "GAS", "GEE",
    "GEX", "GMD", "GVR", "HAG", "HCM", "HDB", "HDC", "HDG", "HHV", "HPG",
    "HSG", "HT1", "IMP", "KBC", "KDC", "KDH", "KOS", "LPB", "MBB", "MCH",
    "MBS", "MSB", "MSN", "MWG", "NAB", "NKG", "NLG", "NT2", "OCB", "PAN",
    "PC1", "PDR", "PHR", "PLX", "PNJ", "POW", "PPC", "PTB", "PVD", "PVT",
    "REE", "SAB", "SBT", "SCS", "SHB", "SIP", "SJS", "SSB", "SSI", "STB",
    "SZC", "TCB", "TCH", "TLG", "TPB", "VCB", "VCG", "VCI", "VGC", "VHC",
    "VHM", "VIB", "VIC", "VIX", "VJC", "VNM", "VPB", "VPI", "VPL", "VRE",
    "VSC", "VTP",
]

APPS_SCRIPT_URL = os.environ.get("APPS_SCRIPT_URL", "").strip()
SSTOCK_COOKIE = os.environ.get("SSTOCK_COOKIE", "").strip()

MIN_VOLUME = 400_000
FLUSH_INTERVAL = 3.0
CONCURRENCY = 8

VN_TZ = datetime.timezone(datetime.timedelta(hours=7))


def get_vn_now() -> datetime.datetime:
    return datetime.datetime.now(VN_TZ)


def is_session_over() -> bool:
    now = get_vn_now()
    hm = now.hour * 60 + now.minute
    if 695 <= hm < 775:
        return True
    if hm >= 905:
        return True
    return False


def build_headers() -> dict:
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://sstock.vn",
        "referer": "https://sstock.vn/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36",
    }
    if SSTOCK_COOKIE:
        headers["cookie"] = SSTOCK_COOKIE
    return headers


def fetch_json(url: str, timeout: int = 10):
    req = urllib.request.Request(url, headers=build_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


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


def parse_single_orders(symbol: str, history_items: list) -> list[list]:
    rows = []
    if not isinstance(history_items, list):
        return rows

    for item in history_items:
        if not isinstance(item, dict):
            continue

        last_vol = item.get("lastVol") or 0
        pt_vol = item.get("pTVol") or 0
        vol = last_vol if isinstance(last_vol, (int, float)) and last_vol > MIN_VOLUME else (
            pt_vol if isinstance(pt_vol, (int, float)) and pt_vol > MIN_VOLUME else 0
        )

        if vol <= MIN_VOLUME:
            continue

        price = item.get("lastPrice") or item.get("price") or 0
        side_raw = str(item.get("side") or "").upper()
        if side_raw in ("B", "BUY", "MUA"):
            side_label = "CHỦ ĐỘNG MUA (B)"
        elif side_raw in ("S", "SELL", "BÁN", "BAN"):
            side_label = "CHỦ ĐỘNG BÁN (S)"
        else:
            side_label = side_raw or "KHÔNG RÕ"

        t_time = item.get("tradingTime") or item.get("time") or item.get("timestamp") or get_vn_now().strftime("%Y-%m-%d %H:%M:%S")
        order_type = "Thỏa thuận lớn" if (pt_vol and pt_vol > MIN_VOLUME and (not last_vol or last_vol <= MIN_VOLUME)) else "Lệnh khớp đơn lẻ"

        if isinstance(price, (int, float)) and price > 0:
            price_vnd = price * 1000 if price < 1000 else price
            value_billion = round((vol * price_vnd) / 1_000_000_000, 2)
        else:
            value_billion = ""

        rows.append([
            str(t_time),
            symbol,
            side_label,
            price if price else "N/A",
            int(vol),
            value_billion,
            order_type,
            f"{symbol}|{t_time}|{price}|{int(vol)}|{side_raw}",
        ])

    return rows


async def scan_one_symbol(symbol: str, sem: asyncio.Semaphore) -> list[list]:
    url = f"{BASE_HISTORY_URL}?symbol={urllib.parse.quote(symbol)}"
    async with sem:
        try:
            data = await asyncio.to_thread(fetch_json, url, 10)
            items = data if isinstance(data, list) else data.get("data") or data.get("items") or []
            return parse_single_orders(symbol, items)
        except Exception:
            return []


async def main():
    if not APPS_SCRIPT_URL:
        print("[!] Thiếu biến môi trường APPS_SCRIPT_URL.")
        sys.exit(1)

    symbols = sorted(set(VN100_SYMBOLS))
    seen_orders = set()
    row_buffer = []
    last_flush = time.monotonic()
    sem = asyncio.Semaphore(CONCURRENCY)

    print(f"[+] Bắt đầu quét TỪNG LỆNH ĐƠN LẺ > {MIN_VOLUME:,} cp cho {len(symbols)} mã VN100...")

    while not is_session_over():
        loop_start = time.monotonic()
        tasks = [scan_one_symbol(sym, sem) for sym in symbols]
        results = await asyncio.gather(*tasks)

        for sym_rows in results:
            for row in sym_rows:
                uid = row[7]
                if uid in seen_orders:
                    continue
                seen_orders.add(uid)
                if len(seen_orders) > 50000:
                    seen_orders.clear()

                row_buffer.append(row)
                print(f"[SINGLE WHALE] {row[0]} | {row[1]} | {row[2]} | Giá: {row[3]} | KL: {row[4]:,} cp | {row[5]} tỷ")

        now = time.monotonic()
        if row_buffer and (now - last_flush >= FLUSH_INTERVAL):
            batch = row_buffer[:]
            row_buffer.clear()
            last_flush = now
            try:
                res = await asyncio.to_thread(post_to_apps_script, batch)
                if res.get("status") == "ok":
                    print(f"    -> Đã ghi {res.get('inserted')} lệnh đơn lẻ vào Google Sheet.")
                else:
                    print(f"[!] Apps Script lỗi: {res}")
            except Exception as err:
                print(f"[!] Lỗi gửi Sheet: {err}")
                row_buffer.extend(batch)

        elapsed = time.monotonic() - loop_start
        if elapsed < 2.0:
            await asyncio.sleep(2.0 - elapsed)

    if row_buffer:
        post_to_apps_script(row_buffer)
    print(f"[*] [{get_vn_now().strftime('%H:%M:%S')}] Hết phiên. Dừng bot.")


if __name__ == "__main__":
    asyncio.run(main())
