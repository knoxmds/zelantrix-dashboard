import os, time, json
from flask import Flask, jsonify, render_template
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

app = Flask(__name__)

SERVICE_ACCOUNT_FILE = os.path.expanduser(
    "~/Documents/zelantrix/zelantrix-322523c37c00.json"
)
# On cloud: set SERVICE_ACCOUNT_JSON env var with the full JSON string
SERVICE_ACCOUNT_JSON_ENV = os.environ.get("SERVICE_ACCOUNT_JSON")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

SHEETS = [
    {"id": "1LcGz0Vzlwc87_SYL1iLwmMiCcO7UKlbh5Kd6OVeAIj8", "name": "amogh"},
    {"id": "1vWrSvBj7W5A2eF317Iw15FoJ74y7ezcT72Nb1eGjSUQ",  "name": "dvk"},
    {"id": "1cwj2lO7cnrKIdNi9cKnkb-R74NkX3Ck8rnfUmgizGus",  "name": "athyul"},
    {"id": "1MQpJSo7DAkQgqGCtWVu0EocifjwuOUEpyWULsHlQ65o",  "name": "arunansh"},
    {"id": "1wmWxhSBxpGXYXSPp1MiAhZ0w5Vhgd_SiF6fWnAu34RI",  "name": "vishwas"},
]

NUMERIC_COLS = [
    "FACTORY #1","FACTORY #2","FACTORY #3","FACTORY #4","FACTORY #5",
    "HOURS - FACTORY 1","HOURS - FACTORY 2","HOURS - FACTORY 3","HOURS - FACTORY 4",
    "OPERATION WORKERS","TOTAL PAYMENT",
]
HOURS_COLS = ["HOURS - FACTORY 1","HOURS - FACTORY 2","HOURS - FACTORY 3","HOURS - FACTORY 4"]

# ── simple in-process cache ───────────────────────────────────────────────────
_cache = {"data": None, "ts": 0}
CACHE_TTL = 90  # seconds

def _client():
    if SERVICE_ACCOUNT_JSON_ENV:
        info = json.loads(SERVICE_ACCOUNT_JSON_ENV)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return gspread.authorize(creds)

def _to_num(v):
    try: return float(str(v).replace(",","").replace("₹","").strip())
    except: return 0.0

def _fetch() -> list[dict]:
    """Fetch every worksheet from all configured spreadsheets and return rows."""
    gc = _client()
    rows = []
    for cfg in SHEETS:
        try:
            sh = gc.open_by_key(cfg["id"])
        except Exception as e:
            print(f"[WARN] Could not open {cfg['name']}: {e}")
            continue
        for ws in sh.worksheets():
            title = ws.title
            if title == "AccessControl":
                continue
            values = ws.get_all_values()
            if len(values) < 2:
                continue
            headers = values[0]
            is_factory = any(k in title.lower() for k in ("factory","factories"))
            for raw in values[1:]:
                row = {"_team": cfg["name"], "_tab": title, "_factory_tab": is_factory}
                for i, h in enumerate(headers):
                    if not h: continue
                    row[h.strip()] = raw[i] if i < len(raw) else ""
                # skip blank rows
                if not any(str(v).strip() for k,v in row.items() if not k.startswith("_")):
                    continue
                # normalise standard numeric cols
                for col in NUMERIC_COLS:
                    if col in row:
                        row[col] = _to_num(row[col])

                # ── Yuvraj-style format: factory #N + factory #N SD+ avg hrs ──
                # Sum hours only for factories that have a non-empty name
                alt_hours = 0.0
                for n in range(1, 10):
                    name_key = f"factory #{n}"
                    hrs_key  = f"factory #{n} SD+ avg hrs"
                    if name_key in row and hrs_key in row:
                        if str(row.get(name_key, "")).strip():  # factory name present
                            alt_hours += _to_num(row[hrs_key])

                # Payment: prefer clean "Total" column, fall back to "total payment" (may be text)
                alt_payment = 0.0
                if "Total" in row and _to_num(row["Total"]) > 0:
                    alt_payment = _to_num(row["Total"])
                elif "total payment" in row:
                    # Extract first number from text like "26100 + 29700"
                    import re
                    nums = re.findall(r'\d[\d,]*', str(row["total payment"]))
                    alt_payment = sum(_to_num(n) for n in nums)

                # Add reimbursement if present
                if "reimbursement" in row:
                    alt_payment += _to_num(row["reimbursement"])

                # Status: map "payment status" → STATUS
                if "payment status" in row and "STATUS" not in row:
                    row["STATUS"] = str(row["payment status"]).strip()

                # Apply alt values only if standard columns gave zero
                row["TOTAL HOURS"]   = alt_hours if alt_hours > 0 else sum(
                    row.get(c, 0) for c in HOURS_COLS if isinstance(row.get(c), (int, float)))
                row["TOTAL PAYMENT"] = row.get("TOTAL PAYMENT") or alt_payment

                # normalise date
                if "DATE" in row:
                    row["DATE"] = str(row["DATE"]).strip()
                rows.append(row)
    return rows

def get_data() -> list[dict]:
    now = time.time()
    if _cache["data"] is None or now - _cache["ts"] > CACHE_TTL:
        _cache["data"] = _fetch()
        _cache["ts"] = now
    return _cache["data"]

# ── routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/api/summary")
def api_summary():
    rows = get_data()
    if not rows:
        return jsonify({"error": "no data"})

    total_hours = sum(r.get("TOTAL HOURS", 0) for r in rows)
    total_pay   = sum(r.get("TOTAL PAYMENT", 0) for r in rows)

    # paid vs unpaid based on STATUS column
    paid = unpaid = 0.0
    for r in rows:
        st = str(r.get("STATUS","")).lower()
        p  = r.get("TOTAL PAYMENT", 0)
        if "paid" in st and "unpaid" not in st:
            paid += p
        else:
            unpaid += p

    # per-team totals
    teams = {}
    for r in rows:
        t = r["_team"]
        if t not in teams:
            teams[t] = {"name": t, "hours": 0, "payment": 0, "rows": 0, "factory_rows": 0}
        teams[t]["hours"]   += r.get("TOTAL HOURS", 0)
        teams[t]["payment"] += r.get("TOTAL PAYMENT", 0)
        teams[t]["rows"]    += 1
        if r["_factory_tab"]:
            teams[t]["factory_rows"] += 1

    team_list = sorted(teams.values(), key=lambda x: x["hours"], reverse=True)
    for t in team_list:
        t["hours"]   = round(t["hours"], 1)
        t["payment"] = round(t["payment"], 2)

    # daily trend (last 30 non-empty dates)
    from collections import defaultdict
    daily = defaultdict(lambda: {"hours": 0, "payment": 0})
    for r in rows:
        d = r.get("DATE","")
        if d:
            daily[d]["hours"]   += r.get("TOTAL HOURS", 0)
            daily[d]["payment"] += r.get("TOTAL PAYMENT", 0)
    trend = sorted([{"date": k, "hours": round(v["hours"],1), "payment": round(v["payment"],2)}
                    for k,v in daily.items() if k], key=lambda x: x["date"])[-30:]

    # factory tabs summary
    factory_hours = sum(r.get("TOTAL HOURS",0) for r in rows if r["_factory_tab"])
    factory_pay   = sum(r.get("TOTAL PAYMENT",0) for r in rows if r["_factory_tab"])

    return jsonify({
        "total_hours":    round(total_hours, 1),
        "total_payment":  round(total_pay, 2),
        "paid":           round(paid, 2),
        "unpaid":         round(unpaid, 2),
        "total_rows":     len(rows),
        "team_count":     len(teams),
        "factory_hours":  round(factory_hours, 1),
        "factory_payment":round(factory_pay, 2),
        "teams":          team_list,
        "trend":          trend,
        "synced_at":      time.strftime("%d %b %Y, %I:%M %p"),
    })

@app.route("/api/rows")
def api_rows():
    from flask import request
    rows = get_data()
    team   = request.args.get("team", "")
    factory= request.args.get("factory", "")
    if team:
        rows = [r for r in rows if r["_team"] == team]
    if factory == "1":
        rows = [r for r in rows if r["_factory_tab"]]
    # Return only display-safe keys (no internal _ keys except for filtering)
    out = []
    for r in rows:
        out.append({k:v for k,v in r.items() if not k.startswith("_")} | {"_team": r["_team"], "_tab": r["_tab"], "_factory_tab": r["_factory_tab"]})
    return jsonify(out[:500])

@app.route("/api/refresh")
def api_refresh():
    _cache["data"] = None
    get_data()
    return jsonify({"ok": True, "synced_at": time.strftime("%d %b %Y, %I:%M %p")})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(debug=False, host="0.0.0.0", port=port)
