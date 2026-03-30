"""
NG.py – Notion Interest System
================================
Chạy trên Render: python NG.py serve
Chạy thủ công  : python NG.py daily | telegram | test

Cấu trúc thư mục:
  .
  ├── NG.py
  ├── env/
  │   └── .env
  ├── logs/
  └── exports/

Cài đặt:
  pip install requests python-dotenv schedule flask
"""

from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
import schedule
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Thư mục & load .env
# ─────────────────────────────────────────────

BASE_DIR   = Path(__file__).resolve().parent
LOG_DIR    = BASE_DIR / "logs"
EXPORT_DIR = BASE_DIR / "exports"

for _d in (LOG_DIR, EXPORT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

_env_file = BASE_DIR / "env" / ".env"
if not _env_file.exists():
    _env_file = BASE_DIR / ".env"
load_dotenv(dotenv_path=_env_file)

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

def _setup_logging() -> logging.Logger:
    lg = logging.getLogger("ng")
    lg.setLevel(logging.DEBUG)
    lg.handlers.clear()
    lg.propagate = False
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for path, level in [
        (LOG_DIR / "app.log",   logging.INFO),
        (LOG_DIR / "error.log", logging.ERROR),
    ]:
        h = RotatingFileHandler(path, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
        h.setLevel(level)
        h.setFormatter(fmt)
        lg.addHandler(h)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    lg.addHandler(ch)
    return lg

logger = _setup_logging()

# ─────────────────────────────────────────────
# Config – đọc từ env/.env
# ─────────────────────────────────────────────

def _e(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()

@dataclass(frozen=True)
class Config:
    # Credentials
    notion_token:    str
    assets_db_id:    str
    interest_db_id:  str
    cashflow_db_id:  str
    reminder_db_id:  str
    tg_token:        str
    tg_chat_id:      str

    # Cài đặt
    daily_run_time: str  = "08:00"
    dry_run:        bool = False

    # ── Cột LỊCH NG (Assets) ──
    a_title:   str = "Name"
    a_asset:   str = "Tên tài sản"
    a_note:    str = "Ghi chú"
    a_capital: str = "Số tiền cầm"
    a_interest:str = "Lãi mỗi kỳ"
    a_cycle:   str = "Chu kỳ nhắc"
    a_pledge:  str = "Ngày cầm"
    a_status:  str = "Trạng thái"
    a_zalo:    str = "Zalo"

    # ── Cột TỔNG LÃI NG ──
    i_title:        str = "Name"
    i_asset:        str = "NG"
    i_due_date:     str = "Ngày phải thu"
    i_remind_date:  str = "Ngày nhắc trước"   # due - 2 ngày
    i_reminded:     str = "Đã nhắc"            # Chưa / Đã
    i_paid_date:    str = "Ngày thu"
    i_amount_due:   str = "Số tiền phải thu"
    i_amount_paid:  str = "Số tiền đã thu"
    i_status:       str = "Trạng thái"
    i_note:         str = "Ghi chú"
    i_cycle:        str = "Chu kỳ nhắc"

    # ── Cột BẢNG LÃI NG (Cashflow) ──
    c_title:   str = "Name"
    c_asset:   str = "NG"
    c_interest:str = "Tổng lãi NG"
    c_type:    str = "Loại tiền"
    c_amount:  str = "Số tiền"
    c_date:    str = "Ngày"
    c_status:  str = "Trạng thái"
    c_note:    str = "Ghi chú"


def load_config() -> Config:
    required = {
        "NOTION_TOKEN":    _e("NOTION_TOKEN"),
        "ASSETS_DB_ID":    _e("ASSETS_DB_ID"),
        "INTEREST_DB_ID":  _e("INTEREST_DB_ID"),
        "CASHFLOW_DB_ID":  _e("CASHFLOW_DB_ID"),
        "REMINDER_DB_ID":  _e("REMINDER_DB_ID"),
        "TELEGRAM_BOT_TOKEN": _e("TELEGRAM_BOT_TOKEN"),
        "TELEGRAM_CHAT_ID":   _e("TELEGRAM_CHAT_ID"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        logger.error("Thiếu biến môi trường: %s", ", ".join(missing))
        raise SystemExit("Kiểm tra lại env/.env")

    return Config(
        notion_token   = required["NOTION_TOKEN"],
        assets_db_id   = required["ASSETS_DB_ID"],
        interest_db_id = required["INTEREST_DB_ID"],
        cashflow_db_id = required["CASHFLOW_DB_ID"],
        reminder_db_id = required["REMINDER_DB_ID"],
        tg_token       = required["TELEGRAM_BOT_TOKEN"],
        tg_chat_id     = required["TELEGRAM_CHAT_ID"],
        daily_run_time = _e("DAILY_RUN_TIME", "08:00"),
        dry_run        = _e("DRY_RUN", "0") not in ("0", "", "false"),
    )

# ─────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────

def send_tg(cfg: Config, text: str) -> bool:
    if not cfg.tg_token or not cfg.tg_chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{cfg.tg_token}/sendMessage",
            json={"chat_id": cfg.tg_chat_id, "text": text},
            timeout=15,
        )
        if not r.ok:
            logger.error("Telegram lỗi %s: %s", r.status_code, r.text[:100])
            return False
        return True
    except Exception as e:
        logger.error("Lỗi gửi Telegram: %s", e)
        return False

# ─────────────────────────────────────────────
# Notion client
# ─────────────────────────────────────────────

class Notion:
    BASE = "https://api.notion.com/v1"
    VER  = "2022-06-28"

    def __init__(self, token: str, dry_run: bool = False):
        self.dry_run = dry_run
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization":  f"Bearer {token}",
            "Notion-Version": self.VER,
            "Content-Type":   "application/json",
        })

    def _r(self, method: str, path: str, body: Optional[dict] = None) -> dict:
        if self.dry_run and method.upper() in {"POST", "PATCH"}:
            logger.info("[DRY_RUN] %s %s", method, path)
            return {"id": "dry-id"}
        resp = self.s.request(method, f"{self.BASE}{path}", json=body, timeout=60)
        if resp.status_code >= 400:
            logger.error("Notion %s %s → %s", method, path, resp.text[:300])
            resp.raise_for_status()
        return resp.json()

    def query(self, db: str,
              filter_: Optional[dict] = None,
              sorts: Optional[list]   = None) -> List[dict]:
        rows, cursor = [], None
        while True:
            body: Dict[str, Any] = {}
            if filter_: body["filter"] = filter_
            if sorts:   body["sorts"]  = sorts
            if cursor:  body["start_cursor"] = cursor
            data = self._r("POST", f"/databases/{db}/query", body)
            rows.extend(data.get("results", []))
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
        return rows

    def get(self, page_id: str) -> dict:
        return self._r("GET", f"/pages/{page_id}")

    def create(self, db: str, props: dict) -> dict:
        return self._r("POST", "/pages",
                       {"parent": {"database_id": db}, "properties": props})

    def update(self, page_id: str, props: dict) -> dict:
        return self._r("PATCH", f"/pages/{page_id}", {"properties": props})

    def archive(self, page_id: str) -> dict:
        return self._r("PATCH", f"/pages/{page_id}", {"archived": True})

# ─────────────────────────────────────────────
# Property builders
# ─────────────────────────────────────────────

def _t(v: str) -> list:
    return [{"type": "text", "text": {"content": str(v)}}]

def p_title(v: str)             -> dict: return {"title": _t(v)}
def p_rich(v: str)              -> dict: return {"rich_text": _t(v)}
def p_num(v: Optional[float])   -> dict: return {"number": None if v is None else float(v)}
def p_date(v: Optional[str])    -> dict: return {"date": None if not v else {"start": v}}
def p_select(v: Optional[str])  -> dict: return {"select": None if not v else {"name": str(v)}}
def p_multi(vals: Iterable[str])-> dict:
    return {"multi_select": [{"name": str(x).strip()} for x in vals if str(x).strip()]}
def p_rel(ids: Iterable[str])   -> dict:
    seen: List[str] = []
    for i in ids:
        if i and i not in seen: seen.append(i)
    return {"relation": [{"id": i} for i in seen]}

# ─────────────────────────────────────────────
# Property readers
# ─────────────────────────────────────────────

def _p(page: dict, name: str) -> dict:
    return page.get("properties", {}).get(name) or {}

def g_title(page: dict, name: str) -> str:
    return "".join(x.get("plain_text","") for x in _p(page,name).get("title",[])).strip()

def g_rich(page: dict, name: str) -> str:
    return "".join(x.get("plain_text","") for x in _p(page,name).get("rich_text",[])).strip()

def g_num(page: dict, name: str) -> Optional[float]:
    return _p(page, name).get("number")

def g_select(page: dict, name: str) -> str:
    return (_p(page,name).get("select") or {}).get("name","")

def g_multi(page: dict, name: str) -> List[str]:
    return [x.get("name","") for x in _p(page,name).get("multi_select",[]) if x.get("name")]

def g_rel(page: dict, name: str) -> List[str]:
    return [x.get("id","") for x in _p(page,name).get("relation",[]) if x.get("id")]

def g_date(page: dict, name: str) -> Optional[str]:
    d = _p(page, name).get("date")
    return d.get("start") if d else None

# ─────────────────────────────────────────────
# Asset helpers
# ─────────────────────────────────────────────

STATUS_CLOSED = {"Đã chuộc", "Thanh lý"}

def a_active(asset: dict, cfg: Config) -> bool:
    return g_select(asset, cfg.a_status) not in STATUS_CLOSED

def a_name(asset: dict, cfg: Config) -> str:
    t  = g_title(asset, cfg.a_title)
    t2 = g_rich(asset, cfg.a_asset)
    return f"{t} - {t2}" if t and t2 else t or t2 or "(không tên)"

def a_interest(asset: dict, cfg: Config) -> float:
    return float(g_num(asset, cfg.a_interest) or 0)

def a_cycle_days(asset: dict, cfg: Config) -> List[int]:
    return [int(m.group()) for v in g_multi(asset, cfg.a_cycle)
            if (m := re.search(r"\d+", str(v)))]

# ─────────────────────────────────────────────
# Daily job – tạo kỳ lãi
# ─────────────────────────────────────────────

def run_daily(notion: Notion, cfg: Config) -> None:
    """
    Mỗi ngày chạy 1 lần:
    - Tạo dòng Tổng lãi NG cho NG đến hạn hôm nay
    - Điền sẵn Ngày nhắc trước = due - 2 ngày
    - Báo Telegram tổng kết
    """
    today     = date.today()
    today_str = today.isoformat()
    logger.info("=== DAILY %s ===", today_str)

    # Lấy tất cả NG đang hoạt động
    assets = notion.query(
        cfg.assets_db_id,
        filter_={"and": [
            {"property": cfg.a_status, "select": {"does_not_equal": "Thanh lý"}},
            {"property": cfg.a_status, "select": {"does_not_equal": "Đã chuộc"}},
        ]},
    )
    active = [a for a in assets if a_active(a, cfg)]
    logger.info("Tìm thấy %d NG đang hoạt động", len(active))

    created      = 0
    report_lines = []

    for asset in active:
        if today.day not in a_cycle_days(asset, cfg):
            continue

        # Kiểm tra kỳ lãi hôm nay đã tồn tại chưa
        exists = notion.query(cfg.interest_db_id, filter_={"and": [
            {"property": cfg.i_asset,    "relation": {"contains": asset["id"]}},
            {"property": cfg.i_due_date, "date":     {"equals":   today_str}},
        ]})
        if exists:
            logger.info("Đã có kỳ lãi: %s | %s", a_name(asset, cfg), today_str)
            continue

        amount       = a_interest(asset, cfg)
        remind_date  = (today - timedelta(days=2)).isoformat()
        title        = f"{a_name(asset, cfg)} | {today_str}"

        props: Dict[str, dict] = {
            cfg.i_title:       p_title(title),
            cfg.i_asset:       p_rel([asset["id"]]),
            cfg.i_due_date:    p_date(today_str),
            cfg.i_remind_date: p_date(remind_date),
            cfg.i_amount_due:  p_num(amount),
            cfg.i_status:      p_select("Chưa thu"),
            cfg.i_reminded:    p_select("Chưa"),
        }
        notion.create(cfg.interest_db_id, props)

        zalo    = g_rich(asset, cfg.a_zalo) or "(chưa có)"
        asset_n = g_rich(asset, cfg.a_asset) or "(chưa ghi)"
        report_lines.append(
            f"  • {a_name(asset, cfg)}\n"
            f"    Tài sản : {asset_n}\n"
            f"    Zalo    : {zalo}\n"
            f"    Lãi kỳ : {amount:,.0f} đ"
        )
        logger.info("✅ Tạo kỳ lãi: %s | %.0f đ", a_name(asset, cfg), amount)
        created += 1

    # Báo Telegram
    if created > 0:
        msg = (
            f"📊 TẠO KỲ LÃI {today_str}\n"
            f"Tổng: {created} kỳ\n\n"
            + "\n\n".join(report_lines)
        )
    else:
        msg = f"📊 DAILY {today_str}\nKhông có kỳ lãi nào hôm nay."

    send_tg(cfg, msg)
    logger.info("Daily xong – tạo %d kỳ lãi", created)

# ─────────────────────────────────────────────
# Thu lãi
# ─────────────────────────────────────────────

def find_open_schedule(notion: Notion, cfg: Config, code: str) -> Optional[dict]:
    """Tìm kỳ lãi Chưa thu / Quá hạn theo mã khách."""
    rows = notion.query(
        cfg.interest_db_id,
        filter_={"or": [
            {"property": cfg.i_status, "select": {"equals": "Chưa thu"}},
            {"property": cfg.i_status, "select": {"equals": "Quá hạn"}},
        ]},
        sorts=[{"property": cfg.i_due_date, "direction": "ascending"}],
    )
    code_up = code.strip().upper()
    return next(
        (r for r in rows if code_up in g_title(r, cfg.i_title).upper()),
        None
    )


def settle(notion: Notion, cfg: Config, schedule_id: str) -> Dict[str, Any]:
    """
    Thu 1 kỳ lãi:
      1. Tạo dòng Bảng lãi NG
      2. Xoá dòng Tổng lãi NG (giữ DB nhẹ)
    """
    row      = notion.get(schedule_id)
    a_ids    = g_rel(row, cfg.i_asset)
    if not a_ids:
        raise ValueError("Không có relation NG.")

    amount   = g_num(row, cfg.i_amount_due) or 0
    due_date = g_date(row, cfg.i_due_date) or date.today().isoformat()
    today    = date.today().isoformat()

    # 1. Tạo Bảng lãi NG
    notion.create(cfg.cashflow_db_id, {
        cfg.c_title:    p_title(f"Thu lãi | {today} | {amount:,.0f}"),
        cfg.c_asset:    p_rel([a_ids[0]]),
        cfg.c_interest: p_rel([schedule_id]),
        cfg.c_type:     p_select("Lãi"),
        cfg.c_amount:   p_num(amount),
        cfg.c_date:     p_date(today),
        cfg.c_status:   p_select("Đã thu"),
    })

    # 2. Xoá Tổng lãi NG
    try:
        notion.archive(schedule_id)
    except Exception as e:
        logger.error("Không xoá được kỳ lãi: %s", e)

    return {"amount": amount, "due_date": due_date}

# ─────────────────────────────────────────────
# Telegram command handlers
# ─────────────────────────────────────────────

def cmd_info(notion: Notion, cfg: Config, code: str) -> str:
    code_up = code.strip().upper()
    assets  = notion.query(cfg.assets_db_id)
    asset = next(
    (a for a in assets
     if g_title(a, cfg.a_title).upper() == code_up),
    None,
    )
    if not asset:
        return f"❌ Không tìm thấy: {code_up}"

    capital  = g_num(asset, cfg.a_capital) or 0
    interest = a_interest(asset, cfg)
    asset_n  = g_rich(asset, cfg.a_asset) or "(chưa ghi)"
    zalo     = g_rich(asset, cfg.a_zalo)  or "(chưa có)"
    days     = ", ".join(str(d) for d in a_cycle_days(asset, cfg))
    pledge   = g_date(asset, cfg.a_pledge) or "(chưa rõ)"
    status   = g_select(asset, cfg.a_status)

    row = find_open_schedule(notion, cfg, code_up)
    if row:
        due_date = g_date(row, cfg.i_due_date) or ""
        due_amt  = g_num(row, cfg.i_amount_due) or 0
        ky_line  = f"Kỳ chưa thu : {due_date} | {due_amt:,.0f} đ"
        hint     = f"👉 /thu {code_up} 1"
    else:
        ky_line = "Kỳ chưa thu : ✅ Không có"
        hint    = ""

    lines = [
        f"📋 {code_up}",
        f"Tài sản     : {asset_n}",
        f"Ngày cầm    : {pledge}",
        f"Số tiền cầm : {capital:,.0f} đ",
        f"Lãi mỗi kỳ : {interest:,.0f} đ",
        f"Chu kỳ      : ngày {days}",
        f"Zalo        : {zalo}",
        f"Trạng thái  : {status}",
        "─────────────────",
        ky_line,
    ]
    if hint: lines.append(hint)
    return "\n".join(lines)


def cmd_thu(notion: Notion, cfg: Config, code: str) -> str:
    row = find_open_schedule(notion, cfg, code.strip().upper())
    if not row:
        return f"❌ Không có kỳ chưa thu: {code.upper()}"
    try:
        result = settle(notion, cfg, row["id"])
        return (
            f"✅ ĐÃ THU\n"
            f"Khách   : {code.upper()}\n"
            f"Kỳ      : {result['due_date']}\n"
            f"Số tiền : {result['amount']:,.0f} đ\n"
            f"🗑️ Đã xoá khỏi Tổng lãi NG"
        )
    except Exception as e:
        logger.error("Lỗi thu %s: %s", code, e)
        return f"❌ Lỗi: {e}"


def cmd_status(notion: Notion, cfg: Config) -> str:
    rows = notion.query(
        cfg.interest_db_id,
        filter_={"or": [
            {"property": cfg.i_status, "select": {"equals": "Chưa thu"}},
            {"property": cfg.i_status, "select": {"equals": "Quá hạn"}},
        ]},
        sorts=[{"property": cfg.i_due_date, "direction": "ascending"}],
    )
    if not rows:
        return "✅ Không còn kỳ nào chưa thu."
    lines, total = [], 0.0
    for r in rows:
        title  = g_title(r, cfg.i_title)
        amt    = g_num(r, cfg.i_amount_due) or 0
        status = g_select(r, cfg.i_status)
        icon   = "⚠️" if status == "Quá hạn" else "🔔"
        lines.append(f"{icon} {title} | {amt:,.0f} đ")
        total += amt
    return (
        f"📋 CHƯA THU ({len(rows)} kỳ)\n"
        + "\n".join(lines)
        + f"\n─────────────\nTổng: {total:,.0f} đ"
    )


def cmd_quahan(notion: Notion, cfg: Config) -> str:
    rows = notion.query(
        cfg.interest_db_id,
        filter_={"property": cfg.i_status, "select": {"equals": "Quá hạn"}},
        sorts=[{"property": cfg.i_due_date, "direction": "ascending"}],
    )
    if not rows:
        return "✅ Không có kỳ quá hạn."
    lines, total = [], 0.0
    for r in rows:
        title = g_title(r, cfg.i_title)
        amt   = g_num(r, cfg.i_amount_due) or 0
        due   = g_date(r, cfg.i_due_date) or ""
        lines.append(f"⚠️ {title} | {amt:,.0f} đ | hạn {due}")
        total += amt
    return (
        f"🚨 QUÁ HẠN ({len(rows)} kỳ)\n"
        + "\n".join(lines)
        + f"\n─────────────\nTổng: {total:,.0f} đ"
    )


def cmd_thang(notion: Notion, cfg: Config) -> str:
    today = date.today()
    start = today.replace(day=1).isoformat()
    end   = (today.replace(day=1) + timedelta(days=32)).replace(day=1).isoformat()

    collected_rows = notion.query(cfg.cashflow_db_id, filter_={"and": [
        {"property": cfg.c_date,   "date":   {"on_or_after": start}},
        {"property": cfg.c_date,   "date":   {"before":      end}},
        {"property": cfg.c_type,   "select": {"equals":      "Lãi"}},
    ]})
    open_rows = notion.query(cfg.interest_db_id, filter_={"or": [
        {"property": cfg.i_status, "select": {"equals": "Chưa thu"}},
        {"property": cfg.i_status, "select": {"equals": "Quá hạn"}},
    ]})

    collected = sum(float(g_num(r, cfg.c_amount) or 0) for r in collected_rows)
    pending   = sum(float(g_num(r, cfg.i_amount_due) or 0) for r in open_rows)

    return (
        f"📅 BÁO CÁO THÁNG {today.strftime('%m/%Y')}\n"
        f"Đã thu       : {collected:,.0f} đ ({len(collected_rows)} kỳ)\n"
        f"Còn chưa thu : {pending:,.0f} đ ({len(open_rows)} kỳ)\n"
        f"Tổng         : {collected + pending:,.0f} đ"
    )

# ─────────────────────────────────────────────
# Telegram polling
# ─────────────────────────────────────────────

HELP = (
    "📌 Lệnh hỗ trợ:\n"
    "/N001           → thông tin khách\n"
    "/thu N001 1     → thu 1 kỳ lãi\n"
    "/status         → danh sách chưa thu\n"
    "/quahan         → quá hạn\n"
    "/thang          → báo cáo tháng"
    "/d              → chạy daily ngay"
)


def run_polling(cfg: Config) -> None:
    notion = Notion(cfg.notion_token, dry_run=cfg.dry_run)
    api    = f"https://api.telegram.org/bot{cfg.tg_token}"
    offset = 0
    logger.info("Telegram polling bắt đầu …")

    while True:
        try:
            resp    = requests.get(
                f"{api}/getUpdates",
                params={"timeout": 30, "offset": offset},
                timeout=40,
            )
            updates = resp.json().get("result", [])

            for upd in updates:
                offset = upd["update_id"] + 1
                msg    = upd.get("message", {})
                text   = (msg.get("text") or "").strip()
                cid    = str(msg.get("chat", {}).get("id", ""))

                if cid != cfg.tg_chat_id or not text:
                    continue

                logger.info("TG: %s", text)
                parts = text.split()
                cmd   = parts[0].lower()

                # /N001 hoặc /G001 ...
                if re.match(r"^/[a-zA-Z]\d+$", parts[0]):
                    reply = cmd_info(notion, cfg, parts[0][1:])

                elif cmd == "/thu":
                    if len(parts) < 3:
                        reply = "❓ Cú pháp: /thu N001 1"
                    else:
                        reply = cmd_thu(notion, cfg, parts[1])

                elif cmd == "/status":
                    reply = cmd_status(notion, cfg)

                elif cmd == "/quahan":
                    reply = cmd_quahan(notion, cfg)

                elif cmd == "/thang":
                    reply = cmd_thang(notion, cfg)

                else:
                    reply = HELP

                send_tg(cfg, reply)

        except Exception as e:
            logger.error("Lỗi polling: %s", e)
            time.sleep(5)
def _handle_tg_msg(notion: Notion, cfg: Config, text: str) -> None:
    parts = text.split()
    cmd   = parts[0].lower()
    if re.match(r"^/[a-zA-Z0-9\-]+$", parts[0]):
      reply = cmd_info(notion, cfg, parts[0][1:])
    elif cmd == "/thu":
        reply = cmd_thu(notion, cfg, parts[1]) if len(parts) >= 3 else "❓ /thu N001 1"
    elif cmd == "/status":
        reply = cmd_status(notion, cfg)
    elif cmd == "/quahan":
        reply = cmd_quahan(notion, cfg)
    elif cmd == "/thang":
        reply = cmd_thang(notion, cfg)
    else:
        reply = HELP
    elif cmd == "/d":
            threading.Thread(
                target=run_daily,
                args=(notion, cfg),
                daemon=True,
            ).start()
            reply = "⚙️ Đang chạy daily..."  
    send_tg(cfg, reply)
    
# ─────────────────────────────────────────────
# Serve – chạy trên Render
# ─────────────────────────────────────────────

def run_serve(cfg: Config) -> None:
    try:
        from flask import Flask
    except ImportError:
        raise SystemExit("pip install flask")

    app    = Flask(__name__)
    notion = Notion(cfg.notion_token, dry_run=cfg.dry_run)

    @app.route("/")
    @app.route("/health")
    def health():
        return {"status": "ok", "date": date.today().isoformat()}, 200

    @app.route("/run-now")
    def run_now():
        threading.Thread(target=job, daemon=True).start()
        return {"status": "ok", "action": "daily running"}, 200

    @app.route("/webhook", methods=["POST"])
    def webhook():
        from flask import request as freq
        upd  = freq.get_json()
        if not upd:
            return "ok", 200
        msg  = upd.get("message", {})
        text = (msg.get("text") or "").strip()
        cid  = str(msg.get("chat", {}).get("id", ""))
        if cid == cfg.tg_chat_id and text:
            threading.Thread(
                target=_handle_tg_msg,
                args=(notion, cfg, text),
                daemon=True,
            ).start()
        return "ok", 200

    # Scheduler
    def job():
        try:
            run_daily(notion, cfg)
        except Exception as e:
            logger.exception("Lỗi daily job: %s", e)

    schedule.every().day.at(cfg.daily_run_time).do(job)
    logger.info("Scheduler: %s mỗi ngày", cfg.daily_run_time)

    def sched_loop():
        while True:
            schedule.run_pending()
            time.sleep(30)
    send_tg(cfg, f"🚀 NG khởi động – {date.today().isoformat()}")

    port = int(os.getenv("PORT", "8000"))
    logger.info("Flask port %d", port)
    app.run(host="0.0.0.0", port=port)

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="NG – Notion Interest System")
    p.add_argument("command", nargs="?", default="serve",
                   help="serve | daily | telegram | test")
    args = p.parse_args()
    cfg  = load_config()

    try:
        if args.command == "serve":
            run_serve(cfg)
        elif args.command == "daily":
            notion = Notion(cfg.notion_token, dry_run=cfg.dry_run)
            run_daily(notion, cfg)
        elif args.command == "telegram":
            run_polling(cfg)
        elif args.command == "test":
            ok = send_tg(cfg, f"✅ NG test OK – {date.today().isoformat()}")
            print("OK" if ok else "THẤT BẠI")
        else:
            raise SystemExit(f"Lệnh không hợp lệ: {args.command}")
    except requests.HTTPError:
        logger.exception("Lỗi HTTP Notion")
        raise
    except Exception as e:
        logger.exception("Lỗi: %s", e)
        raise


if __name__ == "__main__":
    main()
