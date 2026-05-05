import calendar
import csv
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import io
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
from threading import Lock, Thread
from time import monotonic
from typing import List, Optional
import unicodedata
from urllib.parse import quote
from urllib.error import HTTPError, URLError
from urllib.request import Request as UrlRequest, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, URLSafeSerializer


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "delivery_ops.db"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
USE_POSTGRES = bool(DATABASE_URL)
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "3"))
DB_INIT_RETRY_SECONDS = int(os.getenv("DB_INIT_RETRY_SECONDS", "30"))
APP_NAME = "SPARKLE DRIVE"
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_STORAGE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_SERVICE_KEY")
    or os.getenv("SUPABASE_STORAGE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
    or ""
)
SUPABASE_STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "inspection-sheets")
SUPABASE_STORAGE_PUBLIC_BASE = os.getenv("SUPABASE_STORAGE_PUBLIC_URL_BASE", "").rstrip("/")
try:
    APP_TZ = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Tokyo"))
except ZoneInfoNotFoundError:
    APP_TZ = timezone(timedelta(hours=9))
UPLOAD_DIR = BASE_DIR / "uploads" / "inspection_sheets"
SLIP_UPLOAD_DIR = BASE_DIR / "uploads" / "inspection_slips"
SECRET_KEY = "change-this-local-secret"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
SLIP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title=APP_NAME)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.mount("/uploads", StaticFiles(directory=BASE_DIR / "uploads"), name="uploads")
templates = Jinja2Templates(directory=BASE_DIR / "templates")
serializer = URLSafeSerializer(SECRET_KEY, salt="delivery-ops-session")
logger = logging.getLogger("delivery_app")
DB_INIT_LOCK = Lock()
DB_INIT_DONE = False
DB_INIT_ERROR = None
DB_INIT_LAST_ATTEMPT = 0.0
SHIFT_ALLOWED_STATUSES = {"出勤", "1便", "2便", "3便", "休み"}
SHIFT_IMPORT_HEADERS = ["日付", "名前", "出勤区分", "配達エリア", "便", "備考"]
SHIFT_IMPORT_LIMIT = 500
FULLWIDTH_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")
FEATURE_CATALOG = [
    {"key": "basic", "label": "基本管理", "price": 3000, "note": "課金予定"},
    {"key": "members", "label": "メンバー管理", "price": 0, "note": "テスト中"},
    {"key": "deliveries", "label": "配達個数管理", "price": 2000, "note": "課金予定"},
    {"key": "rewards", "label": "給料・報酬管理", "price": 3000, "note": "課金予定"},
    {"key": "shifts", "label": "シフト管理10人まで", "price": 2000, "note": "課金予定"},
    {"key": "shifts_20", "label": "シフト管理20人まで", "price": 4000, "note": "課金予定"},
    {"key": "inspection_ocr", "label": "点検表OCR", "price": 3000, "note": "課金予定"},
    {"key": "safety", "label": "安全記録", "price": 1000, "note": "テスト中"},
    {"key": "vehicle", "label": "車両管理", "price": 1000, "note": "テスト中"},
    {"key": "issues", "label": "不具合管理", "price": 1000, "note": "テスト中"},
    {"key": "holidays", "label": "休み希望管理", "price": 1000, "note": "テスト中"},
    {"key": "multi_depot", "label": "複数拠点管理", "price": 2000, "note": "テスト中"},
]
FEATURE_KEYS = {item["key"] for item in FEATURE_CATALOG}
FEATURE_ALIASES = {"shifts": ("shifts", "shifts_20"), "inspection": ("inspection_ocr",)}


def app_now():
    return datetime.now(APP_TZ)


def app_today():
    return app_now().date()


def seed_company_features(conn, company_id: int, enabled: int = 1):
    now = datetime.now().isoformat(timespec="seconds")
    for feature in FEATURE_CATALOG:
        conn.execute(
            "INSERT OR IGNORE INTO company_features(company_id, feature_key, enabled, updated_at) VALUES (?,?,?,?)",
            (company_id, feature["key"], enabled, now),
        )


if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    DB_INTEGRITY_ERROR = psycopg2.IntegrityError
else:
    psycopg2 = None
    RealDictCursor = None
    DB_INTEGRITY_ERROR = sqlite3.IntegrityError


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class DbCursor:
    def __init__(self, cursor, lastrowid=None):
        self.cursor = cursor
        self.lastrowid = lastrowid if lastrowid is not None else getattr(cursor, "lastrowid", None)

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()


class DbConnection:
    def __init__(self, conn, use_postgres: bool):
        self.conn = conn
        self.use_postgres = use_postgres

    def execute(self, sql: str, params=()):
        if not self.use_postgres:
            return self.conn.execute(sql, params or ())

        return_id = _needs_returning_id(sql)
        translated_sql = _postgres_sql(sql, return_id=return_id)
        cur = self.conn.cursor()
        cur.execute(translated_sql, params or ())
        lastrowid = None
        if return_id:
            row = cur.fetchone()
            if row:
                lastrowid = row["id"]
        return DbCursor(cur, lastrowid)

    def executescript(self, sql: str):
        if not self.use_postgres:
            return self.conn.executescript(sql)
        result = None
        for statement in _split_sql_script(sql):
            result = self.execute(statement)
        return result

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()


def _postgres_sql(sql: str, return_id: bool = False) -> str:
    translated = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
        "SERIAL PRIMARY KEY",
        sql,
        flags=re.IGNORECASE,
    )
    translated = re.sub(r"\bAUTOINCREMENT\b", "", translated, flags=re.IGNORECASE)

    if re.match(r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\b", translated, flags=re.IGNORECASE):
        translated = re.sub(
            r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\b",
            "INSERT INTO",
            translated,
            count=1,
            flags=re.IGNORECASE,
        )
        if "ON CONFLICT" not in translated.upper():
            translated = _append_sql_clause(translated, "ON CONFLICT DO NOTHING")

    translated = translated.replace("?", "%s")
    if return_id:
        translated = _append_sql_clause(translated, "RETURNING id")
    return translated


def _split_sql_script(sql: str):
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _append_sql_clause(sql: str, clause: str) -> str:
    stripped = sql.rstrip()
    has_semicolon = stripped.endswith(";")
    if has_semicolon:
        stripped = stripped[:-1].rstrip()
    stripped = f"{stripped} {clause}"
    return f"{stripped};" if has_semicolon else stripped


def _needs_returning_id(sql: str) -> bool:
    return bool(
        re.match(r"^\s*INSERT\s+INTO\s+(users|vehicle_issues|companies)\b", sql, flags=re.IGNORECASE)
        and "RETURNING" not in sql.upper()
        and "ON CONFLICT" not in sql.upper()
    )


def _quote_identifier(name: str) -> str:
    if not IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return f'"{name}"' if USE_POSTGRES else name


@contextmanager
def db():
    if USE_POSTGRES:
        connect_kwargs = {
            "cursor_factory": RealDictCursor,
            "connect_timeout": DB_CONNECT_TIMEOUT,
        }
        if "sslmode=" not in DATABASE_URL.lower():
            connect_kwargs["sslmode"] = "require"
        conn = psycopg2.connect(DATABASE_URL, **connect_kwargs)
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

    wrapped = DbConnection(conn, USE_POSTGRES)
    try:
        yield wrapped
        wrapped.commit()
    except Exception:
        wrapped.rollback()
        raise
    finally:
        wrapped.close()


def ensure_db_initialized():
    if DB_INIT_DONE:
        return
    if USE_POSTGRES and DB_INIT_ERROR and monotonic() - DB_INIT_LAST_ATTEMPT < DB_INIT_RETRY_SECONDS:
        raise HTTPException(status_code=503, detail="DB初期化に失敗しています。少し時間をおいて再読み込みしてください。")
    init_db()


def hash_password(password: str) -> str:
    return sha256(password.encode("utf-8")).hexdigest()


def safe_upload_name(original_name: str):
    suffix = Path(original_name or "").suffix.lower()
    if suffix not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic", ".heif"}:
        suffix = ".jpg"
    return suffix


def supabase_storage_enabled():
    return bool(SUPABASE_URL and SUPABASE_STORAGE_KEY and SUPABASE_STORAGE_BUCKET)


def _storage_path(folder: str, filename: str):
    clean_folder = folder.strip("/").replace("\\", "/")
    clean_name = Path(filename).name
    return f"{clean_folder}/{clean_name}"


def _encoded_storage_path(storage_path: str):
    return "/".join(quote(part, safe="") for part in storage_path.split("/"))


def upload_to_supabase_storage(folder: str, filename: str, data: bytes, content_type: str):
    if not supabase_storage_enabled():
        return ""
    storage_path = _storage_path(folder, filename)
    encoded_path = _encoded_storage_path(storage_path)
    url = f"{SUPABASE_URL}/storage/v1/object/{quote(SUPABASE_STORAGE_BUCKET, safe='')}/{encoded_path}"
    request = UrlRequest(
        url,
        data=data,
        method="POST",
        headers={
            "apikey": SUPABASE_STORAGE_KEY,
            "Authorization": f"Bearer {SUPABASE_STORAGE_KEY}",
            "Content-Type": content_type or "application/octet-stream",
            "Cache-Control": "3600",
            "x-upsert": "true",
        },
    )
    try:
        with urlopen(request, timeout=DB_CONNECT_TIMEOUT) as response:
            if response.status >= 400:
                logger.warning("Supabase Storage upload failed: %s", response.status)
                return ""
    except (HTTPError, URLError, TimeoutError, OSError):
        logger.exception("Supabase Storage upload failed; falling back to local uploads.")
        return ""
    if SUPABASE_STORAGE_PUBLIC_BASE:
        return f"{SUPABASE_STORAGE_PUBLIC_BASE}/{encoded_path}"
    return f"{SUPABASE_URL}/storage/v1/object/public/{quote(SUPABASE_STORAGE_BUCKET, safe='')}/{encoded_path}"


def save_inspection_image(data: bytes, filename: str, content_type: str, folder: str, local_dir: Path):
    remote_url = upload_to_supabase_storage(folder, filename, data, content_type)
    if remote_url:
        return remote_url, None
    local_dir.mkdir(parents=True, exist_ok=True)
    disk_path = local_dir / filename
    disk_path.write_bytes(data)
    return f"/uploads/{folder}/{filename}", disk_path


def is_remote_image_path(path: str):
    return bool(path and re.match(r"^https?://", path, flags=re.IGNORECASE))


def allowed_local_image_path(path: str):
    return bool(
        path
        and (
            path.startswith("/uploads/inspection_sheets/")
            or path.startswith("/uploads/inspection_slips/")
        )
    )


def local_image_exists(path: str):
    return allowed_local_image_path(path) and (BASE_DIR / path.lstrip("/")).exists()


def inspection_image_info(path: str):
    path = path or ""
    if not path:
        return {"image_path": "", "image_available": False, "image_missing": False, "image_view_url": "", "image_label": "画像なし"}
    if is_remote_image_path(path):
        return {"image_path": path, "image_available": True, "image_missing": False, "image_view_url": path, "image_label": "画像あり"}
    view_url = f"/inspection-file?path={quote(path, safe='')}"
    if local_image_exists(path):
        return {"image_path": path, "image_available": True, "image_missing": False, "image_view_url": view_url, "image_label": "画像あり"}
    return {
        "image_path": path,
        "image_available": False,
        "image_missing": True,
        "image_view_url": view_url,
        "image_label": "画像ファイルが見つかりません。再アップロードしてください",
    }


def with_image_info(row, path_field="file_path"):
    item = dict(row)
    item.update(inspection_image_info(item.get(path_field, "")))
    return item


def attach_image_info(rows, path_field="file_path"):
    return [with_image_info(row, path_field) for row in rows]


def row_value(row, key, default=None):
    if not row:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key] if key in row.keys() else default
    except (AttributeError, KeyError, TypeError):
        return default


def company_id_for(user):
    return int(row_value(user, "company_id", 1) or 1)


def company_name_for(user):
    return row_value(user, "company_name", "") or APP_NAME


def can_manage_companies(user):
    return row_value(user, "role") == "admin" and company_id_for(user) == 1


def deleted_username_for(user_id):
    return f"deleted_member_{user_id}_{app_now().strftime('%Y%m%d%H%M%S%f')}"


def release_purged_username(username: str):
    username = (username or "").strip()
    if not username:
        return
    rows = query_all("SELECT id FROM users WHERE username=? AND COALESCE(purged,0)=1", (username,))
    for row in rows:
        released_username = deleted_username_for(row["id"])
        now = app_now().isoformat(timespec="seconds")
        execute(
            """UPDATE users
               SET username=?, password_hash=?, deleted_at=COALESCE(NULLIF(deleted_at,''), ?)
               WHERE id=? AND username=? AND COALESCE(purged,0)=1""",
            (released_username, hash_password(released_username), now, row["id"], username),
        )


def execute(sql: str, params=()):
    ensure_db_initialized()
    with db() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur


def query_one(sql: str, params=()):
    ensure_db_initialized()
    with db() as conn:
        return conn.execute(sql, params).fetchone()


def query_all(sql: str, params=()):
    ensure_db_initialized()
    with db() as conn:
        return conn.execute(sql, params).fetchall()


def feature_price_label(feature):
    price = int(feature.get("price") or 0)
    return "基本プランに含む" if price <= 0 else f"月額{price:,}円"


def is_feature_enabled(features, key: str) -> bool:
    keys = FEATURE_ALIASES.get(key, (key,))
    return any(bool(features.get(item, True)) for item in keys)


def load_company_features(company_id: int):
    enabled = {feature["key"]: True for feature in FEATURE_CATALOG}
    rows = query_all("SELECT feature_key, enabled FROM company_features WHERE company_id=?", (company_id,))
    for row in rows:
        key = row["feature_key"]
        if key in FEATURE_KEYS:
            enabled[key] = bool(row["enabled"])
    return enabled


def company_feature_rows(company_id: int):
    enabled = load_company_features(company_id)
    rows = []
    for feature in FEATURE_CATALOG:
        item = dict(feature)
        item["enabled"] = bool(enabled.get(feature["key"], True))
        item["price_label"] = feature_price_label(feature)
        rows.append(item)
    return rows


def feature_enabled_for_company(company_id: int, key: str) -> bool:
    return is_feature_enabled(load_company_features(company_id), key)


def require_feature(user, key: str):
    if not feature_enabled_for_company(company_id_for(user), key):
        raise HTTPException(status_code=403, detail="この機能は未契約です")


def ensure_column(conn, table: str, column: str, definition: str):
    if USE_POSTGRES:
        exists = conn.execute(
            """SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = ? AND column_name = ?""",
            (table, column),
        ).fetchone()
        if not exists:
            conn.execute(
                f"ALTER TABLE {_quote_identifier(table)} ADD COLUMN {_quote_identifier(column)} {definition}"
            )
        return

    columns = [row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


ISSUE_STATUSES = ["未対応", "確認中", "修理依頼中", "対応済", "修理済"]
ISSUE_STATUS_CLASSES = {
    "未対応": "todo",
    "確認中": "checking",
    "修理依頼中": "repair-requested",
    "対応済": "handled",
    "修理済": "repaired",
}


def ensure_vehicle_issue_schema():
    with db() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS vehicle_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL DEFAULT 1,
                user_id INTEGER NOT NULL,
                issue_date TEXT NOT NULL,
                vehicle_name TEXT NOT NULL,
                severity TEXT NOT NULL,
                detail TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '未対応',
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )"""
        )
        ensure_column(conn, "vehicle_issues", "company_id", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "vehicle_issues", "status", "TEXT NOT NULL DEFAULT '未対応'")
        ensure_column(conn, "vehicle_issues", "created_at", "TEXT DEFAULT ''")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS vehicle_issue_status_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                changed_by INTEGER NOT NULL,
                changed_at TEXT NOT NULL,
                note TEXT DEFAULT '',
                FOREIGN KEY(issue_id) REFERENCES vehicle_issues(id),
                FOREIGN KEY(changed_by) REFERENCES users(id)
            )"""
        )
        ensure_column(conn, "vehicle_issue_status_logs", "note", "TEXT DEFAULT ''")
        conn.commit()


def normalize_issue_status(status: str) -> str:
    return status if status in ISSUE_STATUSES else "未対応"


def date_label(day: date):
    weekdays = "月火水木金土日"
    return f"{day.isoformat()}（{weekdays[day.weekday()]}）"


def simple_japanese_holidays(year: int):
    days = {
        date(year, 1, 1),
        date(year, 2, 11),
        date(year, 2, 23),
        date(year, 4, 29),
        date(year, 5, 3),
        date(year, 5, 4),
        date(year, 5, 5),
        date(year, 8, 11),
        date(year, 11, 3),
        date(year, 11, 23),
    }

    def nth_monday(month, nth):
        first = date(year, month, 1)
        offset = (7 - first.weekday()) % 7
        return first + timedelta(days=offset + 7 * (nth - 1))

    days.update(
        {
            nth_monday(1, 2),
            nth_monday(7, 3),
            nth_monday(9, 3),
            nth_monday(10, 2),
            date(year, 3, 20),
            date(year, 9, 23),
        }
    )
    return days


def _init_db_schema():
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                code TEXT DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL DEFAULT 1,
                name TEXT NOT NULL,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin','member')),
                phone TEXT DEFAULT '',
                vehicle TEXT DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                purged INTEGER NOT NULL DEFAULT 0,
                deleted_at TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rates (
                user_id INTEGER PRIMARY KEY,
                company_id INTEGER NOT NULL DEFAULT 1,
                delivery_unit INTEGER NOT NULL DEFAULT 180,
                transfer_unit INTEGER NOT NULL DEFAULT 80,
                night_unit INTEGER NOT NULL DEFAULT 120,
                pickup_unit INTEGER NOT NULL DEFAULT 100,
                large_unit INTEGER NOT NULL DEFAULT 150,
                vehicle_rental_type TEXT NOT NULL DEFAULT 'none',
                vehicle_daily_fee INTEGER NOT NULL DEFAULT 0,
                vehicle_monthly_fee INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS vehicle_rates (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                daily_fee INTEGER NOT NULL DEFAULT 1500,
                monthly_fee INTEGER NOT NULL DEFAULT 30000
            );
            CREATE TABLE IF NOT EXISTS work_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL DEFAULT 1,
                user_id INTEGER NOT NULL,
                work_date TEXT NOT NULL,
                log_type TEXT NOT NULL CHECK(log_type IN ('start','end')),
                logged_at TEXT NOT NULL,
                alcohol_result TEXT NOT NULL,
                detector_used TEXT NOT NULL,
                intoxicated TEXT NOT NULL,
                health_status TEXT NOT NULL,
                face_check TEXT NOT NULL,
                breath_check TEXT NOT NULL,
                voice_check TEXT NOT NULL,
                admin_confirm TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL DEFAULT 1,
                user_id INTEGER NOT NULL,
                work_date TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                transfer INTEGER NOT NULL DEFAULT 0,
                night INTEGER NOT NULL DEFAULT 0,
                pickup INTEGER NOT NULL DEFAULT 0,
                large INTEGER NOT NULL DEFAULT 0,
                vehicle_rental TEXT NOT NULL DEFAULT 'none',
                memo TEXT DEFAULT '',
                inspection_sheet_path TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, work_date),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS vehicle_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL DEFAULT 1,
                user_id INTEGER NOT NULL,
                issue_date TEXT NOT NULL,
                vehicle_name TEXT NOT NULL,
                severity TEXT NOT NULL,
                detail TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '未対応',
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS holiday_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL DEFAULT 1,
                user_id INTEGER NOT NULL,
                request_date TEXT NOT NULL,
                reason TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                UNIQUE(user_id, request_date),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL DEFAULT 1,
                user_id INTEGER NOT NULL,
                shift_date TEXT NOT NULL,
                status TEXT NOT NULL,
                start_time TEXT DEFAULT '',
                end_time TEXT DEFAULT '',
                note TEXT DEFAULT '',
                district_id INTEGER,
                town_id INTEGER,
                area_label TEXT DEFAULT '',
                decided INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, shift_date),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS depots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS districts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                depot_id INTEGER,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                UNIQUE(depot_id, name),
                FOREIGN KEY(depot_id) REFERENCES depots(id)
            );
            CREATE TABLE IF NOT EXISTS towns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                district_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                UNIQUE(district_id, name),
                FOREIGN KEY(district_id) REFERENCES districts(id)
            );
            CREATE TABLE IF NOT EXISTS shift_towns (
                shift_id INTEGER NOT NULL,
                town_id INTEGER NOT NULL,
                PRIMARY KEY(shift_id, town_id),
                FOREIGN KEY(shift_id) REFERENCES shifts(id) ON DELETE CASCADE,
                FOREIGN KEY(town_id) REFERENCES towns(id)
            );
            CREATE TABLE IF NOT EXISTS vehicle_issue_status_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                changed_by INTEGER NOT NULL,
                changed_at TEXT NOT NULL,
                note TEXT DEFAULT '',
                FOREIGN KEY(issue_id) REFERENCES vehicle_issues(id),
                FOREIGN KEY(changed_by) REFERENCES users(id)
            );
            CREATE TABLE IF NOT EXISTS inspection_sheets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL DEFAULT 1,
                user_id INTEGER NOT NULL,
                sheet_date TEXT NOT NULL,
                delivery_date TEXT DEFAULT '',
                file_path TEXT NOT NULL,
                original_filename TEXT DEFAULT '',
                content_type TEXT DEFAULT '',
                ocr_status TEXT NOT NULL DEFAULT '未処理',
                ocr_text TEXT DEFAULT '',
                ocr_completed INTEGER NOT NULL DEFAULT 0,
                ocr_pickup INTEGER NOT NULL DEFAULT 0,
                ocr_acceptance INTEGER NOT NULL DEFAULT 0,
                ocr_received INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id),
                UNIQUE(user_id, sheet_date)
            );
            CREATE TABLE IF NOT EXISTS inspection_slips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL DEFAULT 1,
                user_id INTEGER NOT NULL,
                delivery_id INTEGER NOT NULL,
                slip_date TEXT NOT NULL,
                file_path TEXT NOT NULL,
                original_filename TEXT DEFAULT '',
                content_type TEXT DEFAULT '',
                uploaded_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(delivery_id) REFERENCES deliveries(id),
                UNIQUE(user_id, slip_date)
            );
            CREATE TABLE IF NOT EXISTS company_features (
                company_id INTEGER NOT NULL,
                feature_key TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(company_id, feature_key),
                FOREIGN KEY(company_id) REFERENCES companies(id)
            );
            """
        )
        ensure_column(conn, "companies", "code", "TEXT DEFAULT ''")
        ensure_column(conn, "companies", "active", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "companies", "created_at", "TEXT DEFAULT ''")
        ensure_column(conn, "users", "phone", "TEXT DEFAULT ''")
        ensure_column(conn, "users", "company_id", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "users", "vehicle", "TEXT DEFAULT ''")
        ensure_column(conn, "users", "active", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "users", "purged", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "users", "deleted_at", "TEXT DEFAULT ''")
        ensure_column(conn, "users", "created_at", "TEXT DEFAULT ''")
        ensure_column(conn, "rates", "delivery_unit", "INTEGER NOT NULL DEFAULT 180")
        ensure_column(conn, "rates", "company_id", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "rates", "transfer_unit", "INTEGER NOT NULL DEFAULT 80")
        ensure_column(conn, "rates", "night_unit", "INTEGER NOT NULL DEFAULT 120")
        ensure_column(conn, "rates", "pickup_unit", "INTEGER NOT NULL DEFAULT 100")
        ensure_column(conn, "rates", "large_unit", "INTEGER NOT NULL DEFAULT 150")
        ensure_column(conn, "rates", "vehicle_rental_type", "TEXT NOT NULL DEFAULT 'none'")
        ensure_column(conn, "rates", "vehicle_daily_fee", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "rates", "vehicle_monthly_fee", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "work_logs", "alcohol_result", "TEXT DEFAULT ''")
        ensure_column(conn, "work_logs", "company_id", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "work_logs", "detector_used", "TEXT DEFAULT ''")
        ensure_column(conn, "work_logs", "intoxicated", "TEXT DEFAULT ''")
        ensure_column(conn, "work_logs", "health_status", "TEXT DEFAULT ''")
        ensure_column(conn, "work_logs", "face_check", "TEXT DEFAULT ''")
        ensure_column(conn, "work_logs", "breath_check", "TEXT DEFAULT ''")
        ensure_column(conn, "work_logs", "voice_check", "TEXT DEFAULT ''")
        ensure_column(conn, "work_logs", "admin_confirm", "TEXT DEFAULT ''")
        ensure_column(conn, "work_logs", "notes", "TEXT DEFAULT ''")
        ensure_column(conn, "work_logs", "created_at", "TEXT DEFAULT ''")
        ensure_column(conn, "deliveries", "completed", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "deliveries", "company_id", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "deliveries", "transfer", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "deliveries", "night", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "deliveries", "pickup", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "deliveries", "large", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "deliveries", "vehicle_rental", "TEXT NOT NULL DEFAULT 'none'")
        ensure_column(conn, "deliveries", "memo", "TEXT DEFAULT ''")
        ensure_column(conn, "deliveries", "inspection_sheet_path", "TEXT DEFAULT ''")
        ensure_column(conn, "deliveries", "created_at", "TEXT DEFAULT ''")
        ensure_column(conn, "deliveries", "updated_at", "TEXT DEFAULT ''")
        ensure_column(conn, "shifts", "start_time", "TEXT DEFAULT ''")
        ensure_column(conn, "shifts", "company_id", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "shifts", "end_time", "TEXT DEFAULT ''")
        ensure_column(conn, "shifts", "note", "TEXT DEFAULT ''")
        ensure_column(conn, "shifts", "district_id", "INTEGER")
        ensure_column(conn, "shifts", "town_id", "INTEGER")
        ensure_column(conn, "shifts", "area_label", "TEXT DEFAULT ''")
        ensure_column(conn, "shifts", "decided", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "shifts", "updated_at", "TEXT DEFAULT ''")
        ensure_column(conn, "inspection_sheets", "file_path", "TEXT DEFAULT ''")
        ensure_column(conn, "inspection_sheets", "company_id", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "inspection_sheets", "delivery_date", "TEXT DEFAULT ''")
        ensure_column(conn, "inspection_sheets", "original_filename", "TEXT DEFAULT ''")
        ensure_column(conn, "inspection_sheets", "content_type", "TEXT DEFAULT ''")
        ensure_column(conn, "inspection_sheets", "ocr_status", "TEXT DEFAULT '未処理'")
        ensure_column(conn, "inspection_sheets", "ocr_text", "TEXT DEFAULT ''")
        ensure_column(conn, "inspection_sheets", "ocr_completed", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "inspection_sheets", "ocr_pickup", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "inspection_sheets", "ocr_acceptance", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "inspection_sheets", "ocr_received", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "inspection_sheets", "created_at", "TEXT DEFAULT ''")
        ensure_column(conn, "inspection_slips", "file_path", "TEXT DEFAULT ''")
        ensure_column(conn, "inspection_slips", "company_id", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "inspection_slips", "original_filename", "TEXT DEFAULT ''")
        ensure_column(conn, "inspection_slips", "content_type", "TEXT DEFAULT ''")
        ensure_column(conn, "inspection_slips", "uploaded_at", "TEXT DEFAULT ''")
        ensure_column(conn, "vehicle_issues", "status", "TEXT NOT NULL DEFAULT '未対応'")
        ensure_column(conn, "vehicle_issues", "company_id", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "holiday_requests", "company_id", "INTEGER NOT NULL DEFAULT 1")
        conn.execute("INSERT OR IGNORE INTO companies(id, name, created_at) VALUES (1, ?, ?)", ("SPARKLE DRIVE", datetime.now().isoformat(timespec="seconds")))
        for company in conn.execute("SELECT id FROM companies").fetchall():
            seed_company_features(conn, company["id"], 1)
        admin = conn.execute("SELECT id FROM users WHERE username = 'admin'").fetchone()
        if not admin:
            now = datetime.now().isoformat(timespec="seconds")
            cur = conn.execute(
                "INSERT INTO users(name, username, password_hash, role, phone, vehicle, created_at) VALUES (?,?,?,?,?,?,?)",
                ("管理者", "admin", hash_password("admin123"), "admin", "", "", now),
            )
            conn.execute("INSERT OR IGNORE INTO rates(user_id) VALUES (?)", (cur.lastrowid,))
            samples = [
                ("佐藤 花子", "sato", "member123", "070-1111-2222", "軽バン 12-34"),
                ("鈴木 一郎", "suzuki", "member123", "070-3333-4444", "軽バン 56-78"),
            ]
            for name, username, password, phone, vehicle in samples:
                cur = conn.execute(
                    "INSERT INTO users(name, username, password_hash, role, phone, vehicle, created_at) VALUES (?,?,?,?,?,?,?)",
                    (name, username, hash_password(password), "member", phone, vehicle, now),
                )
                conn.execute("INSERT OR IGNORE INTO rates(user_id) VALUES (?)", (cur.lastrowid,))
        conn.execute("INSERT OR IGNORE INTO vehicle_rates(id, daily_fee, monthly_fee) VALUES (1, 1500, 30000)")
        now = datetime.now().isoformat(timespec="seconds")
        cur = conn.execute("INSERT OR IGNORE INTO depots(name, created_at) VALUES (?, ?)", ("未設定拠点", now))
        depot = conn.execute("SELECT id FROM depots WHERE name = ?", ("未設定拠点",)).fetchone()
        seed_areas = {
            "高松": ["高松1丁目", "高松2丁目", "高松3丁目", "高松4丁目", "高松5丁目", "高松6丁目"],
            "土支田": ["土支田2丁目", "土支田3丁目"],
        }
        for district_name, town_names in seed_areas.items():
            conn.execute("INSERT OR IGNORE INTO districts(depot_id, name, created_at) VALUES (?, ?, ?)", (depot["id"], district_name, now))
            district = conn.execute("SELECT id FROM districts WHERE depot_id = ? AND name = ?", (depot["id"], district_name)).fetchone()
            for town_name in town_names:
                conn.execute("INSERT OR IGNORE INTO towns(district_id, name, created_at) VALUES (?, ?, ?)", (district["id"], town_name, now))
        conn.commit()
    ensure_vehicle_issue_schema()


def init_db():
    global DB_INIT_DONE, DB_INIT_ERROR, DB_INIT_LAST_ATTEMPT
    if DB_INIT_DONE:
        return
    with DB_INIT_LOCK:
        if DB_INIT_DONE:
            return
        DB_INIT_LAST_ATTEMPT = monotonic()
        try:
            _init_db_schema()
        except Exception as exc:
            DB_INIT_ERROR = exc
            raise
        else:
            DB_INIT_DONE = True
            DB_INIT_ERROR = None


def safe_startup_init_db():
    try:
        init_db()
    except Exception:
        logger.exception("Database initialization failed; continuing startup so Render can bind the port.")


@app.on_event("startup")
def startup():
    if USE_POSTGRES:
        Thread(target=safe_startup_init_db, daemon=True).start()
    else:
        safe_startup_init_db()


@app.get("/healthz")
def healthz():
    return {
        "ok": True,
        "database": "postgresql" if USE_POSTGRES else "sqlite",
        "db_initialized": DB_INIT_DONE,
        "db_init_error": DB_INIT_ERROR.__class__.__name__ if DB_INIT_ERROR else "",
    }


@app.get("/health")
def health():
    return healthz()


@app.get("/readyz")
def readyz():
    return healthz()


def current_user(request: Request):
    cached = getattr(request.state, "user", None)
    if cached is not None:
        return cached
    raw = request.cookies.get("session")
    if not raw:
        return None
    try:
        user_id = serializer.loads(raw)
    except BadSignature:
        return None
    user = query_one(
        """SELECT u.*, c.name AS company_name, c.code AS company_code, c.active AS company_active
           FROM users u
           LEFT JOIN companies c ON c.id=u.company_id
           WHERE u.id = ? AND u.active = 1""",
        (user_id,),
    )
    if user and row_value(user, "company_active", 1) == 0:
        user = None
    request.state.user = user
    return user


def require_user(request: Request):
    user = current_user(request)
    if not user:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    return user


def require_admin(user=Depends(require_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="管理者だけが利用できます")
    return user


@app.get("/inspection-file", response_class=HTMLResponse)
def inspection_file(request: Request, path: str = "", user=Depends(require_user)):
    if is_remote_image_path(path):
        return RedirectResponse(path, status_code=303)
    if local_image_exists(path):
        return RedirectResponse(path, status_code=303)
    return render(
        request,
        "image_missing.html",
        {
            "path": path,
            "message": "画像ファイルが見つかりません。再アップロードしてください。",
        },
    )


def render(request: Request, name: str, context: dict):
    user = current_user(request)
    features = load_company_features(company_id_for(user)) if user else {feature["key"]: True for feature in FEATURE_CATALOG}
    context.update(
        {
            "request": request,
            "user": user,
            "today": app_today().isoformat(),
            "app_name": APP_NAME,
            "company_name": company_name_for(user) if user else APP_NAME,
            "features": features,
            "feature_enabled": lambda key: is_feature_enabled(features, key),
            "can_manage_companies": can_manage_companies(user) if user else False,
        }
    )
    return templates.TemplateResponse(name, context)


def month_bounds(ym: Optional[str]):
    if not ym:
        base = app_today().replace(day=1)
    else:
        base = datetime.strptime(ym, "%Y-%m").date().replace(day=1)
    if base.month == 12:
        next_month = base.replace(year=base.year + 1, month=1)
    else:
        next_month = base.replace(month=base.month + 1)
    return base, next_month - timedelta(days=1)


def calendar_days_for_month(start: date):
    _, last_day = calendar.monthrange(start.year, start.month)
    return [start.replace(day=day) for day in range(1, last_day + 1)]


def active_towns():
    return query_all(
        """SELECT t.id, t.name, d.id AS district_id, d.name AS district_name, p.name AS depot_name
           FROM towns t
           JOIN districts d ON d.id = t.district_id
           LEFT JOIN depots p ON p.id = d.depot_id
           WHERE t.active=1 AND d.active=1
           ORDER BY p.name, d.name, t.name"""
    )


def attach_shift_areas(shifts):
    items = [dict(row) for row in shifts]
    if not items:
        return []
    shift_ids = [item["id"] for item in items]
    placeholders = ",".join("?" for _ in shift_ids)
    area_rows = query_all(
        f"""SELECT st.shift_id, d.name AS district_name, t.name AS town_name
            FROM shift_towns st
            JOIN towns t ON t.id = st.town_id
            JOIN districts d ON d.id = t.district_id
            WHERE st.shift_id IN ({placeholders})
            ORDER BY d.name, t.name""",
        shift_ids,
    )
    area_map = {}
    short_map = {}
    for area in area_rows:
        area_map.setdefault(area["shift_id"], []).append(area["town_name"])
        short_map.setdefault(area["shift_id"], []).append((area["district_name"], area["town_name"]))
    for item in items:
        label_pairs = parse_area_label(item.get("area_label", ""))
        if label_pairs:
            areas = [town_name for _, town_name in label_pairs]
        else:
            areas = area_map.get(item["id"], [])
        if item["status"] == "休み":
            areas = []
        item["areas"] = areas
        item["area_text"] = "、".join(areas)
        short_pairs = label_pairs or short_map.get(item["id"], [])
        item["area_short_text"] = shorten_areas(short_pairs) if item["status"] != "休み" else ""
    return items


def parse_area_label(area_label):
    pairs = []
    for part in (area_label or "").split("・"):
        if "/" not in part:
            continue
        district_name, town_name = [value.strip() for value in part.split("/", 1)]
        if district_name and town_name:
            pairs.append((district_name, town_name))
    return pairs


def shorten_areas(area_pairs):
    grouped = {}
    for district_name, town_name in area_pairs:
        prefix = district_name[:1]
        match = re.search(r"(\d+|[０-９]+)\s*丁目", town_name)
        number = match.group(1).translate(str.maketrans("０１２３４５６７８９", "0123456789")) if match else town_name
        grouped.setdefault(prefix, []).append(number)
    parts = []
    for prefix, numbers in grouped.items():
        unique_numbers = []
        for number in numbers:
            if number not in unique_numbers:
                unique_numbers.append(number)
        parts.append(prefix + ".".join(unique_numbers))
    return " / ".join(parts)


def normalize_text(value):
    if value is None:
        return ""
    return unicodedata.normalize("NFKC", str(value)).strip()


def compact_area_key(value):
    text = normalize_text(value).translate(FULLWIDTH_DIGITS)
    text = re.sub(r"\s+", "", text)
    return text.replace("丁目", "")


def normalize_import_date(value):
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = normalize_text(value).translate(FULLWIDTH_DIGITS)
    if not text:
        return ""
    text = text.replace("年", "-").replace("月", "-").replace("日", "")
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    if re.fullmatch(r"\d{8}", text):
        try:
            return datetime.strptime(text, "%Y%m%d").date().isoformat()
        except ValueError:
            pass
    return text


def normalize_shift_status(status_value, bin_value=""):
    status = normalize_text(status_value)
    bin_label = normalize_text(bin_value)
    aliases = {"休": "休み", "休日": "休み"}
    status = aliases.get(status, status)
    if status in {"1", "2", "3"}:
        status = f"{status}便"
    if bin_label in {"1", "2", "3"}:
        bin_label = f"{bin_label}便"
    if status in {"", "出勤"} and bin_label in {"1便", "2便", "3便"}:
        return bin_label
    return status


def resolve_import_member(member_name, members):
    name = normalize_text(member_name)
    if not name:
        return None, "名前が空です"
    exact = [member for member in members if normalize_text(member["name"]) == name]
    if len(exact) == 1:
        return exact[0], ""
    partial = [member for member in members if name in normalize_text(member["name"])]
    if len(partial) == 1:
        return partial[0], ""
    if len(partial) > 1:
        return None, f"名前「{name}」に一致するメンバーが複数います"
    return None, f"メンバー「{name}」が見つかりません"


def town_number(town_name):
    match = re.search(r"(\d+)\s*丁目?", normalize_text(town_name).translate(FULLWIDTH_DIGITS))
    return match.group(1) if match else ""


def match_area_town_ids(area_text, towns):
    text = normalize_text(area_text)
    if not text:
        return []
    compact_text = compact_area_key(text)
    matched_ids = []

    def add_town(town_id):
        if town_id not in matched_ids:
            matched_ids.append(town_id)

    for town in towns:
        name_key = compact_area_key(town["name"])
        district_key = compact_area_key(town["district_name"])
        full_key = compact_area_key(f"{town['district_name']}{town['name']}")
        if name_key and (name_key in compact_text or full_key in compact_text):
            add_town(town["id"])
        elif district_key and compact_text == district_key:
            add_town(town["id"])

    normalized = normalize_text(text).translate(FULLWIDTH_DIGITS)
    for match in re.finditer(r"([^\d,、/／\s・･]+)(\d+(?:[・･,、/／\s]+\d+)*)", normalized):
        prefix = compact_area_key(match.group(1))
        numbers = re.findall(r"\d+", match.group(2))
        for number in numbers:
            for town in towns:
                if town_number(town["name"]) != number:
                    continue
                district_key = compact_area_key(town["district_name"])
                name_key = compact_area_key(town["name"])
                if prefix and (
                    district_key.startswith(prefix)
                    or name_key.startswith(prefix)
                    or prefix in district_key
                    or prefix in name_key
                ):
                    add_town(town["id"])

    tokens = [compact_area_key(token) for token in re.split(r"[、,/／\s]+", text) if compact_area_key(token)]
    for token in tokens:
        for town in towns:
            if token in {compact_area_key(town["name"]), compact_area_key(f"{town['district_name']}{town['name']}")}:
                add_town(town["id"])
    return matched_ids


def area_preview(town_ids, towns_by_id):
    labels = []
    for town_id in town_ids:
        town = towns_by_id.get(int(town_id))
        if town:
            labels.append(f"{town['district_name']} / {town['name']}")
    return "、".join(labels)


def load_csv_shift_rows(data):
    text = ""
    for encoding in ("utf-8-sig", "cp932"):
        try:
            text = data.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if not text:
        raise HTTPException(status_code=400, detail="CSVの文字コードを読み取れませんでした")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="ヘッダー行が見つかりません")
    reader.fieldnames = [normalize_text(header) for header in reader.fieldnames]
    rows = []
    for row in reader:
        normalized = {normalize_text(key): value for key, value in row.items() if key}
        if any(normalize_text(value) for value in normalized.values()):
            rows.append(normalized)
    return reader.fieldnames, rows


def load_excel_shift_rows(data):
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise HTTPException(status_code=500, detail="Excel取り込みにはopenpyxlが必要です") from exc
    workbook = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    sheet = workbook.active
    rows_iter = sheet.iter_rows(values_only=True)
    try:
        headers = [normalize_text(value) for value in next(rows_iter)]
    except StopIteration as exc:
        raise HTTPException(status_code=400, detail="Excelにデータがありません") from exc
    rows = []
    for values in rows_iter:
        row = {headers[index]: values[index] if index < len(values) else "" for index in range(len(headers)) if headers[index]}
        if any(normalize_text(value) for value in row.values()):
            rows.append(row)
    return headers, rows


async def load_shift_import_rows(file: UploadFile):
    filename = file.filename or ""
    suffix = Path(filename).suffix.lower()
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="ファイルが空です")
    if len(data) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="ファイルサイズは5MB以内にしてください")
    if suffix == ".csv":
        headers, rows = load_csv_shift_rows(data)
    elif suffix in {".xlsx", ".xlsm"}:
        headers, rows = load_excel_shift_rows(data)
    elif suffix == ".xls":
        raise HTTPException(status_code=400, detail="Excelは.xlsx形式で保存してアップロードしてください")
    else:
        raise HTTPException(status_code=400, detail="CSVまたはExcel(.xlsx)をアップロードしてください")

    missing = [header for header in SHIFT_IMPORT_HEADERS if header not in headers]
    if missing:
        raise HTTPException(status_code=400, detail="不足している列: " + "、".join(missing))
    if not rows:
        raise HTTPException(status_code=400, detail="登録できる行がありません")
    if len(rows) > SHIFT_IMPORT_LIMIT:
        raise HTTPException(status_code=400, detail=f"一度に取り込めるシフトは{SHIFT_IMPORT_LIMIT}件までです")
    return rows


def current_shift_conflicts(rows, company_id=1):
    member_ids = sorted({int(row["member_id"]) for row in rows if row.get("member_id")})
    dates = sorted({row["shift_date"] for row in rows if row.get("shift_date")})
    if not member_ids or not dates:
        return set()
    placeholders = ",".join("?" for _ in member_ids)
    existing = query_all(
        f"""SELECT user_id, shift_date FROM shifts
            WHERE company_id=? AND shift_date BETWEEN ? AND ? AND user_id IN ({placeholders})""",
        [company_id, dates[0], dates[-1]] + member_ids,
    )
    return {(int(row["user_id"]), row["shift_date"]) for row in existing}


def build_shift_import_preview(raw_rows, company_id=1):
    members = query_all("SELECT id, name FROM users WHERE role='member' AND active=1 AND COALESCE(purged,0)=0 AND company_id=? ORDER BY name LIMIT 200", (company_id,))
    towns = active_towns()
    towns_by_id = {int(town["id"]): town for town in towns}
    preview_rows = []
    for index, raw in enumerate(raw_rows, start=2):
        errors = []
        shift_date = normalize_import_date(raw.get("日付"))
        if not shift_date:
            errors.append("日付が空です")
        else:
            try:
                datetime.strptime(shift_date, "%Y-%m-%d")
            except ValueError:
                errors.append("日付はYYYY-MM-DD形式で入力してください")

        member, member_error = resolve_import_member(raw.get("名前"), members)
        if member_error:
            errors.append(member_error)

        status = normalize_shift_status(raw.get("出勤区分"), raw.get("便"))
        if status not in SHIFT_ALLOWED_STATUSES:
            errors.append("出勤区分は「出勤、1便、2便、3便、休み」のいずれかにしてください")

        area_text = normalize_text(raw.get("配達エリア"))
        town_ids = []
        if status != "休み":
            town_ids = match_area_town_ids(area_text, towns)
            if not town_ids:
                errors.append("配達エリアが見つかりません")

        preview_rows.append(
            {
                "row_number": index,
                "shift_date": shift_date,
                "member_id": member["id"] if member else None,
                "member_name": normalize_text(raw.get("名前")),
                "resolved_member_name": member["name"] if member else "",
                "status": status,
                "bin_label": normalize_text(raw.get("便")),
                "area_text": area_text,
                "town_ids": town_ids,
                "area_preview": area_preview(town_ids, towns_by_id),
                "note": normalize_text(raw.get("備考")),
                "errors": errors,
                "conflict": False,
                "company_id": company_id,
            }
        )

    valid_rows = [row for row in preview_rows if not row["errors"]]
    conflicts = current_shift_conflicts(valid_rows, company_id)
    for row in preview_rows:
        if row["member_id"] and row["shift_date"] and (int(row["member_id"]), row["shift_date"]) in conflicts:
            row["conflict"] = True
    payload_rows = [{key: value for key, value in row.items() if key != "errors"} for row in valid_rows]
    return {
        "rows": preview_rows,
        "payload": json.dumps(payload_rows, ensure_ascii=False),
        "has_errors": any(row["errors"] for row in preview_rows),
        "has_conflicts": any(row["conflict"] for row in preview_rows),
    }


def get_town_rows_by_ids(town_ids):
    town_ids = [int(town_id) for town_id in town_ids]
    if not town_ids:
        return []
    placeholders = ",".join("?" for _ in town_ids)
    return query_all(
        f"""SELECT t.id, t.name, d.id AS district_id, d.name AS district_name
            FROM towns t JOIN districts d ON d.id=t.district_id
            WHERE t.id IN ({placeholders}) AND t.active=1 AND d.active=1
            ORDER BY d.name, t.name""",
        town_ids,
    )


def upsert_shift_with_towns(conn, member_id, shift_date, status, town_rows, note="", company_id=1):
    first_town = town_rows[0] if town_rows else None
    area_label = "・".join(f"{town['district_name']} / {town['name']}" for town in town_rows)
    conn.execute(
        """INSERT INTO shifts(company_id, user_id, shift_date, status, start_time, end_time, note, district_id, town_id, area_label, decided, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(user_id, shift_date) DO UPDATE SET status=excluded.status, start_time=excluded.start_time,
           end_time=excluded.end_time, note=excluded.note, district_id=excluded.district_id, town_id=excluded.town_id,
           area_label=excluded.area_label, decided=excluded.decided, updated_at=excluded.updated_at, company_id=excluded.company_id""",
        (
            company_id,
            member_id,
            shift_date,
            status,
            "",
            "",
            note or "",
            first_town["district_id"] if first_town else None,
            first_town["id"] if first_town else None,
            area_label,
            1,
            app_now().isoformat(timespec="seconds"),
        ),
    )
    shift = conn.execute("SELECT id FROM shifts WHERE user_id=? AND shift_date=? AND company_id=?", (member_id, shift_date, company_id)).fetchone()
    if status != "休み":
        for town in town_rows:
            conn.execute("INSERT OR IGNORE INTO shift_towns(shift_id, town_id) VALUES (?, ?)", (shift["id"], town["id"]))


def upsert_shift_record(member_id, shift_date, status, town_ids, note="", company_id=1):
    if status not in SHIFT_ALLOWED_STATUSES:
        raise HTTPException(status_code=400, detail="出勤区分が正しくありません")
    if status != "休み" and not town_ids:
        raise HTTPException(status_code=400, detail="休み以外は町会・配達エリアを1つ以上選択してください")
    town_rows = get_town_rows_by_ids(town_ids)
    if status != "休み" and len(town_rows) != len(set(int(town_id) for town_id in town_ids)):
        raise HTTPException(status_code=400, detail="選択された町会・配達エリアが正しくありません")
    with db() as conn:
        upsert_shift_with_towns(conn, member_id, shift_date, status, town_rows, note, company_id)
        conn.commit()


def shifts_by_date(shifts):
    grouped = {}
    for shift in shifts:
        grouped.setdefault(shift["shift_date"], []).append(shift)
    return grouped


def calc_reward(row, rates, vehicle_rates=None, monthly_mode=False):
    if not row or not rates:
        return 0
    reward = (
        row["completed"] * rates["delivery_unit"]
        + row["transfer"] * rates["transfer_unit"]
        + row["night"] * rates["night_unit"]
        + row["pickup"] * rates["pickup_unit"]
        + row["large"] * rates["large_unit"]
    )
    rental_type = rates["vehicle_rental_type"] if "vehicle_rental_type" in rates.keys() else "none"
    if rental_type == "daily":
        reward -= rates["vehicle_daily_fee"]
    if monthly_mode and rental_type == "monthly":
        reward -= rates["vehicle_monthly_fee"]
    return reward


def calc_reward_gross(row, rates):
    if not row or not rates:
        return 0
    return (
        row["completed"] * rates["delivery_unit"]
        + row["transfer"] * rates["transfer_unit"]
        + row["night"] * rates["night_unit"]
        + row["pickup"] * rates["pickup_unit"]
        + row["large"] * rates["large_unit"]
    )


def monthly_reward_summary(user_id: int, target_month: date, company_id: Optional[int] = None):
    start = target_month.replace(day=1)
    if start.month == 12:
        next_month = start.replace(year=start.year + 1, month=1)
    else:
        next_month = start.replace(month=start.month + 1)
    end = next_month - timedelta(days=1)
    company_filter = ""
    params = [user_id, start.isoformat(), end.isoformat()]
    if company_id is not None:
        company_filter = " AND company_id=?"
        params.append(company_id)
    summary = query_one(
        f"""SELECT
              COALESCE(SUM(completed), 0) AS completed_total,
              COALESCE(SUM(transfer), 0) AS transfer_total,
              COALESCE(SUM(night), 0) AS night_total,
              COALESCE(SUM(pickup), 0) AS pickup_total,
              COALESCE(SUM(large), 0) AS large_total,
              COUNT(*) AS delivery_days
           FROM deliveries
           WHERE user_id=? AND work_date BETWEEN ? AND ?{company_filter}""",
        params,
    )
    if company_id is not None:
        rates = query_one("SELECT * FROM rates WHERE user_id=? AND company_id=?", (user_id, company_id))
    else:
        rates = query_one("SELECT * FROM rates WHERE user_id=?", (user_id,))
    if not rates:
        return {"gross_total": 0, "net_total": 0, "vehicle_deduction": 0, "completed_total": 0, "rows": []}
    gross_total = (
        summary["completed_total"] * rates["delivery_unit"]
        + summary["transfer_total"] * rates["transfer_unit"]
        + summary["night_total"] * rates["night_unit"]
        + summary["pickup_total"] * rates["pickup_unit"]
        + summary["large_total"] * rates["large_unit"]
    )
    rental_type = rates["vehicle_rental_type"] if rates and "vehicle_rental_type" in rates.keys() else "none"
    vehicle_deduction = 0
    if rental_type == "daily":
        vehicle_deduction = summary["delivery_days"] * rates["vehicle_daily_fee"]
    elif rental_type == "monthly" and summary["delivery_days"]:
        vehicle_deduction = rates["vehicle_monthly_fee"]
    net_total = gross_total - vehicle_deduction
    return {
        "gross_total": gross_total,
        "net_total": net_total,
        "vehicle_deduction": vehicle_deduction,
        "completed_total": summary["completed_total"],
        "rows": [],
    }


def upcoming_member_shifts(user_id: int, today_s: str, company_id: Optional[int] = None):
    company_filter = ""
    params = [user_id, today_s]
    if company_id is not None:
        company_filter = " AND s.company_id=?"
        params.append(company_id)
    rows = query_all(
        f"""SELECT s.*, d.name AS district_name, t.name AS town_name
           FROM shifts s
           LEFT JOIN districts d ON d.id=s.district_id
           LEFT JOIN towns t ON t.id=s.town_id
           WHERE s.user_id=? AND s.shift_date>=? AND s.decided=1
           {company_filter}
           ORDER BY s.shift_date LIMIT 3""",
        params,
    )
    return attach_shift_areas(rows)


def extract_delivery_counts(ocr_text: str = ""):
    fields = extract_inspection_fields(ocr_text)
    counts = {
        "completed": fields["completed"],
        "transfer": _count_after_labels(ocr_text, ["転送"]),
        "night": _count_after_labels(ocr_text, ["夜間"]),
        "pickup": fields["pickup"],
        "large": _count_after_labels(ocr_text, ["大型", "大口"]),
    }
    return counts


def normalize_ocr_text(ocr_text: str = ""):
    text = unicodedata.normalize("NFKC", ocr_text or "")
    text = text.translate(FULLWIDTH_DIGITS)
    replacements = {
        "配達完了": "配達完了",
        "配達完ア": "配達完了",
        "配達完丁": "配達完了",
        "引受け": "引受",
        "引き受け": "引受",
        "引 受": "引受",
        "受 入": "受入",
    }
    for before, after in replacements.items():
        text = text.replace(before, after)
    return text.replace(",", "")


def parse_inspection_date(ocr_text: str = "", fallback_date: str = ""):
    text = normalize_ocr_text(ocr_text)
    patterns = [
        r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
        r"(\d{4})[-/\.](\d{1,2})[-/\.](\d{1,2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        year, month, day = [int(value) for value in match.groups()]
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            pass
    return normalize_import_date(fallback_date) if fallback_date else ""


def _numbers_after_label(line: str, label: str):
    match = re.search(rf"{re.escape(label)}[^\d]{{0,18}}(\d+)", line)
    if match:
        return int(match.group(1))
    return None


def _count_after_labels(ocr_text: str = "", labels=None):
    labels = labels or []
    text = normalize_ocr_text(ocr_text)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for label in labels:
        for line in lines:
            value = _numbers_after_label(line, label)
            if value is not None:
                return value
    compact = re.sub(r"\s+", "", text)
    for label in labels:
        value = _numbers_after_label(compact, label)
        if value is not None:
            return value
    return 0


def extract_inspection_fields(ocr_text: str = "", fallback_date: str = ""):
    completed = _count_after_labels(ocr_text, ["配達完了", "配完", "完了"])
    acceptance = _count_after_labels(ocr_text, ["引受", "集荷"])
    received = _count_after_labels(ocr_text, ["受入"])
    return {
        "work_date": parse_inspection_date(ocr_text, fallback_date),
        "completed": completed,
        "pickup": acceptance,
        "acceptance": acceptance,
        "received": received,
    }


def extract_ocr_text(image_path: Path):
    try:
        from PIL import Image
        from PIL import ImageEnhance, ImageFilter, ImageOps
        import pytesseract

        lang = os.getenv("OCR_LANG", "jpn+eng")
        config = os.getenv("OCR_CONFIG", "--psm 6")
        with Image.open(image_path) as image:
            image = ImageOps.exif_transpose(image)
            image.thumbnail((1800, 2600))
            variants = [image]
            gray = ImageOps.grayscale(image)
            gray = ImageOps.autocontrast(gray)
            gray = ImageEnhance.Contrast(gray).enhance(1.8)
            if max(gray.size) < 2200:
                gray = gray.resize((gray.width * 2, gray.height * 2))
            variants.append(gray.filter(ImageFilter.SHARPEN))
            texts = [pytesseract.image_to_string(item, lang=lang, config=config).strip() for item in variants]
            return max(texts, key=len).strip()
    except Exception:
        return ""


def auto_comment(delivery, reward, has_shift=True):
    if not delivery:
        return "今日の入力をすると、ここに応援コメントが表示されます。"
    if delivery["night"] > 0:
        return "夜間までありがとうございました！帰り道も安全第一でお願いします。"
    if delivery["completed"] >= 120:
        return "今日は配達個数が多かったですね。しっかり休んでください！"
    if reward >= 20000:
        return "今月も順調です。この調子でいきましょう！"
    if not has_shift:
        return "休み希望やシフトも忘れず確認しましょう。"
    return "今日もお疲れ様でした！明日も安全運転でお願いします！"


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    return RedirectResponse("/admin" if user["role"] == "admin" else "/member", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return render(request, "login.html", {"error": ""})


@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    user = query_one(
        """SELECT u.*, c.name AS company_name, c.code AS company_code, c.active AS company_active
           FROM users u
           LEFT JOIN companies c ON c.id=u.company_id
           WHERE u.username = ? AND u.active = 1""",
        (username,),
    )
    if not user or user["password_hash"] != hash_password(password):
        return render(request, "login.html", {"error": "IDまたはパスワードが違います"})
    if row_value(user, "company_active", 1) == 0:
        return render(request, "login.html", {"error": "この会社アカウントは停止中です"})
    res = RedirectResponse("/", status_code=303)
    res.set_cookie("session", serializer.dumps(user["id"]), httponly=True, samesite="lax")
    return res


@app.get("/logout")
def logout():
    res = RedirectResponse("/login", status_code=303)
    res.delete_cookie("session")
    return res


@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(request: Request, user=Depends(require_admin)):
    today_s = app_today().isoformat()
    company_id = company_id_for(user)
    stats = query_one(
        """SELECT
             (SELECT COUNT(*) FROM users WHERE role='member' AND active=1 AND COALESCE(purged,0)=0 AND company_id=?) AS member_count,
             (SELECT COUNT(*) FROM work_logs WHERE work_date=? AND company_id=?) AS log_count,
             (SELECT COUNT(*) FROM deliveries WHERE work_date=? AND company_id=?) AS delivery_count""",
        (company_id, today_s, company_id, today_s, company_id),
    )
    logs = query_all(
        """SELECT w.*, u.name FROM work_logs w JOIN users u ON u.id = w.user_id
           WHERE w.work_date = ? AND w.company_id=? ORDER BY w.logged_at DESC LIMIT 30""",
        (today_s, company_id),
    )
    issues = query_all(
        """SELECT v.*, u.name FROM vehicle_issues v JOIN users u ON u.id = v.user_id
           WHERE v.company_id=? ORDER BY v.created_at DESC LIMIT 5""",
        (company_id,),
    )
    deliveries = query_all(
        """SELECT d.*, u.name, COALESCE(s.file_path, d.inspection_sheet_path) AS slip_path
           FROM deliveries d
           JOIN users u ON u.id = d.user_id
           LEFT JOIN inspection_slips s ON s.delivery_id=d.id
           WHERE d.work_date = ? AND d.company_id=? ORDER BY u.name LIMIT 100""",
        (today_s, company_id),
    )
    deliveries = attach_image_info(deliveries, "slip_path")
    return render(
        request,
        "admin_dashboard.html",
        {
            "member_count": stats["member_count"],
            "log_count": stats["log_count"],
            "delivery_count": stats["delivery_count"],
            "logs": logs,
            "issues": issues,
            "deliveries": deliveries,
        },
    )


@app.get("/admin/members", response_class=HTMLResponse)
def members_page(request: Request, message: str = "", error: str = "", user=Depends(require_admin)):
    require_feature(user, "members")
    company_id = company_id_for(user)
    active_members = query_all("SELECT * FROM users WHERE role = 'member' AND active = 1 AND COALESCE(purged,0)=0 AND company_id=? ORDER BY id LIMIT 200", (company_id,))
    retired_members = query_all("SELECT * FROM users WHERE role = 'member' AND active = 0 AND COALESCE(purged,0)=0 AND company_id=? ORDER BY id LIMIT 200", (company_id,))
    return render(request, "members.html", {"active_members": active_members, "retired_members": retired_members, "message": message, "error": error})


@app.post("/admin/members")
def save_member(
    member_id: str = Form(""),
    name: str = Form(...),
    username: str = Form(...),
    password: str = Form(""),
    phone: str = Form(""),
    vehicle: str = Form(""),
    user=Depends(require_admin),
):
    require_feature(user, "members")
    company_id = company_id_for(user)
    now = app_now().isoformat(timespec="seconds")
    username = username.strip()
    if not username:
        return RedirectResponse(f"/admin/members?error={quote('ログインIDを入力してください')}", status_code=303)
    release_purged_username(username)
    if member_id:
        duplicate = query_one("SELECT id FROM users WHERE username=? AND id<>? AND COALESCE(purged,0)=0", (username, member_id))
        if duplicate:
            return RedirectResponse(f"/admin/members?error={quote('同じログインIDが既に使われています')}", status_code=303)
        if password:
            execute(
                "UPDATE users SET name=?, username=?, password_hash=?, phone=?, vehicle=? WHERE id=? AND role='member' AND company_id=? AND COALESCE(purged,0)=0",
                (name, username, hash_password(password), phone, vehicle, member_id, company_id),
            )
        else:
            execute("UPDATE users SET name=?, username=?, phone=?, vehicle=? WHERE id=? AND role='member' AND company_id=? AND COALESCE(purged,0)=0", (name, username, phone, vehicle, member_id, company_id))
        return RedirectResponse("/admin/members?message=" + quote("メンバー情報を更新しました"), status_code=303)
    existing = query_one("SELECT * FROM users WHERE username=? AND COALESCE(purged,0)=0", (username,))
    if existing and row_value(existing, "role") == "member" and existing["active"] == 1:
        return RedirectResponse(f"/admin/members?error={quote('同じログインIDの在籍中メンバーがいます')}", status_code=303)
    if existing and row_value(existing, "role") == "member" and existing["active"] == 0:
        return RedirectResponse(f"/admin/members?error={quote('同じログインIDの退職済みメンバーがいます。完全削除後に再登録できます')}", status_code=303)
    if existing:
        return RedirectResponse(f"/admin/members?error={quote('同じログインIDが既に使われています')}", status_code=303)
    try:
        cur = execute(
            "INSERT INTO users(company_id, name, username, password_hash, role, phone, vehicle, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (company_id, name, username, hash_password(password or "member123"), "member", phone, vehicle, now),
        )
        execute("INSERT OR IGNORE INTO rates(user_id, company_id) VALUES (?, ?)", (cur.lastrowid, company_id))
    except DB_INTEGRITY_ERROR:
        return RedirectResponse(f"/admin/members?error={quote('ログインIDが重複しています。別のIDを入力してください')}", status_code=303)
    return RedirectResponse("/admin/members?message=" + quote("メンバーを追加しました"), status_code=303)


@app.post("/admin/members/{member_id}/delete")
def delete_member(member_id: int, user=Depends(require_admin)):
    require_feature(user, "members")
    execute("UPDATE users SET active = 0 WHERE id=? AND role='member' AND company_id=?", (member_id, company_id_for(user)))
    return RedirectResponse("/admin/members", status_code=303)


@app.post("/admin/members/{member_id}/purge")
def purge_retired_member(member_id: int, user=Depends(require_admin)):
    require_feature(user, "members")
    company_id = company_id_for(user)
    member = query_one("SELECT * FROM users WHERE id=? AND role='member' AND company_id=?", (member_id, company_id))
    if not member:
        return RedirectResponse(f"/admin/members?error={quote('メンバーが見つかりません')}", status_code=303)
    if member["active"] == 1:
        return RedirectResponse(f"/admin/members?error={quote('在籍中メンバーは完全削除できません。先に退職扱いにしてください')}", status_code=303)
    now_dt = app_now()
    now = now_dt.isoformat(timespec="seconds")
    anonymized_username = deleted_username_for(member_id)
    execute(
        """UPDATE users
           SET name=?, username=?, password_hash=?, phone='', vehicle='', active=0, purged=1, deleted_at=?
           WHERE id=? AND role='member' AND active=0 AND company_id=? AND COALESCE(purged,0)=0""",
        (f"削除済みメンバー{member_id}", anonymized_username, hash_password(anonymized_username), now, member_id, company_id),
    )
    return RedirectResponse("/admin/members?message=" + quote("退職済みメンバーを完全削除しました。過去記録は匿名化された名前で保持されます"), status_code=303)


@app.get("/admin/settings", response_class=HTMLResponse)
def admin_settings_page(
    request: Request,
    message: str = "",
    error: str = "",
    feature_company_id: Optional[int] = None,
    user=Depends(require_admin),
):
    company_id = company_id_for(user)
    can_manage = can_manage_companies(user)
    members = query_all(
        """SELECT id, name, username, active
           FROM users
           WHERE role='member' AND company_id=? AND COALESCE(purged,0)=0
           ORDER BY active DESC, name
           LIMIT 200""",
        (company_id,),
    )
    companies = []
    if can_manage:
        companies = query_all(
            """SELECT c.*,
                      (SELECT username FROM users au
                       WHERE au.company_id=c.id AND au.role='admin' AND au.active=1
                       ORDER BY au.id LIMIT 1) AS admin_username
               FROM companies c
               ORDER BY c.id
               LIMIT 200"""
        )
    feature_companies = companies if can_manage else query_all("SELECT id, name FROM companies WHERE id=?", (company_id,))
    feature_target_id = company_id
    if can_manage and feature_company_id:
        selected_company = query_one("SELECT id FROM companies WHERE id=?", (feature_company_id,))
        if selected_company:
            feature_target_id = feature_company_id
    feature_target_company = query_one("SELECT id, name FROM companies WHERE id=?", (feature_target_id,))
    return render(
        request,
        "admin_settings.html",
        {
            "members": members,
            "companies": companies,
            "can_manage_companies": can_manage,
            "feature_companies": feature_companies,
            "feature_target_company": feature_target_company,
            "feature_target_id": feature_target_id,
            "feature_rows": company_feature_rows(feature_target_id),
            "message": message,
            "error": error,
        },
    )


@app.post("/admin/settings/account")
def update_admin_account(
    username: str = Form(...),
    password: str = Form(""),
    user=Depends(require_admin),
):
    username = username.strip()
    if not username:
        return RedirectResponse("/admin/settings?error=" + quote("ログインIDを入力してください"), status_code=303)
    release_purged_username(username)
    duplicate = query_one("SELECT id FROM users WHERE username=? AND id<>? AND COALESCE(purged,0)=0", (username, user["id"]))
    if duplicate:
        return RedirectResponse("/admin/settings?error=" + quote("同じログインIDが既に使われています"), status_code=303)
    if password:
        execute("UPDATE users SET username=?, password_hash=? WHERE id=? AND role='admin'", (username, hash_password(password), user["id"]))
    else:
        execute("UPDATE users SET username=? WHERE id=? AND role='admin'", (username, user["id"]))
    return RedirectResponse("/admin/settings?message=" + quote("管理者アカウントを更新しました"), status_code=303)


@app.post("/admin/settings/member")
def update_member_login(
    member_id: int = Form(...),
    username: str = Form(...),
    password: str = Form(""),
    user=Depends(require_admin),
):
    require_feature(user, "members")
    company_id = company_id_for(user)
    username = username.strip()
    if not username:
        return RedirectResponse("/admin/settings?error=" + quote("メンバーのログインIDを入力してください"), status_code=303)
    release_purged_username(username)
    member = query_one(
        "SELECT id FROM users WHERE id=? AND role='member' AND company_id=? AND COALESCE(purged,0)=0",
        (member_id, company_id),
    )
    if not member:
        return RedirectResponse("/admin/settings?error=" + quote("メンバーが見つかりません"), status_code=303)
    duplicate = query_one("SELECT id FROM users WHERE username=? AND id<>? AND COALESCE(purged,0)=0", (username, member_id))
    if duplicate:
        return RedirectResponse("/admin/settings?error=" + quote("同じログインIDが既に使われています"), status_code=303)
    if password:
        execute("UPDATE users SET username=?, password_hash=? WHERE id=? AND role='member' AND company_id=?", (username, hash_password(password), member_id, company_id))
    else:
        execute("UPDATE users SET username=? WHERE id=? AND role='member' AND company_id=?", (username, member_id, company_id))
    return RedirectResponse("/admin/settings?message=" + quote("メンバーのログイン情報を更新しました"), status_code=303)


@app.post("/admin/settings/company")
def save_company(
    company_id: str = Form(""),
    name: str = Form(...),
    code: str = Form(...),
    admin_username: str = Form(...),
    admin_password: str = Form(""),
    active: str = Form("1"),
    user=Depends(require_admin),
):
    if not can_manage_companies(user):
        raise HTTPException(status_code=403, detail="会社管理の権限がありません")

    cid = int(company_id) if company_id.strip().isdigit() else 0
    name = name.strip()
    code = code.strip()
    admin_username = admin_username.strip()
    release_purged_username(admin_username)
    active_int = 1 if active == "1" else 0
    admin_name = f"{name} 管理者" if name else "管理者"

    if not name or not code or not admin_username:
        return RedirectResponse("/admin/settings?error=" + quote("会社名、会社コード、管理者ログインIDを入力してください"), status_code=303)
    if cid == company_id_for(user) and active_int == 0:
        return RedirectResponse("/admin/settings?error=" + quote("ログイン中の会社は停止できません"), status_code=303)

    admin = None
    if cid:
        existing_company = query_one("SELECT id FROM companies WHERE id=?", (cid,))
        if not existing_company:
            return RedirectResponse("/admin/settings?error=" + quote("会社が見つかりません"), status_code=303)
        admin = query_one(
            "SELECT id FROM users WHERE company_id=? AND role='admin' ORDER BY id LIMIT 1",
            (cid,),
        )

    if not cid and not admin_password:
        return RedirectResponse("/admin/settings?error=" + quote("新しい会社の管理者パスワードを入力してください"), status_code=303)

    duplicate_code = query_one("SELECT id FROM companies WHERE code=? AND id<>?", (code, cid or 0))
    if duplicate_code:
        return RedirectResponse("/admin/settings?error=" + quote("同じ会社コードが既に使われています"), status_code=303)

    ignore_admin_id = admin["id"] if admin else 0
    duplicate_username = query_one("SELECT id FROM users WHERE username=? AND id<>? AND COALESCE(purged,0)=0", (admin_username, ignore_admin_id))
    if duplicate_username:
        return RedirectResponse("/admin/settings?error=" + quote("同じログインIDが既に使われています"), status_code=303)

    now = app_now().isoformat(timespec="seconds")
    if cid:
        execute("UPDATE companies SET name=?, code=?, active=? WHERE id=?", (name, code, active_int, cid))
        if admin:
            if admin_password:
                execute(
                    "UPDATE users SET name=?, username=?, password_hash=?, active=1 WHERE id=? AND role='admin' AND company_id=?",
                    (admin_name, admin_username, hash_password(admin_password), admin["id"], cid),
                )
            else:
                execute(
                    "UPDATE users SET name=?, username=?, active=1 WHERE id=? AND role='admin' AND company_id=?",
                    (admin_name, admin_username, admin["id"], cid),
                )
        else:
            if not admin_password:
                return RedirectResponse("/admin/settings?error=" + quote("管理者が未登録です。パスワードを入力してください"), status_code=303)
            execute(
                "INSERT INTO users(company_id, name, username, password_hash, role, phone, vehicle, active, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (cid, admin_name, admin_username, hash_password(admin_password), "admin", "", "", 1, now),
            )
        return RedirectResponse("/admin/settings?message=" + quote("会社情報を更新しました"), status_code=303)

    cur = execute(
        "INSERT INTO companies(name, code, active, created_at) VALUES (?,?,?,?)",
        (name, code, active_int, now),
    )
    new_company_id = cur.lastrowid
    execute(
        "INSERT INTO users(company_id, name, username, password_hash, role, phone, vehicle, active, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (new_company_id, admin_name, admin_username, hash_password(admin_password), "admin", "", "", 1, now),
    )
    with db() as conn:
        seed_company_features(conn, new_company_id, 1)
        conn.commit()
    return RedirectResponse("/admin/settings?message=" + quote("会社と管理者を追加しました"), status_code=303)


@app.post("/admin/settings/features")
def update_company_features(
    feature_company_id: int = Form(...),
    features: Optional[List[str]] = Form(None),
    user=Depends(require_admin),
):
    own_company_id = company_id_for(user)
    target_company_id = feature_company_id if can_manage_companies(user) else own_company_id
    if not can_manage_companies(user) and feature_company_id != own_company_id:
        raise HTTPException(status_code=403, detail="他社の機能設定は変更できません")
    company = query_one("SELECT id FROM companies WHERE id=?", (target_company_id,))
    if not company:
        return RedirectResponse("/admin/settings?error=" + quote("会社が見つかりません"), status_code=303)
    selected = set(features or [])
    unknown = selected - FEATURE_KEYS
    if unknown:
        return RedirectResponse("/admin/settings?error=" + quote("不明な機能が含まれています"), status_code=303)
    now = app_now().isoformat(timespec="seconds")
    with db() as conn:
        for feature in FEATURE_CATALOG:
            enabled = 1 if feature["key"] in selected else 0
            conn.execute(
                """INSERT INTO company_features(company_id, feature_key, enabled, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(company_id, feature_key) DO UPDATE SET
                   enabled=excluded.enabled, updated_at=excluded.updated_at""",
                (target_company_id, feature["key"], enabled, now),
            )
        conn.commit()
    return RedirectResponse(
        f"/admin/settings?feature_company_id={target_company_id}&message=" + quote("利用機能・プラン設定を更新しました"),
        status_code=303,
    )


@app.get("/settings", response_class=HTMLResponse)
def member_settings_page(request: Request, message: str = "", error: str = "", user=Depends(require_user)):
    return render(request, "member_settings.html", {"message": message, "error": error})


@app.post("/settings/password")
def update_own_password(
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
    user=Depends(require_user),
):
    if user["password_hash"] != hash_password(current_password):
        return RedirectResponse("/settings?error=" + quote("現在のパスワードが違います"), status_code=303)
    if not new_password:
        return RedirectResponse("/settings?error=" + quote("新しいパスワードを入力してください"), status_code=303)
    if new_password != confirm_password:
        return RedirectResponse("/settings?error=" + quote("新しいパスワードが一致しません"), status_code=303)
    execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(new_password), user["id"]))
    return RedirectResponse("/settings?message=" + quote("パスワードを変更しました"), status_code=303)


@app.get("/admin/rates", response_class=HTMLResponse)
def rates_page(request: Request, user=Depends(require_admin)):
    require_feature(user, "rewards")
    company_id = company_id_for(user)
    rows = query_all(
        """SELECT u.id, u.name, r.* FROM users u LEFT JOIN rates r ON r.user_id = u.id AND r.company_id=?
           WHERE u.role='member' AND u.active=1 AND u.company_id=? ORDER BY u.id LIMIT 200""",
        (company_id, company_id),
    )
    return render(request, "rates.html", {"rows": rows})


@app.post("/admin/rates")
def save_rates(
    user_id: int = Form(...),
    delivery_unit: int = Form(...),
    transfer_unit: int = Form(...),
    night_unit: int = Form(...),
    pickup_unit: int = Form(...),
    large_unit: int = Form(...),
    vehicle_rental_type: str = Form("none"),
    vehicle_daily_fee: int = Form(0),
    vehicle_monthly_fee: int = Form(0),
    user=Depends(require_admin),
):
    require_feature(user, "rewards")
    company_id = company_id_for(user)
    member = query_one("SELECT id FROM users WHERE id=? AND role='member' AND company_id=?", (user_id, company_id))
    if not member:
        raise HTTPException(status_code=404, detail="メンバーが見つかりません")
    execute(
        """INSERT INTO rates(user_id, company_id, delivery_unit, transfer_unit, night_unit, pickup_unit, large_unit,
           vehicle_rental_type, vehicle_daily_fee, vehicle_monthly_fee)
           VALUES (?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(user_id) DO UPDATE SET delivery_unit=excluded.delivery_unit,
           company_id=excluded.company_id,
           transfer_unit=excluded.transfer_unit, night_unit=excluded.night_unit,
           pickup_unit=excluded.pickup_unit, large_unit=excluded.large_unit,
           vehicle_rental_type=excluded.vehicle_rental_type,
           vehicle_daily_fee=excluded.vehicle_daily_fee,
           vehicle_monthly_fee=excluded.vehicle_monthly_fee""",
        (user_id, company_id, delivery_unit, transfer_unit, night_unit, pickup_unit, large_unit, vehicle_rental_type, vehicle_daily_fee, vehicle_monthly_fee),
    )
    return RedirectResponse("/admin/rates", status_code=303)


@app.get("/admin/vehicle-rates", response_class=HTMLResponse)
def vehicle_rates_page(request: Request, user=Depends(require_admin)):
    require_feature(user, "vehicle")
    rates = query_one("SELECT * FROM vehicle_rates WHERE id=1")
    return render(request, "vehicle_rates.html", {"rates": rates})


@app.post("/admin/vehicle-rates")
def save_vehicle_rates(daily_fee: int = Form(...), monthly_fee: int = Form(...), user=Depends(require_admin)):
    require_feature(user, "vehicle")
    execute("UPDATE vehicle_rates SET daily_fee=?, monthly_fee=? WHERE id=1", (daily_fee, monthly_fee))
    return RedirectResponse("/admin/vehicle-rates", status_code=303)


@app.get("/member", response_class=HTMLResponse)
def member_home(request: Request, user=Depends(require_user)):
    if user["role"] == "admin":
        return RedirectResponse("/admin", status_code=303)
    today_s = app_today().isoformat()
    company_id = company_id_for(user)
    delivery = query_one("SELECT * FROM deliveries WHERE user_id=? AND company_id=? AND work_date=?", (user["id"], company_id, today_s))
    rates = query_one("SELECT * FROM rates WHERE user_id=? AND company_id=?", (user["id"], company_id))
    vehicle_rates = query_one("SELECT * FROM vehicle_rates WHERE id=1")
    reward = calc_reward(delivery, rates, vehicle_rates) if delivery else 0
    month_summary = monthly_reward_summary(user["id"], app_today(), company_id)
    shifts = upcoming_member_shifts(user["id"], today_s, company_id)
    today_shift = shifts[0] if shifts and shifts[0]["shift_date"] == today_s else None
    next_shifts = [item for item in shifts if item["shift_date"] != today_s][:2]
    comment = auto_comment(delivery, reward, bool(today_shift))
    return render(
        request,
        "member_home.html",
        {
            "delivery": delivery,
            "reward": reward,
            "comment": comment,
            "shift": today_shift,
            "month_summary": month_summary,
            "today_shift": today_shift,
            "next_shifts": next_shifts,
        },
    )


@app.get("/work-log", response_class=HTMLResponse)
def work_log_page(request: Request, type: str = "start", user=Depends(require_user)):
    require_feature(user, "safety")
    log_type = type if type in {"start", "end"} else "start"
    now = app_now()
    logs = query_all(
        "SELECT * FROM work_logs WHERE user_id=? AND company_id=? ORDER BY logged_at DESC LIMIT 20",
        (user["id"], company_id_for(user)),
    )
    return render(
        request,
        "work_log.html",
        {
            "logs": logs,
            "log_type": log_type,
            "work_date": now.date().isoformat(),
            "logged_at": now.strftime("%Y-%m-%dT%H:%M"),
        },
    )


@app.post("/work-log")
def save_work_log(
    work_date: str = Form(...),
    log_type: str = Form(...),
    logged_at: str = Form(...),
    alcohol_result: str = Form(...),
    detector_used: str = Form(...),
    intoxicated: str = Form(...),
    health_status: str = Form(...),
    face_check: str = Form(...),
    breath_check: str = Form(...),
    voice_check: str = Form(...),
    admin_confirm: str = Form(""),
    notes: str = Form(""),
    user=Depends(require_user),
):
    require_feature(user, "safety")
    execute(
        """INSERT INTO work_logs(company_id, user_id, work_date, log_type, logged_at, alcohol_result, detector_used,
           intoxicated, health_status, face_check, breath_check, voice_check, admin_confirm, notes, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (company_id_for(user), user["id"], work_date, log_type, logged_at, alcohol_result, detector_used, intoxicated, health_status, face_check, breath_check, voice_check, admin_confirm, notes, app_now().isoformat(timespec="seconds")),
    )
    return RedirectResponse("/work-log", status_code=303)


@app.get("/deliveries", response_class=HTMLResponse)
def delivery_page(request: Request, day: Optional[str] = None, user=Depends(require_user)):
    require_feature(user, "deliveries")
    target = day or app_today().isoformat()
    if user["role"] == "admin":
        company_id = company_id_for(user)
        rows = query_all(
            """SELECT d.*, u.name, COALESCE(s.file_path, d.inspection_sheet_path) AS slip_path
               FROM deliveries d
               JOIN users u ON u.id=d.user_id
               LEFT JOIN inspection_slips s ON s.delivery_id=d.id
               WHERE d.work_date=? AND d.company_id=? ORDER BY u.name LIMIT 300""",
            (target, company_id),
        )
        rows = attach_image_info(rows, "slip_path")
        return render(request, "admin_deliveries.html", {"rows": rows, "target": target})
    company_id = company_id_for(user)
    delivery = query_one("SELECT * FROM deliveries WHERE user_id=? AND company_id=? AND work_date=?", (user["id"], company_id, target))
    slip = query_one("SELECT * FROM inspection_slips WHERE user_id=? AND company_id=? AND slip_date=?", (user["id"], company_id, target))
    sheet = query_one("SELECT * FROM inspection_sheets WHERE user_id=? AND company_id=? AND sheet_date=?", (user["id"], company_id, target))
    rates = query_one("SELECT * FROM rates WHERE user_id=? AND company_id=?", (user["id"], company_id))
    vehicle_rates = query_one("SELECT * FROM vehicle_rates WHERE id=1")
    reward = calc_reward(delivery, rates, vehicle_rates) if delivery else 0
    inspection_image_path = ""
    if slip:
        inspection_image_path = slip["file_path"]
    elif delivery and "inspection_sheet_path" in delivery.keys() and delivery["inspection_sheet_path"]:
        inspection_image_path = delivery["inspection_sheet_path"]
    elif sheet:
        inspection_image_path = sheet["file_path"]
    inspection_image = inspection_image_info(inspection_image_path)
    return render(
        request,
        "deliveries.html",
        {
            "delivery": delivery,
            "slip": slip,
            "sheet": sheet,
            "inspection_image_path": inspection_image_path,
            "inspection_image": inspection_image,
            "target": target,
            "reward": reward,
            "comment": auto_comment(delivery, reward),
        },
    )


@app.post("/deliveries")
async def save_delivery(
    work_date: str = Form(...),
    completed: int = Form(0),
    transfer: int = Form(0),
    night: int = Form(0),
    pickup: int = Form(0),
    large: int = Form(0),
    vehicle_rental: str = Form("none"),
    memo: str = Form(""),
    inspection_image: Optional[UploadFile] = File(None),
    user=Depends(require_user),
):
    require_feature(user, "deliveries")
    if user["role"] == "admin":
        raise HTTPException(status_code=403, detail="メンバーとしてログインしてください")
    now_dt = app_now()
    now = now_dt.isoformat(timespec="seconds")
    company_id = company_id_for(user)
    with db() as conn:
        conn.execute(
            """INSERT INTO deliveries(company_id, user_id, work_date, completed, transfer, night, pickup, large, vehicle_rental, memo, inspection_sheet_path, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(user_id, work_date) DO UPDATE SET completed=excluded.completed, transfer=excluded.transfer,
               night=excluded.night, pickup=excluded.pickup, large=excluded.large, vehicle_rental=excluded.vehicle_rental,
               memo=excluded.memo, updated_at=excluded.updated_at, company_id=excluded.company_id""",
            (company_id, user["id"], work_date, completed, transfer, night, pickup, large, vehicle_rental, memo, "", now, now),
        )
        delivery = conn.execute("SELECT id FROM deliveries WHERE user_id=? AND company_id=? AND work_date=?", (user["id"], company_id, work_date)).fetchone()
        conn.commit()
    if inspection_image and inspection_image.filename:
        if not inspection_image.content_type or not inspection_image.content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="点検表は画像ファイルを選択してください")
        suffix = safe_upload_name(inspection_image.filename)
        if suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
            raise HTTPException(status_code=400, detail="点検表画像は jpg, jpeg, png, webp に対応しています")
        SLIP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        data = await inspection_image.read()
        if data:
            max_size = 15 * 1024 * 1024
            if len(data) > max_size:
                data = data[:max_size]
            filename = f"user{user['id']}_{work_date.replace('-', '')}_{now_dt.strftime('%H%M%S%f')}{suffix}"
            public_path, _ = save_inspection_image(data, filename, inspection_image.content_type or "", "inspection_slips", SLIP_UPLOAD_DIR)
            with db() as conn:
                conn.execute(
                    """INSERT INTO inspection_slips(company_id, user_id, delivery_id, slip_date, file_path, original_filename, content_type, uploaded_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(user_id, slip_date) DO UPDATE SET delivery_id=excluded.delivery_id,
                       company_id=excluded.company_id, file_path=excluded.file_path, original_filename=excluded.original_filename,
                       content_type=excluded.content_type, uploaded_at=excluded.uploaded_at""",
                    (company_id, user["id"], delivery["id"], work_date, public_path, inspection_image.filename or "", inspection_image.content_type or "", now),
                )
                conn.execute("UPDATE deliveries SET inspection_sheet_path=?, updated_at=? WHERE id=? AND company_id=?", (public_path, now, delivery["id"], company_id))
                conn.commit()
    return RedirectResponse(f"/deliveries?day={work_date}", status_code=303)


@app.get("/inspection-sheets", response_class=HTMLResponse)
def inspection_sheets_page(request: Request, day: Optional[str] = None, member_id: Optional[int] = None, user=Depends(require_user)):
    require_feature(user, "inspection")
    target = day or app_today().isoformat()
    if user["role"] == "admin":
        company_id = company_id_for(user)
        members = query_all("SELECT id, name FROM users WHERE role='member' AND active=1 AND COALESCE(purged,0)=0 AND company_id=? ORDER BY name LIMIT 200", (company_id,))
        params = [company_id, target, target]
        member_filter = ""
        if member_id:
            member_filter = " AND u.id=?"
            params.append(member_id)
        sheets = query_all(
            f"""SELECT s.*, u.name FROM inspection_sheets s
                JOIN users u ON u.id=s.user_id
                WHERE s.company_id=? AND (s.sheet_date=? OR COALESCE(s.delivery_date, '')=?) {member_filter}
                ORDER BY u.name LIMIT 200""",
            params,
        )
        sheets = attach_image_info(sheets)
        submitted_rows = query_all(
            f"""SELECT s.user_id FROM inspection_sheets s
                JOIN users u ON u.id=s.user_id
                WHERE s.company_id=? AND (s.sheet_date=? OR COALESCE(s.delivery_date, '')=?) {member_filter}""",
            params,
        )
        submitted_ids = {sheet["user_id"] for sheet in submitted_rows}
        missing_members = [member for member in members if member["id"] not in submitted_ids and (not member_id or member["id"] == member_id)]
        return render(request, "inspection_admin.html", {"target": target, "members": members, "member_id": member_id, "sheets": sheets, "missing_members": missing_members})
    sheets = attach_image_info(
        query_all(
            "SELECT * FROM inspection_sheets WHERE user_id=? AND company_id=? ORDER BY sheet_date DESC LIMIT 30",
            (user["id"], company_id_for(user)),
        )
    )
    return render(request, "inspection_member.html", {"target": target, "sheets": sheets})


@app.post("/inspection-sheets")
async def upload_inspection_sheet(sheet_date: str = Form(...), image: UploadFile = File(...), user=Depends(require_user)):
    require_feature(user, "inspection")
    if user["role"] == "admin":
        raise HTTPException(status_code=403, detail="メンバーとしてアップロードしてください")
    if not image.content_type or not image.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="画像ファイルを選択してください")
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    now_dt = app_now()
    now = now_dt.isoformat(timespec="seconds")
    suffix = safe_upload_name(image.filename)
    filename = f"user{user['id']}_{sheet_date.replace('-', '')}_{now_dt.strftime('%H%M%S%f')}{suffix}"
    disk_path = UPLOAD_DIR / filename
    data = await image.read()
    if not data:
        raise HTTPException(status_code=400, detail="画像ファイルが空です")
    if len(data) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="画像サイズは10MB以内にしてください")
    disk_path.write_bytes(data)
    public_path = upload_to_supabase_storage("inspection_sheets", filename, data, image.content_type or "") or f"/uploads/inspection_sheets/{filename}"
    ocr_text = extract_ocr_text(disk_path)
    extracted = extract_inspection_fields(ocr_text, fallback_date=sheet_date)
    delivery_date = extracted["work_date"] or sheet_date
    ocr_status = "OCR確認待ち" if ocr_text else "手入力待ち"
    company_id = company_id_for(user)
    with db() as conn:
        conn.execute(
            """INSERT INTO inspection_sheets(company_id, user_id, sheet_date, delivery_date, file_path, original_filename, content_type,
               ocr_status, ocr_text, ocr_completed, ocr_pickup, ocr_acceptance, ocr_received, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id, sheet_date) DO UPDATE SET file_path=excluded.file_path,
               company_id=excluded.company_id, delivery_date=excluded.delivery_date,
               original_filename=excluded.original_filename, content_type=excluded.content_type,
               ocr_status=excluded.ocr_status, ocr_text=excluded.ocr_text,
               ocr_completed=excluded.ocr_completed, ocr_pickup=excluded.ocr_pickup,
               ocr_acceptance=excluded.ocr_acceptance, ocr_received=excluded.ocr_received,
               created_at=excluded.created_at""",
            (
                company_id,
                user["id"],
                sheet_date,
                delivery_date,
                public_path,
                image.filename or "",
                image.content_type or "",
                ocr_status,
                ocr_text,
                extracted["completed"],
                extracted["pickup"],
                extracted["acceptance"],
                extracted["received"],
                now,
            ),
        )
        sheet = conn.execute("SELECT id FROM inspection_sheets WHERE user_id=? AND company_id=? AND sheet_date=?", (user["id"], company_id, sheet_date)).fetchone()
        conn.commit()
    return RedirectResponse(f"/inspection-sheets/{sheet['id']}/correct", status_code=303)


def inspection_sheet_for_user(sheet_id: int, user):
    if user["role"] == "admin":
        sheet = query_one(
            """SELECT s.*, u.name AS member_name FROM inspection_sheets s
               JOIN users u ON u.id=s.user_id
               WHERE s.id=? AND s.company_id=?""",
            (sheet_id, company_id_for(user)),
        )
    else:
        sheet = query_one(
            """SELECT s.*, u.name AS member_name FROM inspection_sheets s
               JOIN users u ON u.id=s.user_id
               WHERE s.id=? AND s.user_id=?""",
            (sheet_id, user["id"]),
        )
    if not sheet:
        raise HTTPException(status_code=404, detail="点検表が見つかりません")
    return sheet


@app.get("/inspection-sheets/{sheet_id}/correct", response_class=HTMLResponse)
def inspection_sheet_correct_page(request: Request, sheet_id: int, user=Depends(require_user)):
    require_feature(user, "inspection")
    sheet = with_image_info(inspection_sheet_for_user(sheet_id, user))
    extracted_fields = extract_inspection_fields(sheet.get("ocr_text", "") or "", fallback_date=sheet.get("delivery_date") or sheet["sheet_date"])
    target_date = sheet.get("delivery_date") or extracted_fields["work_date"] or sheet["sheet_date"]
    delivery = query_one("SELECT * FROM deliveries WHERE user_id=? AND company_id=? AND work_date=?", (sheet["user_id"], row_value(sheet, "company_id", company_id_for(user)), target_date))
    ocr_completed = sheet.get("ocr_completed") or extracted_fields["completed"]
    ocr_pickup = sheet.get("ocr_pickup") or extracted_fields["pickup"]
    counts = {
        "completed": ocr_completed or (delivery["completed"] if delivery else 0),
        "transfer": delivery["transfer"] if delivery else 0,
        "night": delivery["night"] if delivery else 0,
        "pickup": ocr_pickup or (delivery["pickup"] if delivery else 0),
        "large": delivery["large"] if delivery else 0,
    }
    ocr_result = {
        "work_date": target_date,
        "completed": ocr_completed,
        "pickup": ocr_pickup,
        "acceptance": sheet.get("ocr_acceptance") or extracted_fields["acceptance"],
        "received": sheet.get("ocr_received") or extracted_fields["received"],
        "raw_text": (sheet.get("ocr_text") or "").strip(),
    }
    return render(
        request,
        "inspection_correct.html",
        {"sheet": sheet, "delivery": delivery, "counts": counts, "target_date": target_date, "ocr_result": ocr_result},
    )


@app.post("/inspection-sheets/{sheet_id}/correct")
def apply_inspection_sheet_counts(
    sheet_id: int,
    work_date: str = Form(...),
    completed: int = Form(0),
    transfer: int = Form(0),
    night: int = Form(0),
    pickup: int = Form(0),
    large: int = Form(0),
    user=Depends(require_user),
):
    require_feature(user, "inspection")
    sheet = inspection_sheet_for_user(sheet_id, user)
    company_id = int(row_value(sheet, "company_id", company_id_for(user)) or company_id_for(user))
    target_date = normalize_import_date(work_date)
    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="日付はYYYY-MM-DD形式で入力してください") from exc
    now = app_now().isoformat(timespec="seconds")
    existing = query_one("SELECT * FROM deliveries WHERE user_id=? AND company_id=? AND work_date=?", (sheet["user_id"], company_id, target_date))
    memo = existing["memo"] if existing and existing["memo"] else "点検表から反映"
    vehicle_rental = existing["vehicle_rental"] if existing else "none"
    values = {
        "completed": max(completed, 0),
        "transfer": max(transfer, 0),
        "night": max(night, 0),
        "pickup": max(pickup, 0),
        "large": max(large, 0),
    }
    with db() as conn:
        conn.execute(
            """INSERT INTO deliveries(company_id, user_id, work_date, completed, transfer, night, pickup, large, vehicle_rental, memo, inspection_sheet_path, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(user_id, work_date) DO UPDATE SET completed=excluded.completed,
               transfer=excluded.transfer, night=excluded.night, pickup=excluded.pickup, large=excluded.large,
               inspection_sheet_path=excluded.inspection_sheet_path, updated_at=excluded.updated_at, company_id=excluded.company_id""",
            (
                company_id,
                sheet["user_id"],
                target_date,
                values["completed"],
                values["transfer"],
                values["night"],
                values["pickup"],
                values["large"],
                vehicle_rental,
                memo,
                sheet["file_path"],
                now,
                now,
            ),
        )
        conn.execute(
            """UPDATE inspection_sheets
               SET delivery_date=?, ocr_status=?, ocr_completed=?, ocr_pickup=?, ocr_acceptance=?
               WHERE id=? AND company_id=?""",
            (
                target_date,
                "反映済み",
                values["completed"],
                values["pickup"],
                values["pickup"],
                sheet_id,
                company_id,
            ),
        )
        conn.commit()
    return RedirectResponse(f"/deliveries?day={target_date}", status_code=303)


@app.get("/rewards", response_class=HTMLResponse)
def rewards_page(request: Request, ym: Optional[str] = None, member_id: Optional[int] = None, user=Depends(require_user)):
    require_feature(user, "rewards")
    start, end = month_bounds(ym)
    viewing_user_id = user["id"]
    members = []
    company_id = company_id_for(user)
    if user["role"] == "admin":
        members = query_all("SELECT id, name FROM users WHERE role='member' AND active=1 AND company_id=? ORDER BY name LIMIT 200", (company_id,))
        if member_id:
            selected = query_one("SELECT id FROM users WHERE id=? AND role='member' AND company_id=?", (member_id, company_id))
            viewing_user_id = selected["id"] if selected else (members[0]["id"] if members else user["id"])
        elif members:
            viewing_user_id = members[0]["id"]
    rows = query_all(
        "SELECT * FROM deliveries WHERE user_id=? AND company_id=? AND work_date BETWEEN ? AND ? ORDER BY work_date",
        (viewing_user_id, company_id, start.isoformat(), end.isoformat()),
    )
    rates = query_one("SELECT * FROM rates WHERE user_id=? AND company_id=?", (viewing_user_id, company_id))
    vehicle_rates = query_one("SELECT * FROM vehicle_rates WHERE id=1")
    daily = []
    total = 0
    monthly_deducted = False
    rental_type = rates["vehicle_rental_type"] if rates and "vehicle_rental_type" in rates.keys() else "none"
    for row in rows:
        monthly = rental_type == "monthly" and not monthly_deducted
        reward = calc_reward(row, rates, vehicle_rates, monthly_mode=monthly)
        monthly_deducted = monthly_deducted or monthly
        daily.append({"row": row, "reward": reward})
        total += reward
    return render(request, "rewards.html", {"daily": daily, "total": total, "ym": start.strftime("%Y-%m"), "members": members, "member_id": viewing_user_id})


@app.get("/vehicle-issues", response_class=HTMLResponse)
def vehicle_issues_page(request: Request, user=Depends(require_user)):
    require_feature(user, "issues")
    ensure_vehicle_issue_schema()
    if user["role"] == "admin":
        issues = query_all("""SELECT v.*, u.name FROM vehicle_issues v JOIN users u ON u.id=v.user_id WHERE v.company_id=? ORDER BY v.created_at DESC LIMIT 100""", (company_id_for(user),))
    else:
        issues = query_all("SELECT * FROM vehicle_issues WHERE user_id=? ORDER BY created_at DESC", (user["id"],))
    normalized_issues = []
    for issue in issues:
        item = dict(issue)
        item["status"] = normalize_issue_status(item.get("status", "未対応"))
        item["status_class"] = ISSUE_STATUS_CLASSES[item["status"]]
        normalized_issues.append(item)
    histories = query_all(
        """SELECT l.*, u.name AS changed_by_name FROM vehicle_issue_status_logs l
           JOIN users u ON u.id=l.changed_by
           JOIN vehicle_issues v ON v.id=l.issue_id
           WHERE v.company_id=? ORDER BY l.changed_at DESC LIMIT 200""",
        (company_id_for(user),),
    )
    history_by_issue = {}
    for history in histories:
        item = dict(history)
        item["status"] = normalize_issue_status(item.get("status", "未対応"))
        history_by_issue.setdefault(item["issue_id"], []).append(item)
    return render(request, "vehicle_issues.html", {"issues": normalized_issues, "history_by_issue": history_by_issue, "statuses": ISSUE_STATUSES})


@app.post("/vehicle-issues")
def save_vehicle_issue(issue_date: str = Form(...), vehicle_name: str = Form(...), severity: str = Form(...), detail: str = Form(...), user=Depends(require_user)):
    require_feature(user, "issues")
    ensure_vehicle_issue_schema()
    if user["role"] == "admin":
        raise HTTPException(status_code=403, detail="メンバーとして入力してください")
    now = datetime.now().isoformat(timespec="seconds")
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO vehicle_issues(company_id, user_id, issue_date, vehicle_name, severity, detail, status, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (company_id_for(user), user["id"], issue_date, vehicle_name, severity, detail, "未対応", now),
        )
        conn.execute(
            "INSERT INTO vehicle_issue_status_logs(issue_id, status, changed_by, changed_at) VALUES (?, ?, ?, ?)",
            (cur.lastrowid, "未対応", user["id"], now),
        )
        conn.commit()
    return RedirectResponse("/vehicle-issues", status_code=303)


@app.post("/vehicle-issues/{issue_id}/status")
def update_vehicle_issue_status(issue_id: int, status: str = Form(...), user=Depends(require_admin)):
    require_feature(user, "issues")
    ensure_vehicle_issue_schema()
    if status not in ISSUE_STATUSES:
        raise HTTPException(status_code=400, detail="状態が正しくありません")
    issue = query_one("SELECT id FROM vehicle_issues WHERE id=? AND company_id=?", (issue_id, company_id_for(user)))
    if not issue:
        raise HTTPException(status_code=404, detail="車両不具合が見つかりません")
    now = datetime.now().isoformat(timespec="seconds")
    with db() as conn:
        conn.execute("UPDATE vehicle_issues SET status=? WHERE id=? AND company_id=?", (status, issue_id, company_id_for(user)))
        conn.execute(
            "INSERT INTO vehicle_issue_status_logs(issue_id, status, changed_by, changed_at) VALUES (?, ?, ?, ?)",
            (issue_id, status, user["id"], now),
        )
        conn.commit()
    return RedirectResponse("/vehicle-issues", status_code=303)


@app.get("/holidays", response_class=HTMLResponse)
def holidays_page(request: Request, user=Depends(require_user)):
    require_feature(user, "holidays")
    ym = request.query_params.get("ym")
    start, end = month_bounds(ym)
    if user["role"] == "admin":
        company_id = company_id_for(user)
        rows = query_all(
            """SELECT h.*, u.name FROM holiday_requests h JOIN users u ON u.id=h.user_id
               WHERE h.company_id=? AND h.request_date BETWEEN ? AND ? ORDER BY h.request_date, u.name LIMIT 300""",
            (company_id, start.isoformat(), end.isoformat()),
        )
    else:
        rows = query_all("SELECT * FROM holiday_requests WHERE user_id=? ORDER BY request_date", (user["id"],))
    holiday_days = simple_japanese_holidays(start.year)
    rows_by_date = {}
    for row in rows:
        rows_by_date.setdefault(row["request_date"], []).append(row)
    return render(
        request,
        "holidays.html",
        {
            "rows": rows,
            "ym": start.strftime("%Y-%m"),
            "days": calendar_days_for_month(start),
            "rows_by_date": rows_by_date,
            "holiday_dates": {d.isoformat() for d in holiday_days},
        },
    )


@app.post("/holidays")
def save_holiday(request_date: str = Form(...), reason: str = Form(""), user=Depends(require_user)):
    require_feature(user, "holidays")
    if user["role"] == "admin":
        raise HTTPException(status_code=403, detail="メンバーとして入力してください")
    execute(
        """INSERT INTO holiday_requests(company_id, user_id, request_date, reason, created_at) VALUES (?,?,?,?,?)
           ON CONFLICT(user_id, request_date) DO UPDATE SET reason=excluded.reason, company_id=excluded.company_id""",
        (company_id_for(user), user["id"], request_date, reason, datetime.now().isoformat(timespec="seconds")),
    )
    return RedirectResponse("/holidays", status_code=303)


@app.get("/admin/shifts")
def admin_shifts_link(user=Depends(require_admin)):
    require_feature(user, "shifts")
    return RedirectResponse("/shifts", status_code=303)


@app.get("/member/shifts")
def member_shifts_link(user=Depends(require_user)):
    require_feature(user, "shifts")
    if user["role"] == "admin":
        return RedirectResponse("/shifts", status_code=303)
    return RedirectResponse("/shifts", status_code=303)


@app.get("/shifts", response_class=HTMLResponse)
def shifts_page(request: Request, ym: Optional[str] = None, message: str = "", user=Depends(require_user)):
    require_feature(user, "shifts")
    start, end = month_bounds(ym)
    if user["role"] == "admin":
        company_id = company_id_for(user)
        members = query_all("SELECT id, name FROM users WHERE role='member' AND active=1 AND company_id=? ORDER BY name LIMIT 200", (company_id,))
        shifts = query_all(
            """SELECT s.*, u.name, d.name AS district_name, t.name AS town_name
               FROM shifts s
               JOIN users u ON u.id=s.user_id
               LEFT JOIN districts d ON d.id=s.district_id
               LEFT JOIN towns t ON t.id=s.town_id
               WHERE s.company_id=? AND s.shift_date BETWEEN ? AND ? ORDER BY s.shift_date, u.name""",
            (company_id, start.isoformat(), end.isoformat()),
        )
        shifts = attach_shift_areas(shifts)
        holidays = query_all("""SELECT h.*, u.name FROM holiday_requests h JOIN users u ON u.id=h.user_id WHERE h.company_id=? AND h.request_date BETWEEN ? AND ? ORDER BY h.request_date LIMIT 300""", (company_id, start.isoformat(), end.isoformat()))
        return render(
            request,
            "admin_shifts.html",
            {
                "members": members,
                "shifts": shifts,
                "holidays": holidays,
                "ym": start.strftime("%Y-%m"),
                "days": calendar_days_for_month(start),
                "towns": active_towns(),
                "shifts_by_date": shifts_by_date(shifts),
                "message": message,
            },
        )
    shifts = query_all(
        """SELECT s.*, d.name AS district_name, t.name AS town_name
           FROM shifts s
           LEFT JOIN districts d ON d.id=s.district_id
           LEFT JOIN towns t ON t.id=s.town_id
           WHERE s.user_id=? AND s.shift_date BETWEEN ? AND ? AND s.decided=1
           ORDER BY s.shift_date""",
        (user["id"], start.isoformat(), end.isoformat()),
    )
    shifts = attach_shift_areas(shifts)
    return render(request, "member_shifts.html", {"shifts": shifts, "ym": start.strftime("%Y-%m"), "days": calendar_days_for_month(start), "shifts_by_date": shifts_by_date(shifts)})


@app.get("/admin/shifts/import-template")
def download_shift_import_template(user=Depends(require_admin)):
    require_feature(user, "shifts")
    rows = [
        SHIFT_IMPORT_HEADERS,
        ["2026-05-05", "笹本", "出勤", "高松1・2・3", "1便", ""],
        ["2026-05-05", "関口", "出勤", "高松4・5", "2便", ""],
    ]
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerows(rows)
    content = "\ufeff" + buffer.getvalue()
    headers = {"Content-Disposition": 'attachment; filename="shift_import_template.csv"'}
    return Response(content=content, media_type="text/csv; charset=utf-8", headers=headers)


@app.post("/admin/shifts/import/preview", response_class=HTMLResponse)
async def preview_shift_import(request: Request, import_file: UploadFile = File(...), user=Depends(require_admin)):
    require_feature(user, "shifts")
    try:
        raw_rows = await load_shift_import_rows(import_file)
        preview = build_shift_import_preview(raw_rows, company_id_for(user))
    except HTTPException as exc:
        return render(
            request,
            "shift_import_confirm.html",
            {
                "rows": [],
                "payload": "[]",
                "has_errors": True,
                "has_conflicts": False,
                "error": exc.detail,
                "filename": import_file.filename or "",
            },
        )
    return render(
        request,
        "shift_import_confirm.html",
        {
            **preview,
            "error": "",
            "filename": import_file.filename or "",
        },
    )


@app.post("/admin/shifts/import/commit", response_class=HTMLResponse)
def commit_shift_import(request: Request, payload: str = Form(...), overwrite_conflicts: Optional[str] = Form(None), user=Depends(require_admin)):
    require_feature(user, "shifts")
    try:
        rows = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="取り込み内容を読み取れませんでした") from exc
    if not isinstance(rows, list) or not rows:
        return render(
            request,
            "shift_import_confirm.html",
            {
                "rows": [],
                "payload": "[]",
                "has_errors": True,
                "has_conflicts": False,
                "error": "登録できる行がありません",
                "filename": "",
            },
        )

    company_id = company_id_for(user)
    conflicts = current_shift_conflicts(rows, company_id)
    for row in rows:
        row["errors"] = []
        row["conflict"] = bool(row.get("member_id") and row.get("shift_date") and (int(row["member_id"]), row["shift_date"]) in conflicts)
    if conflicts and not overwrite_conflicts:
        return render(
            request,
            "shift_import_confirm.html",
            {
                "rows": rows,
                "payload": json.dumps([{key: value for key, value in row.items() if key != "errors"} for row in rows], ensure_ascii=False),
                "has_errors": False,
                "has_conflicts": True,
                "error": "既存シフトがあります。上書きする場合はチェックを入れて確定してください。",
                "filename": "",
            },
        )

    towns_by_id = {int(town["id"]): town for town in active_towns()}
    with db() as conn:
        for row in rows:
            status = row.get("status", "")
            if status not in SHIFT_ALLOWED_STATUSES:
                raise HTTPException(status_code=400, detail="出勤区分が正しくありません")
            town_ids = [int(town_id) for town_id in row.get("town_ids", [])]
            town_rows = [towns_by_id[town_id] for town_id in town_ids if town_id in towns_by_id]
            if status != "休み" and len(town_rows) != len(set(town_ids)):
                raise HTTPException(status_code=400, detail="配達エリアが正しくありません")
            upsert_shift_with_towns(conn, int(row["member_id"]), row["shift_date"], status, town_rows, row.get("note", ""), company_id)
        conn.commit()
    ym = rows[0]["shift_date"][:7] if rows and rows[0].get("shift_date") else app_today().strftime("%Y-%m")
    message = quote(f"{len(rows)}件のシフトを取り込みました")
    return RedirectResponse(f"/shifts?ym={ym}&message={message}", status_code=303)


@app.post("/shifts")
def save_shift(
    member_id: int = Form(...),
    shift_date: str = Form(...),
    status: str = Form(...),
    town_ids: List[int] = Form(default=[]),
    user=Depends(require_admin),
):
    require_feature(user, "shifts")
    upsert_shift_record(member_id, shift_date, status, town_ids, company_id=company_id_for(user))
    return RedirectResponse("/shifts", status_code=303)


@app.get("/admin/areas", response_class=HTMLResponse)
def areas_page(request: Request, user=Depends(require_admin)):
    require_feature(user, "multi_depot")
    depots = query_all("SELECT * FROM depots WHERE active=1 ORDER BY name")
    districts = query_all(
        """SELECT d.*, p.name AS depot_name FROM districts d
           LEFT JOIN depots p ON p.id=d.depot_id
           WHERE d.active=1 ORDER BY p.name, d.name"""
    )
    towns = query_all(
        """SELECT t.*, d.name AS district_name, p.name AS depot_name FROM towns t
           JOIN districts d ON d.id=t.district_id
           LEFT JOIN depots p ON p.id=d.depot_id
           WHERE t.active=1 AND d.active=1 ORDER BY p.name, d.name, t.name"""
    )
    return render(request, "areas.html", {"depots": depots, "districts": districts, "towns": towns})


@app.post("/admin/areas/districts")
def save_district(district_id: str = Form(""), depot_id: int = Form(...), name: str = Form(...), user=Depends(require_admin)):
    require_feature(user, "multi_depot")
    now = datetime.now().isoformat(timespec="seconds")
    if district_id:
        execute("UPDATE districts SET depot_id=?, name=? WHERE id=?", (depot_id, name, district_id))
    else:
        execute("INSERT OR IGNORE INTO districts(depot_id, name, created_at) VALUES (?, ?, ?)", (depot_id, name, now))
    return RedirectResponse("/admin/areas", status_code=303)


@app.post("/admin/areas/districts/{district_id}/delete")
def delete_district(district_id: int, user=Depends(require_admin)):
    require_feature(user, "multi_depot")
    execute("UPDATE districts SET active=0 WHERE id=?", (district_id,))
    execute("UPDATE towns SET active=0 WHERE district_id=?", (district_id,))
    return RedirectResponse("/admin/areas", status_code=303)


@app.post("/admin/areas/towns")
def save_town(town_id: str = Form(""), district_id: int = Form(...), name: str = Form(...), user=Depends(require_admin)):
    require_feature(user, "multi_depot")
    now = datetime.now().isoformat(timespec="seconds")
    if town_id:
        execute("UPDATE towns SET district_id=?, name=? WHERE id=?", (district_id, name, town_id))
    else:
        execute("INSERT OR IGNORE INTO towns(district_id, name, created_at) VALUES (?, ?, ?)", (district_id, name, now))
    return RedirectResponse("/admin/areas", status_code=303)


@app.post("/admin/areas/towns/{town_id}/delete")
def delete_town(town_id: int, user=Depends(require_admin)):
    require_feature(user, "multi_depot")
    execute("UPDATE towns SET active=0 WHERE id=?", (town_id,))
    return RedirectResponse("/admin/areas", status_code=303)


@app.get("/admin/safety", response_class=HTMLResponse)
def admin_safety(request: Request, user=Depends(require_admin)):
    require_feature(user, "safety")
    company_id = company_id_for(user)
    logs = query_all("""SELECT w.*, u.name FROM work_logs w JOIN users u ON u.id=w.user_id WHERE w.company_id=? ORDER BY w.logged_at DESC LIMIT 100""", (company_id,))
    members = query_all("SELECT id, name FROM users WHERE role='member' AND company_id=? ORDER BY active DESC, name LIMIT 200", (company_id,))
    return render(request, "admin_safety.html", {"logs": logs, "members": members})


@app.get("/admin/safety/print", response_class=HTMLResponse)
def admin_safety_print(request: Request, ym: Optional[str] = None, member_id: Optional[int] = None, user=Depends(require_admin)):
    require_feature(user, "safety")
    start, end = month_bounds(ym)
    company_id = company_id_for(user)
    member = None
    if member_id:
        member = query_one("SELECT * FROM users WHERE id=? AND role='member' AND company_id=?", (member_id, company_id))
    if not member:
        member = query_one("SELECT * FROM users WHERE role='member' AND company_id=? ORDER BY active DESC, name LIMIT 1", (company_id,))
    if not member:
        return render(request, "safety_print.html", {"records": [], "ym": start.strftime("%Y-%m"), "period": f"{start.isoformat()} ～ {end.isoformat()}", "member": None})
    logs = query_all(
        """SELECT w.*, u.name FROM work_logs w JOIN users u ON u.id=w.user_id
           WHERE w.user_id=? AND w.company_id=? AND w.work_date BETWEEN ? AND ? ORDER BY w.work_date, w.log_type""",
        (member["id"], company_id, start.isoformat(), end.isoformat()),
    )
    issues = query_all(
        """SELECT v.*, u.name FROM vehicle_issues v JOIN users u ON u.id=v.user_id
           WHERE v.user_id=? AND v.company_id=? AND v.issue_date BETWEEN ? AND ? ORDER BY v.issue_date""",
        (member["id"], company_id, start.isoformat(), end.isoformat()),
    )
    issue_map = {}
    for issue in issues:
        issue_map.setdefault(issue["issue_date"], []).append(issue["detail"])
    log_map = {}
    for log in logs:
        log_map.setdefault(log["work_date"], {})[log["log_type"]] = log
    records = []
    for day in calendar_days_for_month(start):
        day_s = day.isoformat()
        start_log = log_map.get(day_s, {}).get("start")
        end_log = log_map.get(day_s, {}).get("end")
        day_issues = issue_map.get(day_s, [])
        notes = []
        for log in (start_log, end_log):
            if log and log["notes"]:
                notes.append(log["notes"])
        if day_issues:
            notes.append("車両不具合: " + " / ".join(day_issues))
        records.append(
            {
                "date": date_label(day),
                "vehicle_number": member["vehicle"],
                "vehicle_status": "不具合あり" if day_issues else "良好",
                "roll_call": "実施" if start_log or end_log else "",
                "alcohol": "/".join([log["alcohol_result"] for log in (start_log, end_log) if log]) or "",
                "start_time": start_log["logged_at"][11:16] if start_log and len(start_log["logged_at"]) >= 16 else "",
                "end_time": end_log["logged_at"][11:16] if end_log and len(end_log["logged_at"]) >= 16 else "",
                "start_point": "",
                "end_point": "",
                "distance": "",
                "via": "",
                "notes": " / ".join(notes),
            }
        )
    return render(
        request,
        "safety_print.html",
        {"records": records, "ym": start.strftime("%Y-%m"), "period": f"{start.isoformat()} ～ {end.isoformat()}", "member": member},
    )
