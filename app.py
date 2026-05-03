import calendar
from datetime import date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
import re
import sqlite3
import psycopg2
import psycopg2.extras
from typing import Optional
from urllib.parse import quote
# supabase connected
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, URLSafeSerializer


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "delivery_ops.db"
UPLOAD_DIR = BASE_DIR / "uploads" / "inspection_sheets"
SLIP_UPLOAD_DIR = BASE_DIR / "uploads" / "inspection_slips"
SECRET_KEY = "change-this-local-secret"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
SLIP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="配送業務管理MVP")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.mount("/uploads", StaticFiles(directory=BASE_DIR / "uploads"), name="uploads")
templates = Jinja2Templates(directory=BASE_DIR / "templates")
serializer = URLSafeSerializer(SECRET_KEY, salt="delivery-ops-session")


def db():
    database_url = os.environ.get("DATABASE_URL")

    if database_url:
        conn = psycopg2.connect(
            database_url,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        conn.autocommit = False
        return conn

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def hash_password(password: str) -> str:
    return sha256(password.encode("utf-8")).hexdigest()


def safe_upload_name(original_name: str):
    suffix = Path(original_name or "").suffix.lower()
    if suffix not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic", ".heif"}:
        suffix = ".jpg"
    return suffix


def execute(sql: str, params=()):
    with db() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur


def query_one(sql: str, params=()):
    with db() as conn:
        return conn.execute(sql, params).fetchone()


def query_all(sql: str, params=()):
    with db() as conn:
        return conn.execute(sql, params).fetchall()


def ensure_column(conn, table: str, column: str, definition: str):
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


def init_db():
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                user_id INTEGER NOT NULL,
                work_date TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                transfer INTEGER NOT NULL DEFAULT 0,
                night INTEGER NOT NULL DEFAULT 0,
                pickup INTEGER NOT NULL DEFAULT 0,
                large INTEGER NOT NULL DEFAULT 0,
                vehicle_rental TEXT NOT NULL DEFAULT 'none',
                memo TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, work_date),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS vehicle_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                user_id INTEGER NOT NULL,
                request_date TEXT NOT NULL,
                reason TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                UNIQUE(user_id, request_date),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                user_id INTEGER NOT NULL,
                sheet_date TEXT NOT NULL,
                file_path TEXT NOT NULL,
                original_filename TEXT DEFAULT '',
                content_type TEXT DEFAULT '',
                ocr_status TEXT NOT NULL DEFAULT '未処理',
                ocr_text TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id),
                UNIQUE(user_id, sheet_date)
            );
            CREATE TABLE IF NOT EXISTS inspection_slips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            """
        )
        ensure_column(conn, "rates", "vehicle_rental_type", "TEXT NOT NULL DEFAULT 'none'")
        ensure_column(conn, "rates", "vehicle_daily_fee", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "rates", "vehicle_monthly_fee", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "shifts", "district_id", "INTEGER")
        ensure_column(conn, "shifts", "town_id", "INTEGER")
        ensure_column(conn, "shifts", "area_label", "TEXT DEFAULT ''")
        ensure_column(conn, "vehicle_issues", "status", "TEXT NOT NULL DEFAULT '未対応'")
        ensure_column(conn, "users", "purged", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "users", "deleted_at", "TEXT DEFAULT ''")
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


@app.on_event("startup")
def startup():
    init_db()


def current_user(request: Request):
    raw = request.cookies.get("session")
    if not raw:
        return None
    try:
        user_id = serializer.loads(raw)
    except BadSignature:
        return None
    return query_one("SELECT * FROM users WHERE id = ? AND active = 1", (user_id,))


def require_user(request: Request):
    user = current_user(request)
    if not user:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    return user


def require_admin(user=Depends(require_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="管理者だけが利用できます")
    return user


def render(request: Request, name: str, context: dict):
    try:
        user = current_user(request)
    except:
        user = None

    context.update({
        "request": request,
        "user": user,
        "today": date.today().isoformat()
    })

    return templates.TemplateResponse(request, name, context)
def month_bounds(ym: Optional[str]):
    if not ym:
        base = date.today().replace(day=1)
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
        areas = area_map.get(item["id"], [])
        if not areas and item.get("area_label"):
            areas = [part.strip() for part in item["area_label"].split("・") if part.strip()]
        if item["status"] == "休み":
            areas = []
        item["areas"] = areas
        item["area_text"] = "、".join(areas)
        item["area_short_text"] = shorten_areas(short_map.get(item["id"], [])) if item["status"] != "休み" else ""
    return items


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
    user = query_one("SELECT * FROM users WHERE username = ? AND active = 1", (username,))
    if not user or user["password_hash"] != hash_password(password):
        return render(request, "login.html", {"error": "IDまたはパスワードが違います"})
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
    members = query_all("SELECT * FROM users WHERE role = 'member' AND active = 1 ORDER BY id")
    today_s = date.today().isoformat()
    logs = query_all(
        """SELECT w.*, u.name FROM work_logs w JOIN users u ON u.id = w.user_id
           WHERE w.work_date = ? ORDER BY w.logged_at DESC""",
        (today_s,),
    )
    issues = query_all(
        """SELECT v.*, u.name FROM vehicle_issues v JOIN users u ON u.id = v.user_id
           ORDER BY v.created_at DESC LIMIT 5"""
    )
    deliveries = query_all(
        """SELECT d.*, u.name FROM deliveries d JOIN users u ON u.id = d.user_id
           WHERE d.work_date = ? ORDER BY u.name""",
        (today_s,),
    )
    return render(request, "admin_dashboard.html", {"members": members, "logs": logs, "issues": issues, "deliveries": deliveries})


@app.get("/admin/members", response_class=HTMLResponse)
def members_page(request: Request, message: str = "", error: str = "", user=Depends(require_admin)):
    active_members = query_all("SELECT * FROM users WHERE role = 'member' AND active = 1 AND COALESCE(purged,0)=0 ORDER BY id")
    retired_members = query_all("SELECT * FROM users WHERE role = 'member' AND active = 0 AND COALESCE(purged,0)=0 ORDER BY id")
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
    now = datetime.now().isoformat(timespec="seconds")
    if member_id:
        duplicate = query_one("SELECT id FROM users WHERE username=? AND id<>?", (username, member_id))
        if duplicate:
            return RedirectResponse(f"/admin/members?error={quote('同じログインIDが既に使われています')}", status_code=303)
        if password:
            execute(
                "UPDATE users SET name=?, username=?, password_hash=?, phone=?, vehicle=? WHERE id=? AND role='member'",
                (name, username, hash_password(password), phone, vehicle, member_id),
            )
        else:
            execute("UPDATE users SET name=?, username=?, phone=?, vehicle=? WHERE id=? AND role='member'", (name, username, phone, vehicle, member_id))
        return RedirectResponse("/admin/members?message=" + quote("メンバー情報を更新しました"), status_code=303)
    existing = query_one("SELECT * FROM users WHERE username=? AND role='member'", (username,))
    if existing and existing["active"] == 1:
        return RedirectResponse(f"/admin/members?error={quote('同じログインIDの在籍中メンバーがいます')}", status_code=303)
    if existing and existing["active"] == 0:
        if password:
            execute(
                "UPDATE users SET name=?, password_hash=?, phone=?, vehicle=?, active=1 WHERE id=?",
                (name, hash_password(password), phone, vehicle, existing["id"]),
            )
        else:
            execute("UPDATE users SET name=?, phone=?, vehicle=?, active=1 WHERE id=?", (name, phone, vehicle, existing["id"]))
        execute("INSERT OR IGNORE INTO rates(user_id) VALUES (?)", (existing["id"],))
        return RedirectResponse("/admin/members?message=" + quote("退職済みメンバーを在籍中に戻しました"), status_code=303)
    try:
        cur = execute(
            "INSERT INTO users(name, username, password_hash, role, phone, vehicle, created_at) VALUES (?,?,?,?,?,?,?)",
            (name, username, hash_password(password or "member123"), "member", phone, vehicle, now),
        )
        execute("INSERT OR IGNORE INTO rates(user_id) VALUES (?)", (cur.lastrowid,))
    except sqlite3.IntegrityError:
        return RedirectResponse(f"/admin/members?error={quote('ログインIDが重複しています。別のIDを入力してください')}", status_code=303)
    return RedirectResponse("/admin/members?message=" + quote("メンバーを追加しました"), status_code=303)


@app.post("/admin/members/{member_id}/delete")
def delete_member(member_id: int, user=Depends(require_admin)):
    execute("UPDATE users SET active = 0 WHERE id=? AND role='member'", (member_id,))
    return RedirectResponse("/admin/members", status_code=303)


@app.post("/admin/members/{member_id}/purge")
def purge_retired_member(member_id: int, user=Depends(require_admin)):
    member = query_one("SELECT * FROM users WHERE id=? AND role='member'", (member_id,))
    if not member:
        return RedirectResponse(f"/admin/members?error={quote('メンバーが見つかりません')}", status_code=303)
    if member["active"] == 1:
        return RedirectResponse(f"/admin/members?error={quote('在籍中メンバーは完全削除できません。先に退職扱いにしてください')}", status_code=303)
    now = datetime.now().isoformat(timespec="seconds")
    anonymized_username = f"deleted_member_{member_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    execute(
        """UPDATE users
           SET name=?, username=?, password_hash=?, phone='', vehicle='', active=0, purged=1, deleted_at=?
           WHERE id=? AND role='member' AND active=0""",
        (f"削除済みメンバー{member_id}", anonymized_username, hash_password(anonymized_username), now, member_id),
    )
    return RedirectResponse("/admin/members?message=" + quote("退職済みメンバーを完全削除しました。過去記録は匿名化された名前で保持されます"), status_code=303)


@app.get("/admin/rates", response_class=HTMLResponse)
def rates_page(request: Request, user=Depends(require_admin)):
    rows = query_all(
        """SELECT u.id, u.name, r.* FROM users u LEFT JOIN rates r ON r.user_id = u.id
           WHERE u.role='member' AND u.active=1 ORDER BY u.id"""
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
    execute(
        """INSERT INTO rates(user_id, delivery_unit, transfer_unit, night_unit, pickup_unit, large_unit,
           vehicle_rental_type, vehicle_daily_fee, vehicle_monthly_fee)
           VALUES (?,?,?,?,?,?,?,?,?)
           ON CONFLICT(user_id) DO UPDATE SET delivery_unit=excluded.delivery_unit,
           transfer_unit=excluded.transfer_unit, night_unit=excluded.night_unit,
           pickup_unit=excluded.pickup_unit, large_unit=excluded.large_unit,
           vehicle_rental_type=excluded.vehicle_rental_type,
           vehicle_daily_fee=excluded.vehicle_daily_fee,
           vehicle_monthly_fee=excluded.vehicle_monthly_fee""",
        (user_id, delivery_unit, transfer_unit, night_unit, pickup_unit, large_unit, vehicle_rental_type, vehicle_daily_fee, vehicle_monthly_fee),
    )
    return RedirectResponse("/admin/rates", status_code=303)


@app.get("/admin/vehicle-rates", response_class=HTMLResponse)
def vehicle_rates_page(request: Request, user=Depends(require_admin)):
    rates = query_one("SELECT * FROM vehicle_rates WHERE id=1")
    return render(request, "vehicle_rates.html", {"rates": rates})


@app.post("/admin/vehicle-rates")
def save_vehicle_rates(daily_fee: int = Form(...), monthly_fee: int = Form(...), user=Depends(require_admin)):
    execute("UPDATE vehicle_rates SET daily_fee=?, monthly_fee=? WHERE id=1", (daily_fee, monthly_fee))
    return RedirectResponse("/admin/vehicle-rates", status_code=303)


@app.get("/member", response_class=HTMLResponse)
def member_home(request: Request, user=Depends(require_user)):
    if user["role"] == "admin":
        return RedirectResponse("/admin", status_code=303)
    today_s = date.today().isoformat()
    delivery = query_one("SELECT * FROM deliveries WHERE user_id=? AND work_date=?", (user["id"], today_s))
    rates = query_one("SELECT * FROM rates WHERE user_id=?", (user["id"],))
    vehicle_rates = query_one("SELECT * FROM vehicle_rates WHERE id=1")
    shift = query_one("SELECT * FROM shifts WHERE user_id=? AND shift_date=? AND decided=1", (user["id"], today_s))
    reward = calc_reward(delivery, rates, vehicle_rates) if delivery else 0
    comment = auto_comment(delivery, reward, bool(shift))
    return render(request, "member_home.html", {"delivery": delivery, "reward": reward, "comment": comment, "shift": shift})


@app.get("/work-log", response_class=HTMLResponse)
def work_log_page(request: Request, user=Depends(require_user)):
    logs = query_all("SELECT * FROM work_logs WHERE user_id=? ORDER BY logged_at DESC LIMIT 20", (user["id"],))
    return render(request, "work_log.html", {"logs": logs})


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
    execute(
        """INSERT INTO work_logs(user_id, work_date, log_type, logged_at, alcohol_result, detector_used,
           intoxicated, health_status, face_check, breath_check, voice_check, admin_confirm, notes, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (user["id"], work_date, log_type, logged_at, alcohol_result, detector_used, intoxicated, health_status, face_check, breath_check, voice_check, admin_confirm, notes, datetime.now().isoformat(timespec="seconds")),
    )
    return RedirectResponse("/work-log", status_code=303)


@app.get("/deliveries", response_class=HTMLResponse)
def delivery_page(request: Request, day: Optional[str] = None, user=Depends(require_user)):
    target = day or date.today().isoformat()
    if user["role"] == "admin":
        rows = query_all(
            """SELECT d.*, u.name, s.file_path AS slip_path
               FROM deliveries d
               JOIN users u ON u.id=d.user_id
               LEFT JOIN inspection_slips s ON s.delivery_id=d.id
               WHERE d.work_date=? ORDER BY u.name""",
            (target,),
        )
        return render(request, "admin_deliveries.html", {"rows": rows, "target": target})
    delivery = query_one("SELECT * FROM deliveries WHERE user_id=? AND work_date=?", (user["id"], target))
    slip = query_one("SELECT * FROM inspection_slips WHERE user_id=? AND slip_date=?", (user["id"], target))
    rates = query_one("SELECT * FROM rates WHERE user_id=?", (user["id"],))
    vehicle_rates = query_one("SELECT * FROM vehicle_rates WHERE id=1")
    reward = calc_reward(delivery, rates, vehicle_rates) if delivery else 0
    return render(request, "deliveries.html", {"delivery": delivery, "slip": slip, "target": target, "reward": reward, "comment": auto_comment(delivery, reward)})


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
    inspection_image: UploadFile | None = File(None),
    user=Depends(require_user),
):
    if user["role"] == "admin":
        raise HTTPException(status_code=403, detail="メンバーとしてログインしてください")
    now = datetime.now().isoformat(timespec="seconds")
    with db() as conn:
        conn.execute(
            """INSERT INTO deliveries(user_id, work_date, completed, transfer, night, pickup, large, vehicle_rental, memo, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(user_id, work_date) DO UPDATE SET completed=excluded.completed, transfer=excluded.transfer,
               night=excluded.night, pickup=excluded.pickup, large=excluded.large, vehicle_rental=excluded.vehicle_rental,
               memo=excluded.memo, updated_at=excluded.updated_at""",
            (user["id"], work_date, completed, transfer, night, pickup, large, vehicle_rental, memo, now, now),
        )
        delivery = conn.execute("SELECT id FROM deliveries WHERE user_id=? AND work_date=?", (user["id"], work_date)).fetchone()
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
            filename = f"user{user['id']}_{work_date.replace('-', '')}_{datetime.now().strftime('%H%M%S%f')}{suffix}"
            disk_path = SLIP_UPLOAD_DIR / filename
            disk_path.write_bytes(data)
            public_path = f"/uploads/inspection_slips/{filename}"
            existing = query_one("SELECT file_path FROM inspection_slips WHERE user_id=? AND slip_date=?", (user["id"], work_date))
            with db() as conn:
                conn.execute(
                    """INSERT INTO inspection_slips(user_id, delivery_id, slip_date, file_path, original_filename, content_type, uploaded_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(user_id, slip_date) DO UPDATE SET delivery_id=excluded.delivery_id,
                       file_path=excluded.file_path, original_filename=excluded.original_filename,
                       content_type=excluded.content_type, uploaded_at=excluded.uploaded_at""",
                    (user["id"], delivery["id"], work_date, public_path, inspection_image.filename or "", inspection_image.content_type or "", now),
                )
                conn.commit()
            if existing and existing["file_path"].startswith("/uploads/inspection_slips/"):
                old_path = BASE_DIR / existing["file_path"].lstrip("/")
                if old_path.exists() and old_path != disk_path:
                    try:
                        old_path.unlink()
                    except OSError:
                        pass
    return RedirectResponse(f"/deliveries?day={work_date}", status_code=303)


@app.get("/inspection-sheets", response_class=HTMLResponse)
def inspection_sheets_page(request: Request, day: Optional[str] = None, member_id: Optional[int] = None, user=Depends(require_user)):
    target = day or date.today().isoformat()
    if user["role"] == "admin":
        members = query_all("SELECT id, name FROM users WHERE role='member' AND active=1 AND COALESCE(purged,0)=0 ORDER BY name")
        params = [target]
        member_filter = ""
        if member_id:
            member_filter = " AND u.id=?"
            params.append(member_id)
        sheets = query_all(
            f"""SELECT s.*, u.name FROM inspection_sheets s
                JOIN users u ON u.id=s.user_id
                WHERE s.sheet_date=? {member_filter}
                ORDER BY u.name""",
            params,
        )
        submitted_ids = {sheet["user_id"] for sheet in sheets}
        missing_members = [member for member in members if member["id"] not in submitted_ids and (not member_id or member["id"] == member_id)]
        return render(request, "inspection_admin.html", {"target": target, "members": members, "member_id": member_id, "sheets": sheets, "missing_members": missing_members})
    sheets = query_all("SELECT * FROM inspection_sheets WHERE user_id=? ORDER BY sheet_date DESC LIMIT 30", (user["id"],))
    return render(request, "inspection_member.html", {"target": target, "sheets": sheets})


@app.post("/inspection-sheets")
async def upload_inspection_sheet(sheet_date: str = Form(...), image: UploadFile = File(...), user=Depends(require_user)):
    if user["role"] == "admin":
        raise HTTPException(status_code=403, detail="メンバーとしてアップロードしてください")
    if not image.content_type or not image.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="画像ファイルを選択してください")
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now().isoformat(timespec="seconds")
    suffix = safe_upload_name(image.filename)
    filename = f"user{user['id']}_{sheet_date.replace('-', '')}_{datetime.now().strftime('%H%M%S%f')}{suffix}"
    disk_path = UPLOAD_DIR / filename
    data = await image.read()
    if not data:
        raise HTTPException(status_code=400, detail="画像ファイルが空です")
    if len(data) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="画像サイズは10MB以内にしてください")
    disk_path.write_bytes(data)
    public_path = f"/uploads/inspection_sheets/{filename}"
    existing = query_one("SELECT file_path FROM inspection_sheets WHERE user_id=? AND sheet_date=?", (user["id"], sheet_date))
    with db() as conn:
        conn.execute(
            """INSERT INTO inspection_sheets(user_id, sheet_date, file_path, original_filename, content_type, ocr_status, created_at)
               VALUES (?, ?, ?, ?, ?, '未処理', ?)
               ON CONFLICT(user_id, sheet_date) DO UPDATE SET file_path=excluded.file_path,
               original_filename=excluded.original_filename, content_type=excluded.content_type,
               ocr_status='未処理', ocr_text='', created_at=excluded.created_at""",
            (user["id"], sheet_date, public_path, image.filename or "", image.content_type or "", now),
        )
        conn.commit()
    if existing and existing["file_path"].startswith("/uploads/inspection_sheets/"):
        old_path = BASE_DIR / existing["file_path"].lstrip("/")
        if old_path.exists() and old_path != disk_path:
            try:
                old_path.unlink()
            except OSError:
                pass
    return RedirectResponse(f"/inspection-sheets?day={sheet_date}", status_code=303)


@app.get("/rewards", response_class=HTMLResponse)
def rewards_page(request: Request, ym: Optional[str] = None, member_id: Optional[int] = None, user=Depends(require_user)):
    start, end = month_bounds(ym)
    viewing_user_id = user["id"]
    members = []
    if user["role"] == "admin":
        members = query_all("SELECT id, name FROM users WHERE role='member' AND active=1 ORDER BY name")
        if member_id:
            viewing_user_id = member_id
        elif members:
            viewing_user_id = members[0]["id"]
    rows = query_all("SELECT * FROM deliveries WHERE user_id=? AND work_date BETWEEN ? AND ? ORDER BY work_date", (viewing_user_id, start.isoformat(), end.isoformat()))
    rates = query_one("SELECT * FROM rates WHERE user_id=?", (viewing_user_id,))
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
    ensure_vehicle_issue_schema()
    if user["role"] == "admin":
        issues = query_all("""SELECT v.*, u.name FROM vehicle_issues v JOIN users u ON u.id=v.user_id ORDER BY v.created_at DESC""")
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
           JOIN users u ON u.id=l.changed_by ORDER BY l.changed_at DESC"""
    )
    history_by_issue = {}
    for history in histories:
        item = dict(history)
        item["status"] = normalize_issue_status(item.get("status", "未対応"))
        history_by_issue.setdefault(item["issue_id"], []).append(item)
    return render(request, "vehicle_issues.html", {"issues": normalized_issues, "history_by_issue": history_by_issue, "statuses": ISSUE_STATUSES})


@app.post("/vehicle-issues")
def save_vehicle_issue(issue_date: str = Form(...), vehicle_name: str = Form(...), severity: str = Form(...), detail: str = Form(...), user=Depends(require_user)):
    ensure_vehicle_issue_schema()
    if user["role"] == "admin":
        raise HTTPException(status_code=403, detail="メンバーとして入力してください")
    now = datetime.now().isoformat(timespec="seconds")
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO vehicle_issues(user_id, issue_date, vehicle_name, severity, detail, status, created_at) VALUES (?,?,?,?,?,?,?)",
            (user["id"], issue_date, vehicle_name, severity, detail, "未対応", now),
        )
        conn.execute(
            "INSERT INTO vehicle_issue_status_logs(issue_id, status, changed_by, changed_at) VALUES (?, ?, ?, ?)",
            (cur.lastrowid, "未対応", user["id"], now),
        )
        conn.commit()
    return RedirectResponse("/vehicle-issues", status_code=303)


@app.post("/vehicle-issues/{issue_id}/status")
def update_vehicle_issue_status(issue_id: int, status: str = Form(...), user=Depends(require_admin)):
    ensure_vehicle_issue_schema()
    if status not in ISSUE_STATUSES:
        raise HTTPException(status_code=400, detail="状態が正しくありません")
    issue = query_one("SELECT id FROM vehicle_issues WHERE id=?", (issue_id,))
    if not issue:
        raise HTTPException(status_code=404, detail="車両不具合が見つかりません")
    now = datetime.now().isoformat(timespec="seconds")
    with db() as conn:
        conn.execute("UPDATE vehicle_issues SET status=? WHERE id=?", (status, issue_id))
        conn.execute(
            "INSERT INTO vehicle_issue_status_logs(issue_id, status, changed_by, changed_at) VALUES (?, ?, ?, ?)",
            (issue_id, status, user["id"], now),
        )
        conn.commit()
    return RedirectResponse("/vehicle-issues", status_code=303)


@app.get("/holidays", response_class=HTMLResponse)
def holidays_page(request: Request, user=Depends(require_user)):
    ym = request.query_params.get("ym")
    start, end = month_bounds(ym)
    if user["role"] == "admin":
        rows = query_all(
            """SELECT h.*, u.name FROM holiday_requests h JOIN users u ON u.id=h.user_id
               WHERE h.request_date BETWEEN ? AND ? ORDER BY h.request_date, u.name""",
            (start.isoformat(), end.isoformat()),
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
    if user["role"] == "admin":
        raise HTTPException(status_code=403, detail="メンバーとして入力してください")
    execute(
        """INSERT INTO holiday_requests(user_id, request_date, reason, created_at) VALUES (?,?,?,?)
           ON CONFLICT(user_id, request_date) DO UPDATE SET reason=excluded.reason""",
        (user["id"], request_date, reason, datetime.now().isoformat(timespec="seconds")),
    )
    return RedirectResponse("/holidays", status_code=303)


@app.get("/shifts", response_class=HTMLResponse)
def shifts_page(request: Request, ym: Optional[str] = None, user=Depends(require_user)):
    start, end = month_bounds(ym)
    if user["role"] == "admin":
        members = query_all("SELECT id, name FROM users WHERE role='member' AND active=1 ORDER BY name")
        shifts = query_all(
            """SELECT s.*, u.name, d.name AS district_name, t.name AS town_name
               FROM shifts s
               JOIN users u ON u.id=s.user_id
               LEFT JOIN districts d ON d.id=s.district_id
               LEFT JOIN towns t ON t.id=s.town_id
               WHERE s.shift_date BETWEEN ? AND ? ORDER BY s.shift_date, u.name""",
            (start.isoformat(), end.isoformat()),
        )
        shifts = attach_shift_areas(shifts)
        holidays = query_all("""SELECT h.*, u.name FROM holiday_requests h JOIN users u ON u.id=h.user_id WHERE h.request_date BETWEEN ? AND ? ORDER BY h.request_date""", (start.isoformat(), end.isoformat()))
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


@app.post("/shifts")
def save_shift(
    member_id: int = Form(...),
    shift_date: str = Form(...),
    status: str = Form(...),
    town_ids: list[int] = Form(default=[]),
    user=Depends(require_admin),
):
    allowed = {"出勤", "1便", "2便", "3便", "休み"}
    if status not in allowed:
        raise HTTPException(status_code=400, detail="出勤区分が正しくありません")
    if status != "休み" and not town_ids:
        raise HTTPException(status_code=400, detail="休み以外は町会・配達エリアを1つ以上選択してください")
    town_rows = []
    if town_ids:
        placeholders = ",".join("?" for _ in town_ids)
        town_rows = query_all(
            f"""SELECT t.id, t.name, d.id AS district_id, d.name AS district_name
                FROM towns t JOIN districts d ON d.id=t.district_id
                WHERE t.id IN ({placeholders}) AND t.active=1 AND d.active=1
                ORDER BY d.name, t.name""",
            town_ids,
        )
    if status != "休み" and len(town_rows) != len(set(town_ids)):
        raise HTTPException(status_code=400, detail="選択された町会・配達エリアが正しくありません")
    first_town = town_rows[0] if town_rows else None
    area_label = "・".join(f"{town['district_name']} / {town['name']}" for town in town_rows)
    with db() as conn:
        conn.execute(
            """INSERT INTO shifts(user_id, shift_date, status, start_time, end_time, note, district_id, town_id, area_label, decided, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(user_id, shift_date) DO UPDATE SET status=excluded.status, start_time=excluded.start_time,
               end_time=excluded.end_time, note=excluded.note, district_id=excluded.district_id, town_id=excluded.town_id,
               area_label=excluded.area_label, decided=excluded.decided, updated_at=excluded.updated_at""",
            (
                member_id,
                shift_date,
                status,
                "",
                "",
                area_label,
                first_town["district_id"] if first_town else None,
                first_town["id"] if first_town else None,
                area_label,
                1,
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        shift = conn.execute("SELECT id FROM shifts WHERE user_id=? AND shift_date=?", (member_id, shift_date)).fetchone()
        conn.execute("DELETE FROM shift_towns WHERE shift_id=?", (shift["id"],))
        if status != "休み":
            for town in town_rows:
                conn.execute("INSERT OR IGNORE INTO shift_towns(shift_id, town_id) VALUES (?, ?)", (shift["id"], town["id"]))
        conn.commit()
    return RedirectResponse("/shifts", status_code=303)


@app.get("/admin/areas", response_class=HTMLResponse)
def areas_page(request: Request, user=Depends(require_admin)):
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
    now = datetime.now().isoformat(timespec="seconds")
    if district_id:
        execute("UPDATE districts SET depot_id=?, name=? WHERE id=?", (depot_id, name, district_id))
    else:
        execute("INSERT OR IGNORE INTO districts(depot_id, name, created_at) VALUES (?, ?, ?)", (depot_id, name, now))
    return RedirectResponse("/admin/areas", status_code=303)


@app.post("/admin/areas/districts/{district_id}/delete")
def delete_district(district_id: int, user=Depends(require_admin)):
    execute("UPDATE districts SET active=0 WHERE id=?", (district_id,))
    execute("UPDATE towns SET active=0 WHERE district_id=?", (district_id,))
    return RedirectResponse("/admin/areas", status_code=303)


@app.post("/admin/areas/towns")
def save_town(town_id: str = Form(""), district_id: int = Form(...), name: str = Form(...), user=Depends(require_admin)):
    now = datetime.now().isoformat(timespec="seconds")
    if town_id:
        execute("UPDATE towns SET district_id=?, name=? WHERE id=?", (district_id, name, town_id))
    else:
        execute("INSERT OR IGNORE INTO towns(district_id, name, created_at) VALUES (?, ?, ?)", (district_id, name, now))
    return RedirectResponse("/admin/areas", status_code=303)


@app.post("/admin/areas/towns/{town_id}/delete")
def delete_town(town_id: int, user=Depends(require_admin)):
    execute("UPDATE towns SET active=0 WHERE id=?", (town_id,))
    return RedirectResponse("/admin/areas", status_code=303)


@app.get("/admin/safety", response_class=HTMLResponse)
def admin_safety(request: Request, user=Depends(require_admin)):
    logs = query_all("""SELECT w.*, u.name FROM work_logs w JOIN users u ON u.id=w.user_id ORDER BY w.logged_at DESC LIMIT 100""")
    members = query_all("SELECT id, name FROM users WHERE role='member' ORDER BY active DESC, name")
    return render(request, "admin_safety.html", {"logs": logs, "members": members})


@app.get("/admin/safety/print", response_class=HTMLResponse)
def admin_safety_print(request: Request, ym: Optional[str] = None, member_id: Optional[int] = None, user=Depends(require_admin)):
    start, end = month_bounds(ym)
    member = None
    if member_id:
        member = query_one("SELECT * FROM users WHERE id=? AND role='member'", (member_id,))
    if not member:
        member = query_one("SELECT * FROM users WHERE role='member' ORDER BY active DESC, name LIMIT 1")
    if not member:
        return render(request, "safety_print.html", {"records": [], "ym": start.strftime("%Y-%m"), "period": f"{start.isoformat()} ～ {end.isoformat()}", "member": None})
    logs = query_all(
        """SELECT w.*, u.name FROM work_logs w JOIN users u ON u.id=w.user_id
           WHERE w.user_id=? AND w.work_date BETWEEN ? AND ? ORDER BY w.work_date, w.log_type""",
        (member["id"], start.isoformat(), end.isoformat()),
    )
    issues = query_all(
        """SELECT v.*, u.name FROM vehicle_issues v JOIN users u ON u.id=v.user_id
           WHERE v.user_id=? AND v.issue_date BETWEEN ? AND ? ORDER BY v.issue_date""",
        (member["id"], start.isoformat(), end.isoformat()),
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
@app.get("/login")
def login(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})
