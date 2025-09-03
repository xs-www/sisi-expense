import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
import os, uuid, hashlib, time

DB_PATH = "DB/app.db"

# ---- Database initialization ----
SCHEMAS: Dict[str, str] = {
    "users": """
CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  user_name TEXT NOT NULL,
  hashed_password TEXT NOT NULL
);
""",
    "ledgers": """
CREATE TABLE IF NOT EXISTS ledgers (
  ledger_id TEXT PRIMARY KEY,
  creator_id TEXT NOT NULL,
  ledger_name TEXT NOT NULL,
  create_time TEXT NOT NULL,
  FOREIGN KEY (creator_id) REFERENCES users(user_id)
    ON DELETE RESTRICT ON UPDATE CASCADE
);
""",
    "user_ledgers": """
CREATE TABLE IF NOT EXISTS user_ledgers (
  user_id   TEXT NOT NULL,
  ledger_id TEXT NOT NULL,
  PRIMARY KEY (user_id, ledger_id),
  FOREIGN KEY (user_id)  REFERENCES users(user_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (ledger_id) REFERENCES ledgers(ledger_id)
    ON DELETE CASCADE ON UPDATE CASCADE
);
""",
    "transactions": """
CREATE TABLE IF NOT EXISTS transactions (
  transaction_id TEXT PRIMARY KEY,
  ledger_id   TEXT NOT NULL,
  payer_id    TEXT NOT NULL,
  price       NUMERIC(12,2) NOT NULL,
  description TEXT,
  payment_time TEXT NOT NULL,
  FOREIGN KEY (ledger_id) REFERENCES ledgers(ledger_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (payer_id)  REFERENCES users(user_id)
    ON DELETE RESTRICT ON UPDATE CASCADE
);
""",
    "user_transactions": """
CREATE TABLE IF NOT EXISTS user_transactions (
  user_id        TEXT NOT NULL,
  transaction_id TEXT NOT NULL,
  PRIMARY KEY (user_id, transaction_id),
  FOREIGN KEY (user_id)        REFERENCES users(user_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
    ON DELETE CASCADE ON UPDATE CASCADE
);
"""
}

def uuid7() -> str:
    """
    生成 UUIDv7（基于时间的可排序 UUID），返回字符串形式。
    Python 3.11+ 可直接用 uuid.uuid7()。
    """
    # 当前时间戳（毫秒）
    ts_ms = int(time.time() * 1000)
    # 48 位时间戳，高位放前
    time_high = (ts_ms >> 28) & 0xFFFFFFFF
    time_mid = (ts_ms >> 12) & 0xFFFF
    time_low = ts_ms & 0xFFF

    rand_a = int.from_bytes(os.urandom(2), "big")
    rand_b = int.from_bytes(os.urandom(6), "big")

    # 拼接成 128bit
    msb = (time_high << 32) | (time_mid << 16) | (0x7000 | time_low)  # 版本 7
    lsb = (rand_a << 48) | rand_b
    return str(uuid.UUID(int=((msb << 64) | lsb)))

INDEXES: List[str] = [
    "CREATE INDEX IF NOT EXISTS idx_ledgers_creator ON ledgers(creator_id);",
    "CREATE INDEX IF NOT EXISTS idx_tx_ledger       ON transactions(ledger_id);",
    "CREATE INDEX IF NOT EXISTS idx_tx_payer        ON transactions(payer_id);",
    "CREATE INDEX IF NOT EXISTS idx_ul_ledger       ON user_ledgers(ledger_id);",
    "CREATE INDEX IF NOT EXISTS idx_ut_tx           ON user_transactions(transaction_id);",
]

def CreateDB(db_path: str = "app.db") -> dict:
    """
    初始化/补全多人记账系统数据库。
    - 若表已存在则跳过，仅创建缺失项
    - 开启 foreign_keys、WAL 等基础 PRAGMA
    返回:
        {
          "db_path": "...",
          "existed": [...],
          "created": [...],
          "indexes_created": N
        }
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 3000;")

        # 查询现有表
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        existing = {row[0] for row in cur.fetchall()}

        created = []
        # 按依赖顺序建表：users -> ledgers -> user_ledgers -> transactions -> user_transactions
        order = ["users", "ledgers", "user_ledgers", "transactions", "user_transactions"]
        for name in order:
            if name not in existing:
                conn.executescript(SCHEMAS[name])
                created.append(name)

        # 索引
        idx_created = 0
        for stmt in INDEXES:
            conn.execute(stmt)
            idx_created += 1

        conn.commit()
        return {
            "db_path": str(Path(db_path).resolve()),
            "existed": sorted(existing.intersection(set(order))),
            "created": created,
            "indexes_created": idx_created
        }
    finally:
        conn.close()

# ---------- 连接工具 ----------
def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    # 仅底层交互：开启外键与合理超时
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 3000;")
    return conn

# ---------- 1) 注册：register(user_name, hashed_password) ----------
def register_user(db_path: str, user_name: str, hashed_password: str) -> str:
    """
    新建用户（不做任何业务校验/去重校验；冲突将抛出 sqlite3.IntegrityError）
    :return: user_id (uuid7)
    """
    user_id = uuid7()
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO users (user_id, user_name, hashed_password) VALUES (?, ?, ?);",
            (user_id, user_name, hashed_password),
        )
    return user_id

# ---------- 2) 登陆：login(user_name, hashed_password) ----------
def login(db_path: str, user_name: str, hashed_password: str) -> str | None:
    """
    仅比对哈希是否一致；一致返回 user_id，不一致/不存在返回 None
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT user_id, hashed_password FROM users WHERE user_name = ? LIMIT 1;",
            (user_name,),
        ).fetchone()
    if not row:
        return None
    user_id, stored_hash = row
    return user_id if stored_hash == hashed_password else None

# ---------- 3) 添加账本：add_ledger(user_id) ----------
def add_ledger(db_path: str, user_id: str, ledger_name: str | None = None) -> str:
    """
    创建账本:
      - ledger_id 使用 uuid7
      - ledger_name 可选；若 None/空，则设为 "{user_name} 的账本 YYYY-MM-DD"
      - create_time 自动写入当前 UTC 时间 "YYYY-MM-DD HH:MM:SS"
    返回 ledger_id
    """
    ledger_id = uuid7()
    now_date = time.strftime("%Y-%m-%d", time.gmtime())
    now_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    with _connect(db_path) as conn:
        # 取 user_name
        row = conn.execute(
            "SELECT user_name FROM users WHERE user_id = ? LIMIT 1;", (user_id,)
        ).fetchone()
        if not row:
            raise sqlite3.OperationalError("user_id not found")
        creator_name = row[0]

        # 生成默认账本名
        if not ledger_name or ledger_name.strip() == "":
            ledger_name = f"{creator_name} 的账本 {now_date}"

        # 插入
        conn.execute(
            """
            INSERT INTO ledgers (ledger_id, creator_id, ledger_name, create_time)
            VALUES (?, ?, ?, ?);
            """,
            (ledger_id, user_id, ledger_name, now_datetime),
        )

    return ledger_id

# ---------- 4) 添加交易：add_transaction(ledger_id, price, description) ----------
def add_transaction(
    db_path: str,
    ledger_id: str,
    payer_id: str,
    price: float,
    description: str | None = None,
    payment_time: str | None = None
) -> str:
    """
    创建交易记录：
      - ledger_id 必须存在
      - payer_id 必须存在
      - price 金额（浮点型，精度由上层保证）
      - description 描述，可为空
      - payment_time 可选；若 None，则默认当前 UTC 日期 YYYY-MM-DD
      - 自动写入 user_transactions，把 payer_id 与该交易关联
    返回 transaction_id (uuid7)
    """
    tx_id = uuid7()
    if payment_time is None:
        payment_time = time.strftime("%Y-%m-%d", time.gmtime())

    with _connect(db_path) as conn:
        # 检查账本存在（外键也会检查，但这里可提前明确）
        row = conn.execute(
            "SELECT 1 FROM ledgers WHERE ledger_id = ? LIMIT 1;", (ledger_id,)
        ).fetchone()
        if not row:
            raise sqlite3.OperationalError("ledger_id not found")

        # 插入交易
        conn.execute(
            """
            INSERT INTO transactions
              (transaction_id, ledger_id, payer_id, price, description, payment_time)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (tx_id, ledger_id, payer_id, price, description, payment_time),
        )

        # 自动添加交易关联（付款人参与交易）
        conn.execute(
            "INSERT INTO user_transactions (user_id, transaction_id) VALUES (?, ?);",
            (payer_id, tx_id),
        )

    return tx_id

# ---------- 5) 计算支出：compute_expense(ledger_id) ----------
def compute_expense(db_path: str, ledger_id: str) -> float:
    """
    返回该账本下所有交易 price 之和（None 视为 0）。
    （不做币种/精度处理，上层可自行用 Decimal）
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(price), 0) FROM transactions WHERE ledger_id = ?;",
            (ledger_id,),
        ).fetchone()
    return float(row[0]) if row else 0.0

# ---------- 6) 为账本添加关联用户：link_user_to_ledger(ledger_id, user_id) ----------
def link_user_to_ledger(db_path: str, ledger_id: str, user_id: str) -> None:
    """
    在 user_ledgers 写一条关联（重复插入将触发主键冲突；外键不匹配会抛错）
    """
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO user_ledgers (user_id, ledger_id) VALUES (?, ?);",
            (user_id, ledger_id),
        )

# ---------- 7) 为交易添加关联用户：link_user_to_transaction(transaction_id, user_id) ----------
def link_user_to_transaction(db_path: str, transaction_id: str, user_id: str) -> None:
    """
    在 user_transactions 写一条关联（重复/外键问题交由数据库报错）
    """
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO user_transactions (user_id, transaction_id) VALUES (?, ?);",
            (user_id, transaction_id),
        )

# ---------- 8) 获取账本信息：get_ledger_info(ledger_id) ----------
def get_ledger_info(db_path: str, ledger_id: str) -> dict:
    """
    获取账本信息:
      - 名称: ledgers.ledger_name
      - 参与用户: user_ledgers 中的 user_id；若未包含创建者，则补充 creator_id
      - 创建日期: 将 ledgers.create_time 转为 "MM-YY"（如 "09-25"）

    :raises sqlite3.OperationalError: 当 ledger_id 不存在时
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT ledger_name, create_time, creator_id FROM ledgers WHERE ledger_id = ? LIMIT 1;",
            (ledger_id,),
        ).fetchone()

        if not row:
            raise sqlite3.OperationalError("ledger_id not found")

        ledger_name, create_time, creator_id = row

        # 收集关联用户（来自 user_ledgers）
        users = {u[0] for u in conn.execute(
            "SELECT user_id FROM user_ledgers WHERE ledger_id = ?;",
            (ledger_id,)
        ).fetchall()}

        # 确保将创建者也计入 involved_user（即使未写入 user_ledgers）
        users.add(creator_id)

    # 生成 "MM-YY" 的 created_date
    # create_time 约定为 "YYYY-MM-DD HH:MM:SS"（或至少以 "YYYY-MM" 开头）
    s = (create_time or "").strip()
    # 取出 "YYYY-MM" 前 7 个字符
    yymm = s[:7] if len(s) >= 7 else ""
    if len(yymm) == 7 and yymm[4] == "-":
        yyyy = yymm[:4]
        mm = yymm[5:7]
        created_date = f"{mm}-{yyyy[-2:]}"
    else:
        # 兜底：无法解析时给空字符串
        created_date = ""

    return {
        "ledger_name": ledger_name,
        "involved_user": sorted(users),
        "created_date": created_date,
    }

# ---------- 9) 获取用户信息：get_user_info(user_id) ----------
def get_user_info(db_path: str, user_id: str) -> dict:
    """
    获取用户信息:
      - user_name: 来自 users 表
      - ledgers: 该用户参与的所有账本（包括自己创建的 + user_ledgers 关联的）
                 每个元素通过复用 get_ledger_info 返回结构
    :raises sqlite3.OperationalError: 当 user_id 不存在时
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT user_name FROM users WHERE user_id = ? LIMIT 1;", (user_id,)
        ).fetchone()
        if not row:
            raise sqlite3.OperationalError("user_id not found")
        user_name = row[0]

        # 1. 作为创建者的账本
        creator_ledgers = {
            r[0] for r in conn.execute(
                "SELECT ledger_id FROM ledgers WHERE creator_id = ?;", (user_id,)
            ).fetchall()
        }

        # 2. 通过 user_ledgers 参与的账本
        joined_ledgers = {
            r[0] for r in conn.execute(
                "SELECT ledger_id FROM user_ledgers WHERE user_id = ?;", (user_id,)
            ).fetchall()
        }

        # 合并去重
        all_ledgers = creator_ledgers.union(joined_ledgers)

    # 复用 get_ledger_info
    ledgers_info = []
    for lid in sorted(all_ledgers):
        try:
            ledgers_info.append(get_ledger_info(db_path, lid))
        except sqlite3.OperationalError:
            # 如果账本已被删除，跳过
            continue

    return {
        "user_name": user_name,
        "ledgers": ledgers_info,
    }
