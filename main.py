from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Literal
from datetime import datetime, date
import sqlite3
import json
import uuid
import os

API_KEY = os.environ.get("API_KEY", "dev-key")  # setear en producción

app = FastAPI(title="Expenses Tracking API", version="1.0.0")

DB_PATH = os.environ.get("DB_PATH", "app.db")


# -------------------- DB helpers --------------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS schema_meta (
      name TEXT PRIMARY KEY,
      version TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS schema_fields (
      key TEXT PRIMARY KEY,
      label TEXT NOT NULL,
      type TEXT NOT NULL,
      required INTEGER NOT NULL DEFAULT 0,
      enabled INTEGER NOT NULL DEFAULT 1,
      description TEXT,
      enum_values TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS expenses (
      id TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      schema_version TEXT NOT NULL,
      data TEXT NOT NULL
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS aliases (
      id TEXT PRIMARY KEY,
      kind TEXT NOT NULL,
      alias TEXT NOT NULL,
      value TEXT NOT NULL,
      created_at TEXT NOT NULL
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_aliases_kind_alias ON aliases(kind, alias)")

    conn.commit()
    conn.close()

    seed_schema_if_empty()


def bump_schema_version():
    # versión simple: YYYY-MM-DD.N
    today = date.today().isoformat()
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT version FROM schema_meta WHERE name='expense'")
    row = cur.fetchone()

    if not row:
        version = f"{today}.1"
    else:
        prev = row["version"]
        if prev.startswith(today):
            # incrementa N
            try:
                n = int(prev.split(".")[-1])
                version = f"{today}.{n+1}"
            except:
                version = f"{today}.1"
        else:
            version = f"{today}.1"

    cur.execute("""
      INSERT INTO schema_meta(name, version, updated_at)
      VALUES('expense', ?, ?)
      ON CONFLICT(name) DO UPDATE SET version=excluded.version, updated_at=excluded.updated_at
    """, (version, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    return version


def get_schema_version():
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT version FROM schema_meta WHERE name='expense'")
    row = cur.fetchone()
    conn.close()
    if not row:
        return bump_schema_version()
    return row["version"]


def seed_schema_if_empty():
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as c FROM schema_fields")
    c = cur.fetchone()["c"]
    if c == 0:
        # esquema inicial mínimo
        fields = [
            ("date", "Fecha", "date", 1, 1, "Fecha del gasto", None),
            ("amount", "Monto", "number", 1, 1, "Monto total", None),
            ("currency", "Moneda", "enum", 1, 1, "Código moneda", json.dumps(["ARS", "USD"])),
            ("vendor", "Proveedor", "string", 0, 1, "Proveedor / comercio", None),
            ("category", "Categoría", "string", 0, 1, "Eje principal", None),
            ("payment_method", "Medio de pago", "string", 0, 1, "Efectivo / tarjeta / etc.", None),
            ("client", "Cliente", "string", 0, 1, "Cliente (si aplica)", None),
            ("concept", "Concepto", "string", 0, 1, "Descripción breve", None),
            ("notes", "Notas", "string", 0, 1, "Observaciones", None),
            ("status", "Estado", "enum", 0, 1, "pending_confirmation/confirmed/rejected",
             json.dumps(["pending_confirmation", "confirmed", "rejected"])),
        ]
        cur.executemany("""
          INSERT INTO schema_fields(key,label,type,required,enabled,description,enum_values)
          VALUES(?,?,?,?,?,?,?)
        """, fields)
        bump_schema_version()
    conn.commit()
    conn.close()


def require_key(x_api_key: Optional[str]):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def load_fields():
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM schema_fields")
    rows = cur.fetchall()
    conn.close()
    fields = []
    for r in rows:
        enum_values = json.loads(r["enum_values"]) if r["enum_values"] else None
        fields.append({
            "key": r["key"],
            "label": r["label"],
            "type": r["type"],
            "required": bool(r["required"]),
            "enabled": bool(r["enabled"]),
            "description": r["description"],
            "enum_values": enum_values
        })
    return fields


def validate_against_schema(payload: Dict[str, Any]):
    fields = load_fields()
    enabled = {f["key"]: f for f in fields if f["enabled"]}
    # required check
    for f in fields:
        if f["enabled"] and f["required"]:
            if payload.get(f["key"]) is None:
                raise HTTPException(status_code=400, detail=f"Missing required field: {f['key']}")
    # basic enum check
    for k, v in payload.items():
        if k in enabled and enabled[k]["type"] == "enum":
            opts = enabled[k].get("enum_values") or []
            if opts and v not in opts:
                raise HTTPException(status_code=400, detail=f"Invalid enum for {k}: {v}. Allowed: {opts}")


# -------------------- Models --------------------
FieldType = Literal["string", "number", "integer", "boolean", "date", "datetime", "enum"]

class CreateFieldRequest(BaseModel):
    key: str
    label: str
    type: FieldType
    required: Optional[bool] = False
    enabled: Optional[bool] = True
    description: Optional[str] = None
    enum_values: Optional[List[str]] = None

class UpdateFieldRequest(BaseModel):
    label: Optional[str] = None
    type: Optional[FieldType] = None
    required: Optional[bool] = None
    enabled: Optional[bool] = None
    description: Optional[str] = None
    enum_values: Optional[List[str]] = None

class ExpenseInput(BaseModel):
    date: date
    amount: float
    currency: str
    vendor: Optional[str] = None
    client: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    payment_method: Optional[str] = None
    concept: Optional[str] = None
    receipt_type: Optional[str] = None
    receipt_number: Optional[str] = None
    notes: Optional[str] = None
    attachment_url: Optional[str] = None
    attachment_id: Optional[str] = None
    source: Optional[str] = None
    confidence: Optional[float] = None
    status: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None

class UpdateExpenseRequest(BaseModel):
    status: Optional[str] = None
    data: Optional[ExpenseInput] = None

AliasKind = Literal["vendor","client","category","subcategory","payment_method","concept","project","cost_center"]

class CreateAliasRequest(BaseModel):
    kind: AliasKind
    alias: str
    value: str

# -------------------- Startup --------------------
@app.on_event("startup")
def startup():
    init_db()

# -------------------- /schema --------------------
@app.get("/schema/expense")
def get_expense_schema(x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    version = get_schema_version()
    return {
        "name": "expense",
        "version": version,
        "updated_at": datetime.utcnow().isoformat(),
        "fields": load_fields()
    }

@app.post("/schema/expense/fields")
def create_field(req: CreateFieldRequest, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT key FROM schema_fields WHERE key=?", (req.key,))
    if cur.fetchone():
        conn.close()
        raise HTTPException(status_code=409, detail="Field key already exists")

    enum_values = json.dumps(req.enum_values) if req.enum_values else None
    cur.execute("""
      INSERT INTO schema_fields(key,label,type,required,enabled,description,enum_values)
      VALUES(?,?,?,?,?,?,?)
    """, (req.key, req.label, req.type, int(bool(req.required)), int(bool(req.enabled)), req.description, enum_values))

    conn.commit()
    conn.close()
    bump_schema_version()
    return get_expense_schema(x_api_key)

@app.patch("/schema/expense/fields/{field_key}")
def update_field(field_key: str, req: UpdateFieldRequest,
                 x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM schema_fields WHERE key=?", (field_key,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Field not found")

    updates = {}
    for k, v in req.model_dump(exclude_none=True).items():
        if k == "enum_values":
            updates["enum_values"] = json.dumps(v) if v is not None else None
        else:
            updates[k] = v

    # build SQL
    if not updates:
        conn.close()
        return get_expense_schema(x_api_key)

    cols = []
    vals = []
    for k, v in updates.items():
        if k in ["required", "enabled"]:
            v = int(bool(v))
        cols.append(f"{k}=?")
        vals.append(v)
    vals.append(field_key)

    cur.execute(f"UPDATE schema_fields SET {', '.join(cols)} WHERE key=?", vals)
    conn.commit()
    conn.close()
    bump_schema_version()
    return get_expense_schema(x_api_key)

@app.delete("/schema/expense/fields/{field_key}")
def delete_field(field_key: str, hard: bool = False,
                 x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT key FROM schema_fields WHERE key=?", (field_key,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Field not found")

    if hard:
        cur.execute("DELETE FROM schema_fields WHERE key=?", (field_key,))
    else:
        cur.execute("UPDATE schema_fields SET enabled=0 WHERE key=?", (field_key,))
    conn.commit()
    conn.close()
    bump_schema_version()
    return get_expense_schema(x_api_key)

# -------------------- /expenses --------------------
@app.post("/expenses")
def create_expense(expense: ExpenseInput, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    payload = expense.model_dump()
    validate_against_schema(payload)

    exp_id = f"exp_{uuid.uuid4().hex[:16]}"
    now = datetime.utcnow().isoformat()
    schema_version = get_schema_version()

    conn = db()
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO expenses(id, created_at, updated_at, schema_version, data)
      VALUES(?,?,?,?,?)
    """, (exp_id, now, now, schema_version, json.dumps(payload)))
    conn.commit()
    conn.close()
    return {"id": exp_id, "status": payload.get("status") or "confirmed", "stored": True}

@app.get("/expenses")
def list_expenses(from_: Optional[date] = None, to: Optional[date] = None,
                  vendor: Optional[str] = None, client: Optional[str] = None,
                  category: Optional[str] = None, status: Optional[str] = None,
                  q: Optional[str] = None, cursor: Optional[str] = None, limit: int = 50,
                  x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM expenses ORDER BY created_at DESC LIMIT ?", (min(max(limit,1),200),))
    rows = cur.fetchall()
    conn.close()

    items = []
    for r in rows:
        data = json.loads(r["data"])
        # filters (simple)
        d = date.fromisoformat(data["date"])
        if from_ and d < from_: 
            continue
        if to and d > to:
            continue
        if vendor and (data.get("vendor") or "").lower() != vendor.lower():
            continue
        if client and (data.get("client") or "").lower() != client.lower():
            continue
        if category and (data.get("category") or "").lower() != category.lower():
            continue
        if status and (data.get("status") or "").lower() != status.lower():
            continue
        if q:
            blob = json.dumps(data, ensure_ascii=False).lower()
            if q.lower() not in blob:
                continue

        items.append({
            "id": r["id"],
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
            "schema_version": r["schema_version"],
            **data
        })

    return {"items": items, "next_cursor": None}

@app.get("/expenses/{expense_id}")
def get_expense(expense_id: str, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM expenses WHERE id=?", (expense_id,))
    r = cur.fetchone()
    conn.close()
    if not r:
        raise HTTPException(status_code=404, detail="Not found")
    data = json.loads(r["data"])
    return {
        "id": r["id"],
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
        "schema_version": r["schema_version"],
        **data
    }

@app.patch("/expenses/{expense_id}")
def update_expense(expense_id: str, req: UpdateExpenseRequest,
                   x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM expenses WHERE id=?", (expense_id,))
    r = cur.fetchone()
    if not r:
        conn.close()
        raise HTTPException(status_code=404, detail="Not found")

    data = json.loads(r["data"])
    if req.data is not None:
        new_data = req.data.model_dump()
        # merge
        data.update({k:v for k,v in new_data.items() if v is not None})
    if req.status is not None:
        data["status"] = req.status

    validate_against_schema(data)

    now = datetime.utcnow().isoformat()
    cur.execute("UPDATE expenses SET updated_at=?, data=? WHERE id=?", (now, json.dumps(data), expense_id))
    conn.commit()
    conn.close()
    return get_expense(expense_id, x_api_key)

# -------------------- /aliases --------------------
@app.post("/aliases")
def create_alias(req: CreateAliasRequest, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM aliases WHERE kind=? AND alias=?", (req.kind, req.alias.lower()))
    if cur.fetchone():
        conn.close()
        raise HTTPException(status_code=409, detail="Alias already exists")
    al_id = f"al_{uuid.uuid4().hex[:16]}"
    now = datetime.utcnow().isoformat()
    cur.execute("INSERT INTO aliases(id, kind, alias, value, created_at) VALUES(?,?,?,?,?)",
                (al_id, req.kind, req.alias.lower(), req.value, now))
    conn.commit()
    conn.close()
    return {"id": al_id, "kind": req.kind, "alias": req.alias.lower(), "value": req.value, "created_at": now}

@app.get("/aliases")
def list_aliases(kind: Optional[str] = None, alias: Optional[str] = None, value: Optional[str] = None,
                 cursor: Optional[str] = None, limit: int = 50,
                 x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM aliases ORDER BY created_at DESC LIMIT ?", (min(max(limit,1),200),))
    rows = cur.fetchall()
    conn.close()

    items = []
    for r in rows:
        if kind and r["kind"] != kind:
            continue
        if alias and alias.lower() not in r["alias"]:
            continue
        if value and value.lower() not in r["value"].lower():
            continue
        items.append({"id": r["id"], "kind": r["kind"], "alias": r["alias"], "value": r["value"], "created_at": r["created_at"]})
    return {"items": items, "next_cursor": None}

@app.delete("/aliases/{alias_id}")
def delete_alias(alias_id: str, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM aliases WHERE id=?", (alias_id,))
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(status_code=404, detail="Not found")
    conn.commit()
    conn.close()
    return {"deleted": True, "id": alias_id}
