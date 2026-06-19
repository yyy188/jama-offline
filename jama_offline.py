#!/usr/bin/env python3
"""
jama_offline.py - Bulk-download Jama Connect projects into per-project SQLite caches, then search OFFLINE.
Cross-platform (Windows / Linux / macOS). Core (projects/query/sync/status) is pure standard library;
`search`/`semantic` add vector search via fastembed + sqlite-vec (auto-installed on first use).

Why: the Jama REST API caps pages at 50 items and has no bulk-get, so any live search re-pages the whole
project (tens of seconds). This downloads a project ONCE (every item, every field) into a local SQLite
file with an FTS5 index + a vector index, after which searches are millisecond-fast. Caches are PERSISTENT:
each use FIRST incrementally pulls items changed since last sync (by modifiedDate) and upserts them — no
expiry, no auto-delete. Server-side deletions aren't tracked; `rebuild` = clean full re-download.

Commands:
    login     --base <url> --client-id <id> --client-secret <s>   save creds to a user-level file (once)
    logout                                                        remove the saved credentials
    projects  --project <regex>                       resolve/list matching projects (the gate)
    sync      --project <id|name>[,..]                 create-or-incrementally-update up to 5 projects
              [--no-vectors] [--with-links] [--link-cap N]        (also builds/maintains the vector index)
    rebuild   --project <id|name>[,..]                 force a clean FULL re-download (drops deletions too)
    status    [--project <id|name>[,..]]               caches: last-sync, watermark, counts, size, vectors
    search    --project <id> --keyword a,b | --query "..."   HYBRID: FTS + LIKE + vector, RRF-fused/de-duped
              [--type REQ,FEAT] [--top N] [--max-distance D] [--match any|all] [--field name|all] [--json]
    semantic  --project <id> --query "..."             pure vector (KNN) search; [--max-distance D] [--top N]
    query     --project <id> --sql "SELECT ..."        read-only SQL — USE THIS for counts/stats/aggregates
    purge     --project <id|name>[,..] | --all         delete cache file(s) (incl. the vector index)

Storage: per-user dir (override with $JAMA_OFFLINE_DIR): jama-proj-<id>.db (main) + jama-proj-<id>.vec.db
    (vectors) + credentials.json + models/. Windows %LOCALAPPDATA%\\jama-offline · macOS ~/Library/
    Application Support/jama-offline · Linux $XDG_DATA_HOME/jama-offline or ~/.local/share/jama-offline.

Credentials (first found wins): env JAMA_BASE/JAMA_CLIENT_ID/JAMA_CLIENT_SECRET; else user-level
credentials.json (saved by `login`); else config.local.json next to this script; else config.local.ps1.
"""

import argparse
import base64
import hashlib
import http.client
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent
ENGINE_VERSION = "4.0.0-py"
# v4: caches are PERSISTENT (no TTL/expiry). Each use incrementally syncs items whose modifiedDate is
# >= the cache's watermark (= MAX(modifiedDate)), upserts them, and rebuilds the FTS index. Deletions
# on the server are NOT tracked (only adds/changes) — use `rebuild` for a clean full re-download.
SCHEMA_VERSION = "4"
MAX_PROJECTS = 5
PAGE = 50
# 16 is the measured sweet spot on this Jama instance for the big initial sweep (ProjectA, 201 pages /
# 10k items): ~30% faster than 8 (≈50s vs ≈71s) AND faster + far more stable than 32 (≈53s but
# swinging 46-61s — the server-side rate limit starts queueing/penalising past 16). All levels return
# every page (data-safe); 429s self-heal via api_get's backoff. Lower it if the instance throttles.
CONCURRENCY_DEFAULT = 16

_cfg = {}  # BASE / CID / CSEC, populated lazily by ensure_credentials() on first network use


# ============================ cross-platform paths ============================
def tmp_root():
    return Path(tempfile.gettempdir())


def user_data_dir():
    if env := os.environ.get("JAMA_OFFLINE_DIR"):
        d = Path(env)
    elif sys.platform == "win32":
        d = Path(os.environ.get("LOCALAPPDATA") or Path.home()) / "jama-offline"
    elif sys.platform == "darwin":
        d = Path.home() / "Library" / "Application Support" / "jama-offline"
    else:
        d = Path(os.environ.get("XDG_DATA_HOME") or Path.home() / ".local" / "share") / "jama-offline"
    d.mkdir(parents=True, exist_ok=True)
    return d


def db_path(proj_id):
    return user_data_dir() / f"jama-proj-{proj_id}.db"


def creds_file():
    """User-level credential store, INDEPENDENT of the skill folder (so secrets never live next to the
    shared/synced code). Windows uses LOCALAPPDATA (non-roaming = doesn't sync to a domain server)."""
    return user_data_dir() / "credentials.json"


CACHE_GLOB = "jama-proj-*.db"
CACHE_RE = re.compile(r"^jama-proj-(\d+)\.db$")  # anchored: ignores siblings like jama-proj-12345.vec.db


def _ro_uri(p):
    return f"{Path(p).resolve().as_uri()}?mode=ro"


def read_meta(proj_id):
    """Return the cache's meta dict, or None if the file is absent / unreadable / corrupt. No network,
    read-only open — safe to call for offline name resolution and status."""
    p = db_path(proj_id)
    if not p.exists():
        return None
    try:
        con = sqlite3.connect(_ro_uri(p), uri=True)
        try:
            return dict(con.execute("SELECT key, value FROM meta").fetchall())
        finally:
            con.close()
    except sqlite3.Error:
        return None


def cached_project_ids():
    """Project ids that currently have a cache file (anchored match, excludes .vec.db & other siblings)."""
    return sorted(int(m.group(1)) for f in user_data_dir().glob(CACHE_GLOB)
                  if (m := CACHE_RE.match(f.name)))


# ============================ credentials ============================
CRED_KEYS = ("JAMA_BASE", "JAMA_CLIENT_ID", "JAMA_CLIENT_SECRET")


def _parse_ps1(path):
    out = {}
    try:
        txt = path.read_text(encoding="utf-8-sig", errors="ignore")
    except OSError:
        return out
    for k in CRED_KEYS:
        if m := re.search(k + r"""\s*=\s*['"]([^'"]+)['"]""", txt):
            out[k] = m.group(1)
    return out


def _merge_json_creds(path, creds):
    """Fill any still-missing creds from a JSON file (BOM-tolerant). Returns the merged dict."""
    if all(creds.values()) or not path.exists():
        return creds
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        return {k: creds[k] or data.get(k) for k in CRED_KEYS}
    except (OSError, json.JSONDecodeError):
        return creds


def save_credentials(base, cid, csec):
    """Persist creds to the user-level store so the user logs in ONCE. Owner-only perms where supported."""
    f = creds_file()
    f.write_text(json.dumps({"JAMA_BASE": base.rstrip("/"), "JAMA_CLIENT_ID": cid,
                             "JAMA_CLIENT_SECRET": csec}, indent=2), encoding="utf-8")
    try:
        os.chmod(f, 0o600)  # POSIX: rw owner only. Best-effort on Windows (ACLs already user-scoped).
    except OSError:
        pass
    return f


def ensure_credentials():
    """Populate _cfg, idempotently. Source order (first non-empty wins per field): env vars → user-level
    credentials.json (the persistent 'login', INDEPENDENT of the skill folder) → skill-dir config.local.json
    → config.local.ps1. Only the FIRST network-touching call pays for it, so offline commands need none."""
    if _cfg:
        return
    creds = {k: os.environ.get(k) for k in CRED_KEYS}
    creds = _merge_json_creds(creds_file(), creds)                      # user-level persistent store
    creds = _merge_json_creds(SKILL_DIR / "config.local.json", creds)   # back-compat (skill folder)
    if not all(creds.values()):
        for cand in (SKILL_DIR / "config.local.ps1",
                     SKILL_DIR.parent / "jama-query" / "config.local.ps1"):
            if cand.exists():
                parsed = _parse_ps1(cand)
                creds = {k: creds[k] or parsed.get(k) for k in CRED_KEYS}
                if all(creds.values()):
                    break

    if not all(creds.values()):
        missing = ", ".join(k for k in CRED_KEYS if not creds[k])
        sys.exit(f"Missing credentials: {missing}. Run once:  jama_offline.py login --base <url> "
                 f"--client-id <id> --client-secret <secret>  (saved to {creds_file()}); "
                 f"or set env vars.")

    _cfg["BASE"] = creds["JAMA_BASE"].rstrip("/")
    _cfg["CID"] = creds["JAMA_CLIENT_ID"]
    _cfg["CSEC"] = creds["JAMA_CLIENT_SECRET"]


# ============================ auth + HTTP ============================
def _token_file():
    key = hashlib.md5(f"{_cfg['BASE']}|{_cfg['CID']}".encode()).hexdigest()
    return tmp_root() / f"jama_token_{key}.json"


def get_token():
    f = _token_file()
    if f.exists():
        try:  # a corrupt cache file must NOT crash — just fall through and re-auth
            c = json.loads(f.read_text(encoding="utf-8"))
            if time.time() - c["fetched"] < 3300:
                return c["token"]
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            pass
    auth = base64.b64encode(f"{_cfg['CID']}:{_cfg['CSEC']}".encode()).decode()
    body = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
    req = urllib.request.Request(
        _cfg["BASE"] + "/rest/oauth/token", data=body,
        headers={"Authorization": "Basic " + auth, "Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        tok = json.loads(r.read())["access_token"]
    try:
        f.write_text(json.dumps({"token": tok, "fetched": time.time()}))
    except OSError:
        pass
    return tok


# One persistent keep-alive HTTPS connection PER worker thread (a stdlib connection pool): the 200 page
# GETs of a sync reuse a handful of connections instead of doing 200 fresh TLS handshakes -> much faster,
# and no bursts of new handshakes for the server to reset.
_local = threading.local()


def _conn():
    c = getattr(_local, "conn", None)
    if c is None:
        host = urllib.parse.urlparse(_cfg["BASE"]).netloc
        c = http.client.HTTPSConnection(host, timeout=120)
        _local.conn = c
    return c


def _drop_conn():
    c = getattr(_local, "conn", None)
    if c is not None:
        try:
            c.close()
        except OSError:
            pass
        _local.conn = None


def api_get(path, _depth=0):
    ensure_credentials()  # lazy: first network touch loads creds; offline commands never get here
    tok = get_token()
    try:
        c = _conn()
        c.request("GET", path, headers={"Authorization": "Bearer " + tok})
        resp = c.getresponse()
        body = resp.read()  # must fully read the body to reuse the keep-alive connection
    except (http.client.HTTPException, OSError) as e:
        _drop_conn()  # connection may be broken -> rebuild on retry
        if _depth < 6:
            time.sleep(min(20, 1 + _depth * 2))
            return api_get(path, _depth + 1)
        raise RuntimeError(f"GET {path} failed (network): {e}")
    if resp.status == 200:
        return json.loads(body)
    if resp.status == 429 and _depth < 6:
        time.sleep(min(30, 3 + _depth * 5))
        return api_get(path, _depth + 1)
    if resp.status == 401 and _depth < 2:
        _token_file().unlink(missing_ok=True)
        _drop_conn()
        return api_get(path, _depth + 1)
    raise RuntimeError(f"GET {path} failed: {resp.status} {body.decode(errors='ignore')[:200]}")


def get_pages(path, limit=10**9, concurrency=CONCURRENCY_DEFAULT, progress_label=None):
    """Page 1 sequentially (learn total + warm token), then the rest concurrently. With progress_label,
    shows a download progress bar on stderr (used for the project-items sweep)."""
    sep = "&" if "?" in path else "?"
    first = api_get(f"{path}{sep}startAt=0&maxResults={PAGE}")
    total = int(first["meta"]["pageInfo"]["totalResults"])
    want = min(total, limit)
    acc = list(first.get("data") or [])
    cb = _progress(progress_label, want) if (progress_label and want > PAGE) else None
    if cb:
        cb(len(acc))
    if want > PAGE:
        starts = range(PAGE, want, PAGE)
        fetch = lambda s: api_get(f"{path}{sep}startAt={s}&maxResults={PAGE}").get("data") or []
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            for d in ex.map(fetch, starts):
                acc.extend(d)
                if cb:
                    cb(min(len(acc), want))
    return acc


# ============================ reference data (projects / itemtypes / reltypes) ============================
def _ref_file(name):
    key = hashlib.md5(f"{_cfg['BASE']}|{_cfg['CID']}".encode()).hexdigest()
    return tmp_root() / f"jama_ref_{name}_{key}.json"


def get_ref(name, endpoint, ttl=21600):
    ensure_credentials()  # _ref_file() reads _cfg even on a cache hit, so creds must be loaded first
    f = _ref_file(name)
    if f.exists():
        try:  # a corrupt cache file must NOT crash — just fall through and re-fetch
            c = json.loads(f.read_text(encoding="utf-8"))
            if time.time() - c["fetched"] < ttl:
                return c["data"]
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            pass
    data = get_pages(endpoint)
    try:
        f.write_text(json.dumps({"fetched": time.time(), "data": data}), encoding="utf-8")
    except OSError:
        pass
    return data


_memo = {}


def all_projects():
    if "projects" not in _memo:
        _memo["projects"] = get_ref("projects", "/rest/v1/projects")
    return _memo["projects"]


def all_types():
    if "types" not in _memo:
        _memo["types"] = get_ref("itemtypes", "/rest/v1/itemtypes")
    return _memo["types"]


def type_maps():
    if "tmaps" not in _memo:
        key, name = {}, {}
        for t in all_types():
            key[str(t["id"])] = t.get("typeKey")
            name[str(t["id"])] = t.get("display")
        _memo["tmaps"] = (key, name)
    return _memo["tmaps"]


def reltype_map():
    if "rtmap" not in _memo:
        _memo["rtmap"] = {str(x["id"]): x.get("name") for x in get_pages("/rest/v1/relationshiptypes")}
    return _memo["rtmap"]


# ============================ project resolution (<=5 gate) ============================
def resolve_projects(tokens):
    flat = [p.strip() for tok in (tokens or []) for p in str(tok).split(",") if p.strip()]
    if not flat:
        sys.exit("--project is required (id or name; comma-separated or repeated).")
    found = {}
    for tok in flat:
        if tok.isdigit():
            # Offline-first: if a cache exists, take the name from its meta and DON'T hit the network —
            # this is what lets `search/query --project <id>` serve a fresh cache with zero API calls.
            meta = read_meta(tok)
            if meta and meta.get("project_name"):
                found[tok] = {"id": int(tok), "name": meta["project_name"]}
            else:
                pr = next((p for p in all_projects() if str(p["id"]) == tok), None)
                found[tok] = {"id": int(tok), "name": pr["fields"]["name"] if pr else f"(id {tok})"}
        else:
            hits = [p for p in all_projects() if re.search(tok, p["fields"]["name"], re.I)]
            if not hits:
                sys.exit(f"No project name matches /{tok}/. Re-express, or pass a numeric project id.")
            for h in hits:
                found[str(h["id"])] = {"id": int(h["id"]), "name": h["fields"]["name"]}
    projs = sorted(found.values(), key=lambda x: x["name"])
    if len(projs) > MAX_PROJECTS:
        listing = "\n".join(f"  {p['id']}  {p['name']}" for p in projs)
        sys.exit(f"Your request resolves to {len(projs)} projects (max {MAX_PROJECTS}). "
                 f"Please narrow / re-express. Candidates:\n{listing}")
    return projs


def single_project(tokens):
    projs = resolve_projects(tokens)
    if len(projs) != 1:
        listing = "\n".join(f"  {p['id']}  {p['name']}" for p in projs)
        sys.exit(f"This command needs exactly ONE project; got {len(projs)}:\n{listing}")
    return projs[0]


# ============================ value helpers ============================
def scalar(v):
    """Collapse 1-element arrays (some Jama numeric fields arrive boxed) so sqlite3 can bind them."""
    if isinstance(v, (list, tuple)):
        return v[0] if v else None
    return v


def field_text(v):
    if v is None or isinstance(v, str):
        return v
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (list, tuple)):
        return ";".join(x for x in map(field_text, v) if x is not None)
    try:
        return json.dumps(v, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(v)


_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
_ENTITIES = (("&nbsp;", " "), ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"), ("&quot;", '"'))


def strip_html(html):
    if not html:
        return ""
    text = _TAG.sub(" ", html)
    for a, b in _ENTITIES:
        text = text.replace(a, b)
    return _WS.sub(" ", text).strip()


# ============================ schema ============================
DDL = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE items(
  id INTEGER PRIMARY KEY, documentKey TEXT, globalId TEXT,
  itemType INTEGER, typeKey TEXT, typeName TEXT, project INTEGER,
  name TEXT, description TEXT, status TEXT, statusName TEXT, priority TEXT, priorityName TEXT,
  sequence TEXT, globalSortOrder INTEGER, parentItem INTEGER, parentProject INTEGER,
  createdDate TEXT, modifiedDate TEXT, lastActivityDate TEXT, createdBy INTEGER, modifiedBy INTEGER
);
CREATE TABLE fields_kv(itemId INTEGER, key TEXT, value TEXT);
CREATE TABLE picklist(id INTEGER PRIMARY KEY, name TEXT, pickList INTEGER);
CREATE TABLE relationships(id INTEGER PRIMARY KEY, fromItem INTEGER, toItem INTEGER,
  relationshipType INTEGER, relTypeName TEXT, suspect INTEGER);
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
-- external-content FTS5: indexes items.name + items.description with no duplicate storage; rowid = items.id
CREATE VIRTUAL TABLE fts USING fts5(name, description, content='items', content_rowid='id',
  tokenize='porter unicode61');
CREATE INDEX idx_items_typekey ON items(typeKey);
CREATE INDEX idx_items_itemtype ON items(itemType);
CREATE INDEX idx_items_name ON items(name COLLATE NOCASE);
CREATE INDEX idx_items_statusname ON items(statusName COLLATE NOCASE);
CREATE INDEX idx_items_seq ON items(sequence);
CREATE INDEX idx_kv_key ON fields_kv(key);
CREATE INDEX idx_kv_item ON fields_kv(itemId);
CREATE INDEX idx_rel_from ON relationships(fromItem);
CREATE INDEX idx_rel_to ON relationships(toItem);
"""

ITEM_COLUMNS = ("id", "documentKey", "globalId", "itemType", "typeKey", "typeName", "project",
                "name", "description", "status", "statusName", "priority", "priorityName",
                "sequence", "globalSortOrder", "parentItem", "parentProject",
                "createdDate", "modifiedDate", "lastActivityDate", "createdBy", "modifiedBy")
_MODIFIED_IDX = ITEM_COLUMNS.index("modifiedDate")  # position of modifiedDate in an item row tuple


# ============================ build a project cache ============================
def _fetch_links(items, link_cap, concurrency):
    ids = [int(it["id"]) for it in items]
    if link_cap > 0:
        ids = ids[:link_cap]

    def fetch(item_id):
        out, start = [], 0
        while True:
            try:
                d = api_get(f"/rest/v1/items/{item_id}/downstreamrelationships?startAt={start}&maxResults={PAGE}")
            except RuntimeError:
                return out  # some item types (e.g. attachments) have no relationships endpoint -> skip
            out.extend(d.get("data") or [])
            start += PAGE
            if start >= int(d["meta"]["pageInfo"]["totalResults"]):
                return out

    edges = []
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        for got in ex.map(fetch, ids):
            edges.extend(got)
    return edges


def _resolve_picklists(items, concurrency):
    ids = {str(scalar(it.get("fields", {}).get(k)))
           for it in items for k in ("status", "priority")
           if str(scalar(it.get("fields", {}).get(k) or "")).isdigit()}
    if not ids:
        return {}, []

    def fetch(opt_id):
        try:
            return opt_id, api_get(f"/rest/v1/picklistoptions/{opt_id}").get("data")
        except RuntimeError:
            return opt_id, None

    name_map, rows = {}, []
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        for opt_id, d in ex.map(fetch, ids):
            if d and d.get("name"):
                name_map[opt_id] = d["name"]
                rows.append((d["id"], d["name"], d.get("pickList")))
    return name_map, rows


def _item_row(it, tkey, tname, picks):
    f = it.get("fields") or {}
    loc = it.get("location") or {}
    par = loc.get("parent") or {}
    tid = str(scalar(it.get("itemType")))
    sid, pid = scalar(f.get("status")), scalar(f.get("priority"))
    return (
        scalar(it.get("id")), it.get("documentKey"), it.get("globalId"),
        scalar(it.get("itemType")), tkey.get(tid), tname.get(tid), scalar(it.get("project")),
        f.get("name"), strip_html(f.get("description") or ""),
        field_text(f.get("status")), picks.get(str(sid)) if sid is not None else None,
        field_text(f.get("priority")), picks.get(str(pid)) if pid is not None else None,
        loc.get("sequence"), scalar(loc.get("globalSortOrder")),
        scalar(par.get("item")), scalar(par.get("project")),
        it.get("createdDate"), it.get("modifiedDate"), it.get("lastActivityDate"),
        scalar(it.get("createdBy")), scalar(it.get("modifiedBy")),
    )


def _rows_from_items(items, concurrency):
    """Turn raw API items into (item_rows, kv_rows, pick_rows). Shared by full build + incremental sync."""
    tkey, tname = type_maps()
    picks, pick_rows = _resolve_picklists(items, concurrency)
    item_rows, kv_rows = [], []
    for it in items:
        item_rows.append(_item_row(it, tkey, tname, picks))
        item_id = scalar(it.get("id"))
        for fk, fv in (it.get("fields") or {}).items():
            if fk != "description":  # raw HTML dropped; plain text lives in items.description
                kv_rows.append((item_id, fk, field_text(fv)))
    return item_rows, kv_rows, pick_rows


def build_db(proj_id, name, with_links=False, link_cap=0, concurrency=CONCURRENCY_DEFAULT):
    timings, t = {}, time.time()
    items = get_pages(f"/rest/v1/abstractitems?project={proj_id}", concurrency=concurrency,
                      progress_label=f"download {name}")
    timings["fetch_ms"] = int((time.time() - t) * 1000)

    t = time.time()
    edges = _fetch_links(items, link_cap, concurrency) if with_links else []
    timings["link_ms"] = int((time.time() - t) * 1000)

    item_rows, kv_rows, pick_rows = _rows_from_items(items, concurrency)

    t = time.time()
    p = db_path(proj_id)
    # Build into a sibling temp file, then atomically swap it in. A crash / Ctrl-C mid-build therefore
    # leaves the PREVIOUS good cache intact (the old code unlinked it up-front, so an interrupted
    # rebuild destroyed the cache and left a corrupt half-written file).
    tmp = p.with_name(f"{p.stem}.tmp-{os.getpid()}.db")
    tmp.unlink(missing_ok=True)
    con = sqlite3.connect(str(tmp))
    try:
        con.executescript(DDL)
        placeholders = ",".join("?" * len(ITEM_COLUMNS))
        con.executemany(f"INSERT INTO items({','.join(ITEM_COLUMNS)}) VALUES ({placeholders})", item_rows)
        con.executemany("INSERT INTO fields_kv(itemId,key,value) VALUES (?,?,?)", kv_rows)
        # populate the external-content FTS index from items
        con.execute("INSERT INTO fts(rowid, name, description) SELECT id, name, description FROM items")
        if pick_rows:
            con.executemany("INSERT OR IGNORE INTO picklist(id,name,pickList) VALUES (?,?,?)", pick_rows)
        if edges:
            rt = reltype_map()
            con.executemany(
                "INSERT OR IGNORE INTO relationships(id,fromItem,toItem,relationshipType,relTypeName,suspect) "
                "VALUES (?,?,?,?,?,?)",
                [(scalar(e.get("id")), scalar(e.get("fromItem")), scalar(e.get("toItem")),
                  scalar(e.get("relationshipType")), rt.get(str(e.get("relationshipType"))),
                  1 if e.get("suspect") else 0) for e in edges])
        watermark = max((r[_MODIFIED_IDX] for r in item_rows if r[_MODIFIED_IDX]), default="")
        now = time.time()
        meta = {
            "project_id": str(proj_id), "project_name": name,
            "fetched_at": repr(now), "last_sync_at": repr(now), "watermark": watermark,
            "item_count": str(len(item_rows)), "field_kv_count": str(len(kv_rows)),
            "relationship_count": str(len(edges)), "picklist_count": str(len(pick_rows)),
            "with_links": str(bool(with_links)),
            "engine_version": ENGINE_VERSION, "schema_version": SCHEMA_VERSION, "base_url": _cfg["BASE"],
        }
        con.executemany("INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)", list(meta.items()))
        con.commit()
    except BaseException:
        con.close()
        tmp.unlink(missing_ok=True)  # don't leave a half-written temp behind on error/Ctrl-C
        raise
    con.close()
    try:
        os.replace(str(tmp), str(p))  # atomic swap onto the live cache path
    except OSError:
        tmp.unlink(missing_ok=True)   # swap failed (e.g. dest locked on Windows) -> don't leak the temp
        raise
    timings["write_ms"] = int((time.time() - t) * 1000)

    return {"proj_id": proj_id, "name": name, "items": len(item_rows), "fields": len(kv_rows),
            "links": len(edges), "picklist": len(pick_rows),
            "size_mb": round(p.stat().st_size / 1048576, 2), **timings}


def incremental_sync(proj_id, name, concurrency=CONCURRENCY_DEFAULT):
    """Pull only items modified at/after the cache watermark (= MAX(modifiedDate)), upsert them, and
    rebuild the FTS index. Catches new + changed items in one modifiedDate sweep (modifiedDate >=
    createdDate holds for every item). Does NOT detect server-side DELETIONS — `rebuild` for that.
    Works on a COPY of the cache, then atomically swaps it in, so a failure leaves the cache untouched."""
    timings, t = {}, time.time()
    meta0 = read_meta(proj_id) or {}
    watermark = meta0.get("watermark") or ""
    if not watermark:  # no usable watermark (legacy/empty) -> caller should full-build instead
        return None

    # modifiedDate filter is >= (inclusive) and the value's '+0000' MUST be url-encoded (a literal '+'
    # means space in a query string -> 400). Inclusive re-pulls the 1-2 items sharing the exact watermark
    # every time; that's a cheap idempotent re-upsert and is deliberate — using '>' instead would miss a
    # new item written in the same millisecond as the current high-water mark.
    enc = urllib.parse.quote(watermark, safe="")
    items = get_pages(f"/rest/v1/abstractitems?project={proj_id}&modifiedDate={enc}", concurrency=concurrency)
    timings["fetch_ms"] = int((time.time() - t) * 1000)
    timings["pulled"] = len(items)

    p = db_path(proj_id)
    now = time.time()
    if not items:  # nothing changed -> just stamp last_sync_at on the live cache (cheap, safe)
        try:
            con = sqlite3.connect(str(p))
            con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('last_sync_at',?)", (repr(now),))
            con.commit()
            con.close()
        except sqlite3.Error:
            pass
        timings["upserted"] = 0
        return {"proj_id": proj_id, "name": name, "upserted": 0, "watermark": watermark,
                "size_mb": round(p.stat().st_size / 1048576, 2), **timings}

    item_rows, kv_rows, pick_rows = _rows_from_items(items, concurrency)
    ids = [r[0] for r in item_rows]

    t = time.time()
    tmp = p.with_name(f"{p.stem}.tmp-{os.getpid()}.db")
    tmp.unlink(missing_ok=True)
    shutil.copy2(p, tmp)  # ~17ms for 30MB; keeps the swap atomic and the original pristine on failure
    con = sqlite3.connect(str(tmp))
    try:
        placeholders = ",".join("?" * len(ITEM_COLUMNS))
        qmarks = ",".join("?" * len(ids))
        con.executemany(f"INSERT OR REPLACE INTO items({','.join(ITEM_COLUMNS)}) VALUES ({placeholders})",
                        item_rows)
        # fields_kv has no PK -> clear each changed item's rows, then re-insert
        con.execute(f"DELETE FROM fields_kv WHERE itemId IN ({qmarks})", ids)
        con.executemany("INSERT INTO fields_kv(itemId,key,value) VALUES (?,?,?)", kv_rows)
        if pick_rows:
            con.executemany("INSERT OR IGNORE INTO picklist(id,name,pickList) VALUES (?,?,?)", pick_rows)
        con.execute("INSERT INTO fts(fts) VALUES('rebuild')")  # full FTS rebuild from items (~58ms @10k)
        new_wm = max((r[_MODIFIED_IDX] for r in item_rows if r[_MODIFIED_IDX]), default=watermark)
        new_wm = max(new_wm, watermark)
        counts = dict(con.execute("SELECT 'i',COUNT(*) FROM items UNION ALL SELECT 'k',COUNT(*) FROM fields_kv").fetchall())
        upd = {"fetched_at": repr(now), "last_sync_at": repr(now), "watermark": new_wm,
               "item_count": str(counts.get("i", 0)), "field_kv_count": str(counts.get("k", 0)),
               "engine_version": ENGINE_VERSION, "schema_version": SCHEMA_VERSION}
        con.executemany("INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)", list(upd.items()))
        con.commit()
    except BaseException:
        con.close()
        tmp.unlink(missing_ok=True)
        raise
    con.close()
    try:
        os.replace(str(tmp), str(p))
    except OSError:
        tmp.unlink(missing_ok=True)
        raise
    timings["write_ms"] = int((time.time() - t) * 1000)
    timings["upserted"] = len(item_rows)
    _NAME_IDX, _DESC_IDX = ITEM_COLUMNS.index("name"), ITEM_COLUMNS.index("description")
    changed = [(r[0], r[_NAME_IDX], r[_DESC_IDX]) for r in item_rows]  # for the vector index to re-embed
    return {"proj_id": proj_id, "name": name, "upserted": len(item_rows), "watermark": new_wm,
            "size_mb": round(p.stat().st_size / 1048576, 2), "changed_items": changed, **timings}


# ============================ semantic / vector search (OPTIONAL: fastembed + sqlite-vec) ============================
# Kept fully optional via lazy imports: the core engine (FTS/LIKE/SQL) imports nothing here, so the skill
# still runs with only the standard library. Semantic search needs `pip install fastembed sqlite-vec`
# (+ a one-time ~200MB model download). Vectors live in a SEPARATE jama-proj-<id>.vec.db so the main cache
# stays pure-SQLite (openable read-only without the extension).
EMBED_MODEL = "BAAI/bge-base-en-v1.5"
EMBED_DIM = 768
# bge-base attention is O(batch * 512^2); batch 64 can spike ~300MB/buffer and OOM. 16 is the safe,
# still-fast default (compute-bound at 80% threads, not batch-overhead-bound).
VEC_BATCH = 16
VEC_SCHEMA = "1"
VEC_MAX_DISTANCE = 0.30  # vector hits must have cosine distance <= this (i.e. cosine similarity >= 0.70)
LEG_CANDIDATES = 200     # FTS/LIKE per-leg candidate depth fed into RRF fusion. BM25's tail is near-zero
                         # noise (multi-word OR can match 40-70% of the corpus); 200 covers the real signal
                         # and the RRF tail weight 1/(60+200) is negligible. The FUSED output stays uncapped.


def embed_threads():
    """~80% of logical CPUs (per spec), at least 1. 16 cores -> 12."""
    return max(1, int((os.cpu_count() or 1) * 0.8))


def model_cache_dir():
    d = user_data_dir() / "models"   # persistent, next to the caches (NOT the harness temp dir)
    d.mkdir(parents=True, exist_ok=True)
    return d


def vec_db_path(proj_id):
    return user_data_dir() / f"jama-proj-{proj_id}.vec.db"


_vectors_ready = False


def _vectors_importable():
    import importlib.util
    return all(importlib.util.find_spec(m) for m in ("fastembed", "sqlite_vec"))


def ensure_vectors():
    """Vectors are REQUIRED — no graceful degradation. If fastembed + sqlite-vec are absent, auto
    `pip install` them once and retry. Exits only if the install itself fails."""
    global _vectors_ready
    if _vectors_ready:
        return
    if _vectors_importable():
        _vectors_ready = True
        return
    print("[deps] vector libraries missing -> installing fastembed + sqlite-vec (one-time)...", file=sys.stderr)
    import importlib
    import subprocess
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--disable-pip-version-check",
                               "--quiet", "fastembed", "sqlite-vec"])
    except (subprocess.CalledProcessError, OSError) as e:
        sys.exit(f"Could not auto-install vector deps ({e}). Run manually: "
                 f"{sys.executable} -m pip install fastembed sqlite-vec")
    importlib.invalidate_caches()
    if not _vectors_importable():
        sys.exit("Vector deps still unavailable after install.")
    _vectors_ready = True


_embedder = None


def get_embedder():
    """Lazy singleton TextEmbedding pinned to a persistent cache dir and 80%-CPU threads."""
    global _embedder
    if _embedder is None:
        os.environ.setdefault("OMP_NUM_THREADS", str(embed_threads()))  # belt-and-suspenders w/ threads=
        from fastembed import TextEmbedding
        _embedder = TextEmbedding(model_name=EMBED_MODEL, threads=embed_threads(),
                                  cache_dir=str(model_cache_dir()))
    return _embedder


def _embed_text(name, description):
    n, d = (name or "").strip(), (description or "").strip()
    t = (n + ". " + d).strip(". ").strip() if d else n
    return t[:4000]  # the model truncates at 512 tokens (~2k chars) anyway -> lossless, caps tokenizer work


def _vec_connect(path, create=False):
    import sqlite_vec
    con = sqlite3.connect(str(path))
    con.enable_load_extension(True)
    sqlite_vec.load(con)
    con.enable_load_extension(False)
    if create:
        con.execute("PRAGMA journal_mode=OFF")
        con.execute("PRAGMA synchronous=OFF")
        # distance_metric=cosine -> KNN `distance` is (1 - cosine_similarity); bge vectors are
        # normalized so this ranks identically to L2 but gives an interpretable score = 1 - distance.
        con.execute(f"CREATE VIRTUAL TABLE IF NOT EXISTS vec USING vec0("
                    f"item_id INTEGER PRIMARY KEY, embedding float[{EMBED_DIM}] distance_metric=cosine)")
        con.execute("CREATE TABLE IF NOT EXISTS vmeta(key TEXT PRIMARY KEY, value TEXT)")
    return con


def _progress(label, total):
    """A throttled stderr progress bar (stdout stays clean for JSON). Returns cb(done)."""
    state = {"pct": -1, "t0": time.time()}

    def cb(done):
        pct = int(done * 100 / total) if total else 100
        if pct != state["pct"] or done >= total:
            state["pct"] = pct
            el = time.time() - state["t0"]
            rate = done / el if el > 0 else 0
            eta = (total - done) / rate if rate > 0 else 0
            bar = "#" * (pct // 5) + "-" * (20 - pct // 5)
            sys.stderr.write(f"\r  {label} [{bar}] {pct:3d}%  {done}/{total}  {rate:.0f}/s  ETA {eta:4.0f}s  ")
            sys.stderr.flush()
            if done >= total:
                sys.stderr.write("\n")
    return cb


def embed_corpus(texts, label="embedding", quiet=False):
    """Embed texts with a progress bar; returns list of float32 vectors (order preserved)."""
    emb = get_embedder()
    cb = (lambda d: None) if quiet else _progress(label, len(texts))
    out = []
    for i, v in enumerate(emb.embed(texts, batch_size=VEC_BATCH), 1):
        out.append(v.astype("float32"))
        if not quiet and (i % 64 == 0 or i == len(texts)):
            cb(i)
    return out


def vec_index_state(proj_id):
    """('absent'|'stale'|'ready', meta). 'stale' = built with a different model/dim than current."""
    p = vec_db_path(proj_id)
    if not p.exists():
        return "absent", {}
    try:
        con = sqlite3.connect(_ro_uri(p), uri=True)
        try:
            m = dict(con.execute("SELECT key,value FROM vmeta").fetchall())
        finally:
            con.close()
    except sqlite3.Error:
        return "absent", {}
    if m.get("embed_model") != EMBED_MODEL or m.get("dim") != str(EMBED_DIM):
        return "stale", m
    return "ready", m


def build_vectors(proj_id, quiet=False):
    """Full (re)build of the project's vector index from the main cache. Atomic swap; progress bar."""
    ensure_vectors()  # required: auto-installs fastembed/sqlite-vec if missing
    main = open_db(proj_id)
    try:
        rows = main.execute("SELECT id, name, description FROM items ORDER BY id").fetchall()
        wmrow = main.execute("SELECT value FROM meta WHERE key='watermark'").fetchone()
    finally:
        main.close()
    # Sort items by text length so each fixed-size batch is length-homogeneous: fastembed pads every
    # batch to its longest member, so mixing a 512-token monster with short docs wastes ~3x compute.
    # Sorting cut the real ProjectA build from ~105min to ~35min. We keep ids aligned with their text.
    pairs = sorted(((r["id"], _embed_text(r["name"], r["description"])) for r in rows), key=lambda p: len(p[1]))
    ids = [p[0] for p in pairs]
    texts = [p[1] for p in pairs]
    if not quiet:
        print(f"[vectors] embedding {len(ids)} items — {EMBED_MODEL}, {embed_threads()} threads "
              f"(one-time, ~35 min for 10k on CPU; later syncs only re-embed changed items)")
    t = time.time()
    vecs = embed_corpus(texts, label="embed", quiet=quiet)
    p = vec_db_path(proj_id)
    tmp = p.with_name(f"{p.stem}.tmp-{os.getpid()}.vec.db")
    tmp.unlink(missing_ok=True)
    con = _vec_connect(tmp, create=True)
    try:
        con.executemany("INSERT INTO vec(item_id, embedding) VALUES (?, ?)",
                        [(i, v.tobytes()) for i, v in zip(ids, vecs)])
        meta = {"embed_model": EMBED_MODEL, "dim": str(EMBED_DIM), "vec_count": str(len(ids)),
                "main_watermark": (wmrow[0] if wmrow else ""), "built_at": repr(time.time()),
                "vec_schema": VEC_SCHEMA, "engine_version": ENGINE_VERSION}
        con.executemany("INSERT OR REPLACE INTO vmeta(key,value) VALUES (?,?)", list(meta.items()))
        con.commit()
    except BaseException:
        con.close()
        tmp.unlink(missing_ok=True)
        raise
    con.close()
    try:
        os.replace(str(tmp), str(p))
    except OSError:
        tmp.unlink(missing_ok=True)
        raise
    if not quiet:
        print(f"[vectors] built {len(ids)} vectors in {time.time()-t:.0f}s "
              f"-> {p.name} ({p.stat().st_size/1048576:.1f} MB)")
    return {"vec_count": len(ids), "build_s": round(time.time() - t, 1)}


def sync_vectors(proj_id, changed_items, quiet=True):
    """Upsert embeddings for changed items into an EXISTING vec index (copy+swap). No-op if the index is
    absent (built by sync/rebuild/semantic) or stale (model changed -> a full rebuild handles it)."""
    if not changed_items:
        return None
    state, _ = vec_index_state(proj_id)
    if state != "ready":
        return None  # absent/stale -> the caller's build_vectors() (re)builds the whole index instead
    ensure_vectors()
    ids = [it[0] for it in changed_items]
    texts = [_embed_text(it[1], it[2]) for it in changed_items]
    vecs = embed_corpus(texts, quiet=True)
    p = vec_db_path(proj_id)
    tmp = p.with_name(f"{p.stem}.tmp-{os.getpid()}.vec.db")
    tmp.unlink(missing_ok=True)
    shutil.copy2(p, tmp)
    con = _vec_connect(tmp)
    try:
        con.execute(f"DELETE FROM vec WHERE item_id IN ({','.join('?' * len(ids))})", ids)
        con.executemany("INSERT INTO vec(item_id, embedding) VALUES (?, ?)",
                        [(i, v.tobytes()) for i, v in zip(ids, vecs)])
        con.execute("INSERT OR REPLACE INTO vmeta(key,value) VALUES('built_at',?)", (repr(time.time()),))
        con.commit()
    except BaseException:
        con.close()
        tmp.unlink(missing_ok=True)
        raise
    con.close()
    try:
        os.replace(str(tmp), str(p))
    except OSError:
        tmp.unlink(missing_ok=True)
        raise
    return {"updated": len(ids)}


def _ensure_vector_index(proj_id):
    """Make sure the vector index exists and matches the current model (build it if absent/stale)."""
    ensure_vectors()
    state, _ = vec_index_state(proj_id)
    if state != "ready":
        reason = "no vector index yet" if state == "absent" else "index built with a different model"
        print(f"[vectors] {reason} -> building now...", file=sys.stderr)
        if build_vectors(proj_id) is None:
            sys.exit("Could not build the vector index.")


def _semantic_ids(proj_id, query, max_distance=VEC_MAX_DISTANCE):
    """ALL (item_id, cosine_score) within the distance threshold, nearest first — no count cap. vec0 needs
    a LIMIT, so we ask for the whole index then keep only cosine distance <= max_distance (sim >= 1-thr)."""
    _ensure_vector_index(proj_id)
    _, vmeta = vec_index_state(proj_id)
    # vec0 KNN requires a LIMIT and caps it at 4096. That's far more than the count of items within a
    # sensible similarity threshold (>=0.70), so "all matches above the threshold" is fully covered.
    k = min(int(vmeta.get("vec_count") or 4096), 4096)
    qv = list(get_embedder().query_embed([query]))[0].astype("float32")  # bge query-side prefix applied
    con = _vec_connect(vec_db_path(proj_id))
    try:
        knn = con.execute("SELECT item_id, distance FROM vec WHERE embedding MATCH ? ORDER BY distance LIMIT ?",
                          (qv.tobytes(), k)).fetchall()
    finally:
        con.close()
    return [(r[0], round(1 - r[1], 3)) for r in knn if r[1] <= max_distance]  # distance = 1 - cosine sim


def _fts_ids(con, keywords, match, field):
    """Top-LEG_CANDIDATES FTS (keyword/BM25) matches — the relevant head; the long BM25 tail is noise."""
    if not keywords:
        return []
    m = fts_query(keywords, match)
    if field == "name":
        m = "name : (" + m + ")"
    try:
        rows = con.execute("SELECT i.id FROM fts JOIN items i ON i.id = fts.rowid WHERE fts MATCH ? "
                           "ORDER BY bm25(fts) LIMIT ?", (m, LEG_CANDIDATES)).fetchall()
        return [r[0] for r in rows]
    except sqlite3.OperationalError:
        return []  # odd chars -> fts5 syntax error: drop this leg, the others still run


def _like_ids(con, keywords, match, field):
    """Up-to-LEG_CANDIDATES substring (LIKE) matches. LIKE has no relevance order, so take them in document
    order (deterministic/reproducible). Need EVERY item containing a term? That's a SQL `query`, not search."""
    if not keywords:
        return []
    col = "(name || ' ' || COALESCE(description,''))" if field == "all" else "name"
    joiner = " AND " if match == "all" else " OR "
    where = "(" + joiner.join(f"{col} LIKE {sql_lit('%' + kw + '%')}" for kw in keywords) + ")"
    rows = con.execute(f"SELECT id FROM items WHERE {where} "
                       f"ORDER BY CASE WHEN globalSortOrder IS NULL THEN 1 ELSE 0 END, globalSortOrder "
                       f"LIMIT ?", (LEG_CANDIDATES,)).fetchall()
    return [r[0] for r in rows]


def _join_items(proj_id, ids, type_arg):
    """Map id -> item row for the given ids, applying an optional --type filter."""
    if not ids:
        return {}
    main = open_db(proj_id)
    try:
        where = f"id IN ({','.join('?' * len(ids))})"
        tclause = type_clause(type_arg)
        if tclause:
            where += " AND " + tclause
        rows = main.execute(f"SELECT id, documentKey, typeKey, sequence, name FROM items WHERE {where}",
                            ids).fetchall()
    finally:
        main.close()
    return {r["id"]: r for r in rows}


def semantic_search(proj_id, query, top=0, type_arg=None, max_distance=VEC_MAX_DISTANCE):
    """Pure vector search: every item within the cosine-distance threshold, nearest first. top=0 = no cap."""
    hits = _semantic_ids(proj_id, query, max_distance)  # distance <= max_distance
    by_id, cos, out = _join_items(proj_id, [i for i, _ in hits], type_arg), dict(hits), []
    for iid, _ in hits:
        if iid in by_id:
            if top and len(out) >= top:
                break
            r = by_id[iid]
            out.append({"id": iid, "documentKey": r["documentKey"], "typeKey": r["typeKey"],
                        "sequence": r["sequence"], "score": cos[iid], "name": r["name"]})
    return out


def hybrid_search(proj_id, query_text, keywords, match="any", field="all", top=0, type_arg=None,
                  max_distance=VEC_MAX_DISTANCE):
    """DEFAULT content search: UNION of FTS (keyword/BM25) + LIKE (substring) + semantic (vector, thresholded
    at cosine distance <= max_distance), fused by Reciprocal Rank Fusion and de-duped by item id. Each leg
    returns ALL its matches (no cap); top=0 returns the whole fused union. `via` = which legs matched."""
    con = open_db(proj_id)
    try:
        legs = {"fts": _fts_ids(con, keywords, match, field),
                "like": _like_ids(con, keywords, match, field)}
    finally:
        con.close()
    legs["vec"] = [i for i, _ in _semantic_ids(proj_id, query_text, max_distance)]
    fused, srcs = {}, {}
    for leg, ids in legs.items():
        for rank, iid in enumerate(ids):
            fused[iid] = fused.get(iid, 0.0) + 1.0 / (60 + rank + 1)  # RRF, k=60
            srcs.setdefault(iid, set()).add(leg)
    if not fused:
        return []
    by_id = _join_items(proj_id, list(fused.keys()), type_arg)
    ranked = sorted((i for i in fused if i in by_id), key=lambda i: -fused[i])
    if top:
        ranked = ranked[:top]
    out = []
    for iid in ranked:
        r = by_id[iid]
        out.append({"id": iid, "documentKey": r["documentKey"], "typeKey": r["typeKey"],
                    "sequence": r["sequence"], "score": round(fused[iid], 4),
                    "via": "+".join(sorted(srcs[iid])), "name": r["name"]})
    return out


# ============================ freshness (persistent cache: present / missing / corrupt) ============================
def cache_state(proj_id):
    """No TTL/expiry: a cache is 'present' (usable, will be incrementally synced), 'missing', or
    'corrupt' (unreadable, or older schema -> rebuilt). 'last_sync_at'/'watermark' surface freshness."""
    p = db_path(proj_id)
    if not p.exists():
        return {"state": "missing", "db": p}
    meta = read_meta(proj_id)  # read-only; None = unreadable/corrupt
    if meta is None:
        return {"state": "corrupt", "db": p}
    if meta.get("schema_version") != SCHEMA_VERSION or "watermark" not in meta:
        return {"state": "corrupt", "db": p}  # pre-v4 cache -> rebuild once to gain the watermark
    return {"state": "present", "db": p, "meta": meta}


def ensure_synced(proj_id, name, force=False, offline=False, with_links=False, link_cap=0,
                  quiet=False, concurrency=CONCURRENCY_DEFAULT, want_vectors=False):
    """ALWAYS checks the cache before a query (spec point 3): missing/corrupt -> full download (with a
    progress bar); present -> incremental sync of changed items; force (rebuild) -> full re-download.
    The cache is never auto-deleted for age. `offline` = escape hatch: use the existing cache as-is, skip
    the online sync (errors if there's no cache yet — that first build needs the network).

    Vectors: a full (re)build also (re)builds the vector index when want_vectors (search/semantic/sync/
    rebuild); an incremental sync re-embeds just the changed items IF an index already exists."""
    info = cache_state(proj_id)
    if offline:
        if info["state"] == "present":
            if not quiet:
                print(f"[{proj_id} {name}] --offline -> using existing cache as-is (no sync)")
            return None
        sys.exit(f"--offline: no usable cache for project {proj_id} ({info['state']}). "
                 f"Run it once online to build the cache first.")
    if force or info["state"] in ("missing", "corrupt"):
        if not quiet:
            why = "rebuild" if force else info["state"]
            print(f"[{proj_id} {name}] {why} -> full download...")
        res = build_db(proj_id, name, with_links, link_cap, concurrency)
        if want_vectors:
            build_vectors(proj_id, quiet=quiet)
        return res
    res = incremental_sync(proj_id, name, concurrency)
    if res is None:  # present but no usable watermark -> heal via full build
        if not quiet:
            print(f"[{proj_id} {name}] no watermark -> full download...")
        res = build_db(proj_id, name, with_links, link_cap, concurrency)
        if want_vectors:
            build_vectors(proj_id, quiet=quiet)
        return res
    if want_vectors:  # only semantic/sync/rebuild touch vectors -> a plain keyword search never imports
        sync_vectors(proj_id, res.get("changed_items"))       # fastembed (stays ms-fast). semantic auto-
        if vec_index_state(proj_id)[0] != "ready":            # refreshes the index before every query, so
            build_vectors(proj_id, quiet=quiet)               # it's never stale when it matters.
    if not quiet:
        if res.get("upserted"):
            print(f"[{proj_id} {name}] synced: {res['upserted']} item(s) updated")
        else:
            print(f"[{proj_id} {name}] up to date (no changes)")
    return res


# ============================ SQL helpers ============================
def open_db(proj_id):
    # Read-only: search/query never mutate the cache, and mode=ro makes a stray write in user SQL fail
    # loudly instead of silently corrupting the snapshot.
    con = sqlite3.connect(_ro_uri(db_path(proj_id)), uri=True)
    con.row_factory = sqlite3.Row
    return con


def sql_lit(s):
    return "'" + str(s).replace("'", "''") + "'"


def type_clause(type_arg):
    """Build a 'itemType=.. OR typeKey=..' clause from a --type value, or '' if none."""
    if not type_arg:
        return ""
    parts = [f"itemType = {t}" if t.isdigit() else f"typeKey = {sql_lit(t)} COLLATE NOCASE"
             for t in (x.strip() for x in type_arg.split(",")) if t]
    return "(" + " OR ".join(parts) + ")" if parts else ""


def keywords_of(values):
    return [k for v in (values or []) for k in (x.strip() for x in v.split(",")) if k]


def fts_query(keywords, match):
    """Compose an FTS5 MATCH string from keywords. Phrases are quoted; joined by OR/AND."""
    terms = ['"' + k.replace('"', '""') + '"' for k in keywords]
    return (" AND " if match == "all" else " OR ").join(terms)


# ============================ result rendering ============================
def emit_rows(rows, as_json, header):
    if as_json:
        print(json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2))
        return
    print(header)
    if not rows:
        return
    cols = rows[0].keys()
    widths = {"id": 9, "documentKey": 12, "typeKey": 7, "sequence": 16, "score": 8, "via": 14}
    print("  ".join(c.ljust(widths.get(c, 0)) for c in cols))
    for r in rows:
        cells = []
        for c in cols:
            val = "" if r[c] is None else str(r[c])
            if c == "name" and len(val) > 60:
                val = val[:60]
            cells.append(val.ljust(widths.get(c, 0)))
        print("  ".join(cells))


# ============================ commands ============================
def cmd_projects(a):
    rows = all_projects()
    if a.project:
        rx = "|".join(a.project)
        rows = [p for p in rows if re.search(rx, p["fields"]["name"], re.I)]
    elif not a.all:
        print(f"Tip: pass --project <regex> to filter, or --all for every project ({len(all_projects())}).")
    for p in sorted(rows, key=lambda x: x["fields"]["name"]):
        print(f"{p['id']:<8} {p.get('projectKey', ''):<12} {p['fields']['name']}")


def cmd_sync(a, force=False):
    projs = resolve_projects(a.project)
    force = force or a.force
    mode = "REBUILD (full re-download)" if force else "sync (incremental)"
    print(f"[{mode}] resolved {len(projs)} project(s): " +
          ", ".join(f"{p['id']}={p['name']}" for p in projs))
    want_vectors = not a.no_vectors  # sync/rebuild build+maintain the vector index by default
    summary = []
    for pr in projs:
        t = time.time()
        s = ensure_synced(pr["id"], pr["name"], force=force,
                          with_links=a.with_links, link_cap=a.link_cap, concurrency=a.concurrency,
                          want_vectors=want_vectors)
        total = int((time.time() - t) * 1000)
        s = {**(s or {"proj_id": pr["id"], "name": pr["name"], "items": "(no change)"}), "total_ms": total}
        s.pop("changed_items", None)  # internal (list of changed item tuples) — don't print/serialize
        summary.append(s)
    if a.json:
        print(json.dumps(summary, indent=2))
        return
    print("\n== summary ==")
    for s in summary:
        print("  " + "  ".join(f"{k}={v}" for k, v in s.items() if k not in ("proj_id",)))


def cmd_status(a):
    if a.project:
        targets = resolve_projects(a.project)
    else:
        targets = [{"id": pid, "name": ""} for pid in cached_project_ids()]
    if not targets:
        print(f"No caches found in {user_data_dir()}.")
        return
    for t in targets:
        info = cache_state(t["id"])
        line = {"project": t["id"], "state": info["state"]}
        if info["state"] == "present":
            m = info["meta"]
            def _fmt(epoch_repr):
                try:
                    return time.strftime("%Y-%m-%d %H:%M", time.localtime(float(epoch_repr)))
                except (TypeError, ValueError):
                    return "?"
            vstate, vmeta = vec_index_state(t["id"])
            vinfo = vstate
            if vstate != "absent":
                vp = vec_db_path(t["id"])
                vinfo = f"{vstate}({vmeta.get('vec_count','?')} vecs, {vp.stat().st_size/1048576:.1f}MB)"
            line.update(name=m.get("project_name"), items=m.get("item_count"),
                        links=m.get("relationship_count"),
                        last_sync=_fmt(m.get("last_sync_at")),
                        watermark=(m.get("watermark") or "")[:19],  # newest modifiedDate captured
                        size_mb=round(info["db"].stat().st_size / 1048576, 2),
                        vectors=vinfo)
        print("  " + "  ".join(f"{k}={v}" for k, v in line.items()))
    print(f"cache dir: {user_data_dir()}")


def cmd_search(a):
    # DEFAULT = hybrid (LIKE + keyword/FTS + semantic, RRF-fused, de-duped) per spec point 4.
    keywords = keywords_of(a.keyword)
    query_text = " ".join(a.query) if a.query else " ".join(keywords)
    if a.query and not keywords:
        keywords = keywords_of(a.query)  # feed the FTS/LIKE legs from the query words too
    if not query_text:
        sys.exit('search needs --keyword a,b or --query "natural language text".')
    md = a.max_distance if a.max_distance is not None else VEC_MAX_DISTANCE
    pr = single_project(a.project)
    ensure_synced(pr["id"], pr["name"], force=a.force, offline=a.offline, quiet=True,
                  concurrency=a.concurrency, want_vectors=True)
    t = time.time()
    rows = hybrid_search(pr["id"], query_text, keywords, match=a.match, field=a.field,
                         top=a.top, type_arg=a.type, max_distance=md)
    ms = int((time.time() - t) * 1000)
    emit_rows(rows, a.json, f"{len(rows)} hybrid match(es) [FTS+LIKE+vector; vec sim>={1-md:.2f}] in {ms} ms   "
                            f"[offline cache: {pr['id']} {pr['name']}]")


def cmd_query(a):
    if not a.sql:
        sys.exit("--sql required, e.g. --sql 'SELECT typeKey, COUNT(*) c FROM items GROUP BY typeKey'")
    if not re.match(r"(?is)^\s*(SELECT|WITH)\b", a.sql):
        sys.exit("Only read-only SELECT/WITH queries are allowed.")
    pr = single_project(a.project)
    ensure_synced(pr["id"], pr["name"], force=a.force, offline=a.offline, quiet=True, concurrency=a.concurrency)
    con = open_db(pr["id"])
    try:
        t = time.time()
        rows = con.execute(a.sql).fetchall()
        ms = int((time.time() - t) * 1000)
    finally:
        con.close()
    emit_rows(rows, a.json, f"{len(rows)} row(s) in {ms} ms   [offline cache: {pr['id']} {pr['name']}]")


def cmd_semantic(a):
    q = " ".join(a.query) if a.query else None
    if not q:
        sys.exit('semantic needs --query "natural language text", e.g. --query "headset won\'t charge on the cradle"')
    md = a.max_distance if a.max_distance is not None else VEC_MAX_DISTANCE
    pr = single_project(a.project)
    # ensure main cache fresh AND the vector index exists/updated (want_vectors=True)
    ensure_synced(pr["id"], pr["name"], force=a.force, offline=a.offline, quiet=True,
                  concurrency=a.concurrency, want_vectors=True)
    t = time.time()
    rows = semantic_search(pr["id"], q, top=a.top, type_arg=a.type, max_distance=md)
    ms = int((time.time() - t) * 1000)
    emit_rows(rows, a.json, f"{len(rows)} semantic match(es) [cosine sim >= {1-md:.2f}] in {ms} ms via "
                            f"{EMBED_MODEL}   [offline cache: {pr['id']} {pr['name']}]")


def _purge_one(proj_id):
    """Delete a project's main cache AND its vector index. Returns count of files removed."""
    n = 0
    for p in (db_path(proj_id), vec_db_path(proj_id)):
        if p.exists():
            p.unlink(missing_ok=True)
            n += 1
    return n


def cmd_purge(a):
    if a.all:
        n = sum(_purge_one(pid) for pid in cached_project_ids())
        print(f"Deleted {n} cache file(s) from {user_data_dir()}.")
        return
    if not a.project:
        sys.exit("purge needs --project <id|name>[,..] or --all.")
    for pr in resolve_projects(a.project):
        n = _purge_one(pr["id"])
        print(f"Deleted cache for {pr['id']} {pr['name']}." if n else f"No cache for {pr['id']} {pr['name']}.")


def cmd_login(a):
    base = a.base or os.environ.get("JAMA_BASE")
    cid = a.client_id or os.environ.get("JAMA_CLIENT_ID")
    csec = a.client_secret or os.environ.get("JAMA_CLIENT_SECRET")
    if not (base and cid and csec):
        sys.exit("login needs --base <url> --client-id <id> --client-secret <secret> (or the matching env vars).")
    _cfg.clear()  # validate the supplied creds by actually fetching a token before we persist them
    _cfg.update(BASE=base.rstrip("/"), CID=cid, CSEC=csec)
    _token_file().unlink(missing_ok=True)
    try:
        get_token()
    except Exception as e:
        sys.exit(f"Login FAILED — could not get a token with those credentials: {str(e)[:200]}")
    f = save_credentials(base, cid, csec)
    print(f"Login OK — credentials saved to {f}\nThey'll be reused automatically; no need to log in again "
          f"(run `logout` to remove).")


def cmd_logout(a):
    f = creds_file()
    if f.exists():
        f.unlink()
        print(f"Logged out — removed {f}.")
    else:
        print(f"No saved credentials at {f}.")


# ============================ CLI ============================
COMMANDS = {
    "login": cmd_login,
    "logout": cmd_logout,
    "projects": cmd_projects,
    "sync": cmd_sync,
    "rebuild": lambda a: cmd_sync(a, force=True),
    "refresh": lambda a: cmd_sync(a, force=True),
    "update": lambda a: cmd_sync(a, force=True),
    "status": cmd_status,
    "search": cmd_search,
    "semantic": cmd_semantic,
    "query": cmd_query,
    "purge": cmd_purge,
}


def build_parser():
    p = argparse.ArgumentParser(prog="jama_offline.py", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in COMMANDS:
        sp = sub.add_parser(name)
        sp.add_argument("--project", action="append", help="id or name (comma-separated or repeated)")
        sp.add_argument("--concurrency", type=int, default=CONCURRENCY_DEFAULT)
        sp.add_argument("--json", action="store_true")
        sp.add_argument("--force", action="store_true")
        sp.add_argument("--offline", action="store_true",
                        help="search/semantic/query: use the existing cache as-is, skip the online sync")
        sp.add_argument("--no-vectors", action="store_true", dest="no_vectors",
                        help="sync/rebuild: skip building/updating the semantic vector index")
        sp.add_argument("--with-links", action="store_true", dest="with_links")
        sp.add_argument("--link-cap", type=int, default=0, dest="link_cap")
        sp.add_argument("--all", action="store_true")
        if name == "search":
            sp.add_argument("--keyword", action="append", help="keyword(s), comma-separated")
            sp.add_argument("--query", action="append", help="natural-language text (drives the vector leg)")
            sp.add_argument("--match", choices=["any", "all"], default="any")
            sp.add_argument("--field", choices=["name", "all"], default="all")
            sp.add_argument("--type", default=None)
            sp.add_argument("--top", type=int, default=0, help="cap rows (0 = no cap, return all matches)")
            sp.add_argument("--max-distance", type=float, default=None, dest="max_distance",
                            help=f"vector cosine-distance ceiling (default {VEC_MAX_DISTANCE}; lower = stricter)")
        if name == "semantic":
            sp.add_argument("--query", action="append", help="natural-language query text")
            sp.add_argument("--type", default=None)
            sp.add_argument("--top", type=int, default=0, help="cap rows (0 = no cap)")
            sp.add_argument("--max-distance", type=float, default=None, dest="max_distance",
                            help=f"vector cosine-distance ceiling (default {VEC_MAX_DISTANCE}; lower = stricter)")
        if name == "query":
            sp.add_argument("--sql", default=None)
        if name == "login":
            sp.add_argument("--base", default=None, help="Jama base URL, e.g. https://x.jamacloud.com")
            sp.add_argument("--client-id", default=None, dest="client_id")
            sp.add_argument("--client-secret", default=None, dest="client_secret")
    return p


def main(argv=None):
    a = build_parser().parse_args(argv)
    # Credentials are loaded lazily on the first network call (see ensure_credentials / api_get), so
    # offline-serviceable commands (fresh-cache search/query, status, purge) run with no creds at all.
    COMMANDS[a.cmd](a)


if __name__ == "__main__":
    main()
