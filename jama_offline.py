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
    init      --project <id|name>[,..]                 FIRST-TIME full build (data + vector index + model);
                                                       no-op-with-hint if a cache already exists
    update    --project <id|name>[,..]                 incrementally update an EXISTING cache + its vectors
              [--no-vectors] [--prune-deleted]          (streamed, low-memory; no size limit)
    rebuild   --project <id|name>[,..]                 force a clean FULL re-download (drops deletions too)
    sync      --project <id|name>[,..]                 build-if-missing else incremental (init+update in one)
              [--no-vectors] [--with-links] [--link-cap N] [--prune-deleted]
    status    [--project <id|name>[,..]]               caches: last-sync, watermark, counts, size, vectors
    search    --project <id> --keyword a,b | --query "..."   HYBRID: FTS + LIKE + vector, RRF-fused/de-duped
              [--type REQ,FEAT] [--top N] [--max-distance D] [--match any|all] [--field name|all] [--json]
    semantic  --project <id> --query "..."             pure vector (KNN) search; [--max-distance D] [--top N]
    query     --project <id> --sql "SELECT ..."        read-only SQL — USE THIS for counts/stats/aggregates
    purge     --project <id|name>[,..] | --all         delete cache file(s) (incl. the vector index)

  search / semantic / query are OFFLINE-FIRST and FAIL-FAST: they never silently trigger a long build. If
  the cache / vector index / embedding model is missing, or the pending server delta (or vector lag) exceeds
  DELTA_LIMIT (200) items, they STOP with an exact message telling you to run `init` or `update` first. A
  small delta IS auto-synced (bounded). `--offline` serves the existing cache as-is; `--force` overrides.

  PARALLEL USE: before fanning out parallel queries — or parallel agents that each call this script — on the
  SAME project, you MUST prepare it ONCE first: a single serial `init`/`update` (which also downloads the
  one-time ~210 MB embedding model, shared by ALL projects, and builds the vectors), then confirm it is
  caught up (`status`: state=present, vectors=ready), THEN run the parallel workers with `--offline` so each
  reads the fresh cache and none re-syncs or re-downloads. Otherwise every worker either hits the offline-first
  gate and STOPs together, or redundantly re-syncs the same delta — and WORST of all, parallel
  init/update/sync/rebuild with no model yet makes each process download the SAME ~210 MB model at once.
  Never run init/update/sync/rebuild on one project (or the first-ever build of any project) concurrently —
  serialize the build, parallelize only the --offline reads.

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
ENGINE_VERSION = "4.3.0-py"
# v4: caches are PERSISTENT (no TTL/expiry). Each use incrementally syncs items whose modifiedDate is
# >= the cache's watermark (= MAX(modifiedDate)), upserts them, and rebuilds the FTS index. Deletions
# on the server are NOT tracked (only adds/changes) — use `rebuild` for a clean full re-download.
# v4.1 (schema 5): a test case's authored steps (testCaseSteps -> action/expectedResult/notes) are
# extracted into items.stepsText, indexed by FTS + LIKE, and embedded into the vector index. The vector
# index now CHUNKS each item's full text (name + description + steps) with overlap instead of truncating,
# so long test cases are searchable end to end; chunk hits fold back to one row per item at query time.
# v4.2: (a) every NON-Jama download (pip deps + the embedding model) PREFERS a China mirror chosen by a
# live speed test (China-first, international fallback, abort if neither is fast enough) — Jama API traffic
# is untouched; (b) all long downloads/builds emit periodic progress to stderr (pip install, model
# download, full + incremental cache builds, vector (re)embedding); (c) `search --expr` accepts boolean
# keyword expressions — AND/OR/NOT + parentheses, e.g. "(upload or download) and not deprecated". No schema change.
# v4.3: (a) the vector index is built AND refreshed in bounded-memory STREAMING waves (read item ids, then
# fetch->chunk->embed->insert one wave at a time), and incremental_sync upserts its delta wave-by-wave too,
# so peak memory stays small no matter the project size; (b) search/semantic/query are OFFLINE-FIRST &
# FAIL-FAST: a missing cache / vector index / embedding model, or a pending server delta (or vector lag)
# over DELTA_LIMIT (200) items, STOPS the query with an actionable message instead of silently kicking off
# a long download/build; (c) new `init` (first build) and `update` (incremental) commands. No schema change.
SCHEMA_VERSION = "5"
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
    # Retry transient network/TLS hiccups (e.g. a sandbox/proxy SSL EOF mid-handshake) with backoff, like
    # api_get. A 4xx (e.g. 401 bad creds) is a REAL failure -> raise immediately so `login` reports it
    # fast; only 429/5xx and connection-level errors are retried.
    tok, last = None, None
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                tok = json.loads(r.read())["access_token"]
            break
        except urllib.error.HTTPError as e:
            if e.code != 429 and not (500 <= e.code < 600):
                raise
            last = e
        except (urllib.error.URLError, http.client.HTTPException, OSError) as e:
            last = e
        if attempt < 5:
            time.sleep(min(20, 1 + attempt * 2))
    if tok is None:
        raise RuntimeError(f"token fetch failed after retries: {last}")
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


def iter_pages(path, limit=10**9, concurrency=CONCURRENCY_DEFAULT, progress_label=None):
    """STREAMING pager: yield items WAVE by WAVE (each wave = up to `concurrency` pages fetched concurrently),
    so the caller never holds more than ~concurrency*PAGE items at once — peak memory is bounded regardless of
    project size. Page 1 is fetched first (to learn the total + warm the token), then the rest in waves. With
    progress_label, shows a download progress bar on stderr. Yields a (possibly empty) list of items per wave."""
    sep = "&" if "?" in path else "?"
    first = api_get(f"{path}{sep}startAt=0&maxResults={PAGE}")
    total = int(first["meta"]["pageInfo"]["totalResults"])
    want = min(total, limit)
    cb = _progress(progress_label, want) if (progress_label and want > PAGE) else None
    page1 = list(first.get("data") or [])
    first = None  # release page 1's raw response promptly; we've taken the total + data
    done = len(page1)
    if cb:
        cb(done)
    yield page1
    page1 = None  # consumer has it now -> drop the generator's reference so it can be freed
    if want > PAGE:
        starts = list(range(PAGE, want, PAGE))
        fetch = lambda s: api_get(f"{path}{sep}startAt={s}&maxResults={PAGE}").get("data") or []
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            for i in range(0, len(starts), concurrency):          # one wave = <= concurrency pages
                wave = []
                for d in ex.map(fetch, starts[i:i + concurrency]):  # bounded look-ahead -> bounded memory
                    wave.extend(d)
                done += len(wave)
                if cb:
                    cb(min(done, want))
                yield wave
                wave = None  # release this wave before fetching the next one


def get_pages(path, limit=10**9, concurrency=CONCURRENCY_DEFAULT, progress_label=None):
    """Accumulating wrapper over iter_pages: returns ALL items as one list. Use for small result sets
    (reference data, incremental deltas); the big project sweep streams via iter_pages to bound memory."""
    acc = []
    for wave in iter_pages(path, limit, concurrency, progress_label):
        acc.extend(wave)
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


def steps_text(fields):
    """Plain-text of a test case's AUTHORED steps: each testCaseSteps entry's action / expectedResult /
    notes, HTML-stripped and space-joined. Only `testCaseSteps` is used — `testRunSteps` (per-execution
    copies) are ignored on purpose so we don't embed near-duplicate run text. Returns '' when absent."""
    steps = (fields or {}).get("testCaseSteps")
    if not isinstance(steps, (list, tuple)):
        return ""
    parts = []
    for s in steps:
        if not isinstance(s, dict):
            continue
        for k in ("action", "expectedResult", "notes"):
            t = strip_html(s.get(k) or "")
            if t:
                parts.append(t)
    return " ".join(parts).strip()


# ============================ schema ============================
DDL = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE items(
  id INTEGER PRIMARY KEY, documentKey TEXT, globalId TEXT,
  itemType INTEGER, typeKey TEXT, typeName TEXT, project INTEGER,
  name TEXT, description TEXT, status TEXT, statusName TEXT, priority TEXT, priorityName TEXT,
  sequence TEXT, globalSortOrder INTEGER, parentItem INTEGER, parentProject INTEGER,
  createdDate TEXT, modifiedDate TEXT, lastActivityDate TEXT, createdBy INTEGER, modifiedBy INTEGER,
  stepsText TEXT
);
CREATE TABLE fields_kv(itemId INTEGER, key TEXT, value TEXT);
CREATE TABLE picklist(id INTEGER PRIMARY KEY, name TEXT, pickList INTEGER);
CREATE TABLE relationships(id INTEGER PRIMARY KEY, fromItem INTEGER, toItem INTEGER,
  relationshipType INTEGER, relTypeName TEXT, suspect INTEGER);
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
-- external-content FTS5: indexes items.name + items.description + items.stepsText (test-case steps) with
-- no duplicate storage; rowid = items.id
CREATE VIRTUAL TABLE fts USING fts5(name, description, stepsText, content='items', content_rowid='id',
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
                "createdDate", "modifiedDate", "lastActivityDate", "createdBy", "modifiedBy",
                "stepsText")
_MODIFIED_IDX = ITEM_COLUMNS.index("modifiedDate")  # position of modifiedDate in an item row tuple


# ============================ build a project cache ============================
_TMP_SEQ = 0


def _unique_tmp(p, ext):
    """A unique sibling temp path for an atomic build+swap. Includes the PID (+ a process-local counter), so
    CONCURRENT downloads of the same project — each a separate process triggered by parallel queries — get
    DISTINCT temp files and never clobber one another; the final atomic swap then picks the single winner."""
    global _TMP_SEQ
    _TMP_SEQ += 1
    return p.with_name(f"{p.stem}.tmp-{os.getpid()}-{_TMP_SEQ}.{ext}")


def _atomic_swap(tmp, dest, retries=8, base_delay=0.1, max_delay=2.0):
    """os.replace(tmp -> dest) with retry/backoff. On Windows a concurrent reader can briefly hold dest and
    make the rename fail (EACCES); retry a few times, then give up cleanly (unlink the temp, raise) rather
    than leak it. The rename itself is atomic, so readers always see either the old or the new cache."""
    for attempt in range(retries):
        try:
            os.replace(str(tmp), str(dest))
            return
        except OSError:
            if attempt == retries - 1:
                Path(tmp).unlink(missing_ok=True)
                raise
            time.sleep(min(max_delay, base_delay * (2 ** attempt)))


def _fetch_links(ids, link_cap, concurrency):
    """Fetch downstream relationships for the given item ids (collected during the streaming build)."""
    ids = list(ids)
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


def _resolve_picklists(items, concurrency, cache=None):
    """Resolve the status/priority picklist-option ids in `items` to names. `cache` (option_id -> name) is
    reused ACROSS streaming batches so each option is fetched only once. Returns (name_map_for_these_items,
    new_rows) where new_rows are only the freshly-fetched picklist rows to insert (INSERT OR IGNORE)."""
    cache = cache if cache is not None else {}
    wanted = {str(scalar(it.get("fields", {}).get(k)))
              for it in items for k in ("status", "priority")
              if str(scalar(it.get("fields", {}).get(k) or "")).isdigit()}
    new_ids = [i for i in wanted if i not in cache]
    rows = []
    if new_ids:
        def fetch(opt_id):
            try:
                return opt_id, api_get(f"/rest/v1/picklistoptions/{opt_id}").get("data")
            except RuntimeError:
                return opt_id, None

        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            for opt_id, d in ex.map(fetch, new_ids):
                if d and d.get("name"):
                    cache[opt_id] = d["name"]
                    rows.append((d["id"], d["name"], d.get("pickList")))
    name_map = {i: cache[i] for i in wanted if i in cache}  # full mapping for THIS batch (incl. cache hits)
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
        steps_text(f),
    )


def _rows_from_items(items, concurrency, pick_cache=None):
    """Turn raw API items into (item_rows, kv_rows, pick_rows). Shared by full build + incremental sync.
    `pick_cache` (optional) memoizes resolved picklist options across streaming batches."""
    tkey, tname = type_maps()
    picks, pick_rows = _resolve_picklists(items, concurrency, pick_cache)
    item_rows, kv_rows = [], []
    for it in items:
        item_rows.append(_item_row(it, tkey, tname, picks))
        item_id = scalar(it.get("id"))
        for fk, fv in (it.get("fields") or {}).items():
            if fk != "description":  # raw HTML dropped; plain text lives in items.description
                kv_rows.append((item_id, fk, field_text(fv)))
    return item_rows, kv_rows, pick_rows


def build_db(proj_id, name, with_links=False, link_cap=0, concurrency=CONCURRENCY_DEFAULT):
    """Full download -> per-project SQLite cache, STREAMED to disk wave-by-wave (see iter_pages) so peak
    memory stays bounded (~concurrency*PAGE items) no matter how large the project is — instead of buffering
    the whole project in memory. Each wave's rows are inserted and COMMITTED into a per-process temp DB (so
    SQLite never holds the whole project in its page cache either); FTS + meta are built at the end and the
    temp is atomically swapped onto the live cache. Concurrent downloads of the same project use DISTINCT
    temp files (PID + counter) and the last atomic swap wins (both hold equivalent data) -> no memory blowup,
    no temp-file clash, and readers never see a torn cache. A crash/Ctrl-C just discards the temp; the
    previous good cache stays intact."""
    timings = {"fetch_ms": 0, "link_ms": 0, "write_ms": 0}
    p = db_path(proj_id)
    tmp = _unique_tmp(p, "db")
    tmp.unlink(missing_ok=True)
    con = sqlite3.connect(str(tmp))
    pick_cache, link_ids, watermark = {}, [], ""   # pick_cache memoizes picklists across waves
    placeholders = ",".join("?" * len(ITEM_COLUMNS))
    item_cols = ",".join(ITEM_COLUMNS)
    t = time.time()
    try:
        con.executescript(DDL)
        # STREAM: one wave (<= concurrency pages) at a time; insert + commit + drop before the next wave.
        for wave in iter_pages(f"/rest/v1/abstractitems?project={proj_id}", concurrency=concurrency,
                               progress_label=f"download {name}"):
            if not wave:
                continue
            item_rows, kv_rows, pick_rows = _rows_from_items(wave, concurrency, pick_cache)
            # OR REPLACE: a long sweep can re-see an item that shifted pages under concurrent server edits;
            # overwrite instead of crashing on a duplicate primary key.
            con.executemany(f"INSERT OR REPLACE INTO items({item_cols}) VALUES ({placeholders})", item_rows)
            con.executemany("INSERT INTO fields_kv(itemId,key,value) VALUES (?,?,?)", kv_rows)
            if pick_rows:
                con.executemany("INSERT OR IGNORE INTO picklist(id,name,pickList) VALUES (?,?,?)", pick_rows)
            bw = max((r[_MODIFIED_IDX] for r in item_rows if r[_MODIFIED_IDX]), default="")
            if bw > watermark:
                watermark = bw
            if with_links:
                link_ids.extend(r[0] for r in item_rows)
            con.commit()  # flush this wave so SQLite's page cache (and our row lists) don't accumulate
        timings["fetch_ms"] = int((time.time() - t) * 1000)

        t = time.time()
        edges = _fetch_links(link_ids, link_cap, concurrency) if with_links else []
        timings["link_ms"] = int((time.time() - t) * 1000)

        t = time.time()
        # build the external-content FTS index once, from the fully-populated items table
        con.execute("INSERT INTO fts(rowid, name, description, stepsText) "
                    "SELECT id, name, description, stepsText FROM items")
        if edges:
            rt = reltype_map()
            con.executemany(
                "INSERT OR IGNORE INTO relationships(id,fromItem,toItem,relationshipType,relTypeName,suspect) "
                "VALUES (?,?,?,?,?,?)",
                [(scalar(e.get("id")), scalar(e.get("fromItem")), scalar(e.get("toItem")),
                  scalar(e.get("relationshipType")), rt.get(str(e.get("relationshipType"))),
                  1 if e.get("suspect") else 0) for e in edges])
        n_items, n_kv, n_pick = con.execute(
            "SELECT (SELECT COUNT(*) FROM items), (SELECT COUNT(*) FROM fields_kv), "
            "(SELECT COUNT(*) FROM picklist)").fetchone()
        now = time.time()
        meta = {
            "project_id": str(proj_id), "project_name": name,
            "fetched_at": repr(now), "last_sync_at": repr(now), "watermark": watermark,
            "item_count": str(n_items), "field_kv_count": str(n_kv),
            "relationship_count": str(len(edges)), "picklist_count": str(n_pick),
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
    _atomic_swap(tmp, p)  # atomic swap onto the live cache path (retries a Windows reader-lock race)
    timings["write_ms"] = int((time.time() - t) * 1000)

    return {"proj_id": proj_id, "name": name, "items": n_items, "fields": n_kv,
            "links": len(edges), "picklist": n_pick,
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
    path = f"/rest/v1/abstractitems?project={proj_id}&modifiedDate={enc}"
    p = db_path(proj_id)
    placeholders = ",".join("?" * len(ITEM_COLUMNS))
    item_cols = ",".join(ITEM_COLUMNS)
    # STREAM the delta wave-by-wave (see iter_pages) so a large `update` never buffers the whole changed set
    # in memory; the cache is cloned lazily only once we see the first changed item, and each wave is upserted
    # + committed + released. FTS is rebuilt once at the end. Mirrors build_db's bounded-memory pattern.
    tmp = con = None
    pick_cache, pulled, new_wm = {}, 0, watermark
    try:
        for wave in iter_pages(path, concurrency=concurrency, progress_label=f"sync {name}"):
            if not wave:
                continue
            if con is None:  # per-process temp -> concurrent syncs of the same project don't collide
                tmp = _unique_tmp(p, "db")
                tmp.unlink(missing_ok=True)
                shutil.copy2(p, tmp)  # keeps the swap atomic and the original pristine on failure
                con = sqlite3.connect(str(tmp))
            item_rows, kv_rows, pick_rows = _rows_from_items(wave, concurrency, pick_cache)
            ids = [r[0] for r in item_rows]
            qmarks = ",".join("?" * len(ids))
            con.executemany(f"INSERT OR REPLACE INTO items({item_cols}) VALUES ({placeholders})", item_rows)
            con.execute(f"DELETE FROM fields_kv WHERE itemId IN ({qmarks})", ids)  # no PK -> clear, re-insert
            con.executemany("INSERT INTO fields_kv(itemId,key,value) VALUES (?,?,?)", kv_rows)
            if pick_rows:
                con.executemany("INSERT OR IGNORE INTO picklist(id,name,pickList) VALUES (?,?,?)", pick_rows)
            bw = max((r[_MODIFIED_IDX] for r in item_rows if r[_MODIFIED_IDX]), default="")
            if bw > new_wm:
                new_wm = bw
            pulled += len(item_rows)
            con.commit()  # flush this wave so neither SQLite's page cache nor our row lists accumulate
            item_rows = kv_rows = pick_rows = ids = None
    except BaseException:
        if con is not None:
            con.close()
        if tmp is not None:
            tmp.unlink(missing_ok=True)
        raise
    timings["fetch_ms"] = int((time.time() - t) * 1000)
    timings["pulled"] = pulled

    now = time.time()
    if con is None:  # nothing changed -> just stamp last_sync_at on the live cache (cheap, safe)
        try:
            live = sqlite3.connect(str(p))
            live.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('last_sync_at',?)", (repr(now),))
            live.commit()
            live.close()
        except sqlite3.Error:
            pass
        timings["upserted"] = 0
        return {"proj_id": proj_id, "name": name, "upserted": 0, "watermark": watermark,
                "size_mb": round(p.stat().st_size / 1048576, 2), **timings}

    t = time.time()
    try:
        con.execute("INSERT INTO fts(fts) VALUES('rebuild')")  # full FTS rebuild from items (~58ms @10k)
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
    _atomic_swap(tmp, p)
    timings["write_ms"] = int((time.time() - t) * 1000)
    timings["upserted"] = pulled
    # NOTE: vectors are refreshed separately by refresh_vectors(), which sources the changed set from the
    # main cache by vec_watermark — so this return intentionally carries no item list for the vector path.
    return {"proj_id": proj_id, "name": name, "upserted": pulled, "watermark": new_wm,
            "size_mb": round(p.stat().st_size / 1048576, 2), **timings}


# ============================ deletion reconcile (OPT-IN: prune server-side deletions) ============================
# Incremental sync only catches adds/changes (by modifiedDate). This OPT-IN step removes items deleted on
# the server that still linger in the cache. Performance: it must run AFTER a normal sync (so the cache holds
# every current server item), which makes the cache a strict SUPERSET of the server -> local_count -
# server_count == the exact number of stale (deleted) items. So a SINGLE cheap count request decides whether
# any deletions exist; the full id sweep runs ONLY when that count says some do.
def _batches(seq, n=500):
    """Yield slices of <= n (keeps SQL `IN (...)` parameter lists under SQLite's limit)."""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _server_item_count(proj_id):
    """Current server item count for a project — one cheap request (maxResults=1, read totalResults)."""
    d = api_get(f"/rest/v1/abstractitems?project={proj_id}&startAt=0&maxResults=1")
    return int(d["meta"]["pageInfo"]["totalResults"])


def _fetch_all_ids(proj_id, concurrency=CONCURRENCY_DEFAULT):
    """All current server item ids for a project (paged sweep, concurrent). Returns a set of ints. This is
    the only expensive part of a reconcile, so callers gate it behind the cheap count pre-check."""
    items = get_pages(f"/rest/v1/abstractitems?project={proj_id}", concurrency=concurrency,
                      progress_label=f"id-sweep {proj_id}")
    return {int(it["id"]) for it in items if it.get("id") is not None}


def _apply_deletions(proj_id, deleted_ids, quiet=False):
    """Remove the given item ids from the main cache (items, fields_kv, relationships, FTS) AND the vector
    index (chunks via chunk_map). Atomic copy+swap per file, so a failure leaves both caches intact.
    Pure-local — no network — so it is unit-testable on a copied cache."""
    ids = sorted({int(i) for i in deleted_ids})
    if not ids:
        return {"items": 0, "chunks": 0}
    p = db_path(proj_id)
    tmp = _unique_tmp(p, "db")
    tmp.unlink(missing_ok=True)
    shutil.copy2(p, tmp)
    con = sqlite3.connect(str(tmp))
    try:
        for batch in _batches(ids):
            q = ",".join("?" * len(batch))
            con.execute(f"DELETE FROM items WHERE id IN ({q})", batch)
            con.execute(f"DELETE FROM fields_kv WHERE itemId IN ({q})", batch)
            con.execute(f"DELETE FROM relationships WHERE fromItem IN ({q})", batch)
            con.execute(f"DELETE FROM relationships WHERE toItem IN ({q})", batch)
        con.execute("INSERT INTO fts(fts) VALUES('rebuild')")  # rebuild external-content FTS from items
        ic = con.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        kc = con.execute("SELECT COUNT(*) FROM fields_kv").fetchone()[0]
        con.executemany("INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)",
                        [("item_count", str(ic)), ("field_kv_count", str(kc)),
                         ("last_sync_at", repr(time.time()))])
        con.commit()
    except BaseException:
        con.close()
        tmp.unlink(missing_ok=True)
        raise
    con.close()
    _atomic_swap(tmp, p)

    # vector index: drop the deleted items' chunks (no embedding needed -> cheap)
    chunks_removed = 0
    state, _ = vec_index_state(proj_id)
    if state in ("ready", "stale"):
        ensure_vectors()
        vp = vec_db_path(proj_id)
        vtmp = _unique_tmp(vp, "vec.db")
        vtmp.unlink(missing_ok=True)
        shutil.copy2(vp, vtmp)
        con = _vec_connect(vtmp)
        try:
            for batch in _batches(ids):
                q = ",".join("?" * len(batch))
                cids = [r[0] for r in
                        con.execute(f"SELECT chunk_id FROM chunk_map WHERE item_id IN ({q})", batch).fetchall()]
                for cb in _batches(cids):
                    cq = ",".join("?" * len(cb))
                    con.execute(f"DELETE FROM vec WHERE chunk_id IN ({cq})", cb)
                    con.execute(f"DELETE FROM chunk_map WHERE chunk_id IN ({cq})", cb)
                    chunks_removed += len(cb)
            vc = con.execute("SELECT COUNT(*) FROM vec").fetchone()[0]
            icv = con.execute("SELECT COUNT(DISTINCT item_id) FROM chunk_map").fetchone()[0]
            con.executemany("INSERT OR REPLACE INTO vmeta(key,value) VALUES (?,?)",
                            [("vec_count", str(vc)), ("item_count", str(icv)), ("built_at", repr(time.time()))])
            con.commit()
        except BaseException:
            con.close()
            vtmp.unlink(missing_ok=True)
            raise
        con.close()
        _atomic_swap(vtmp, vp)
    return {"items": len(ids), "chunks": chunks_removed}


def reconcile_deletions(proj_id, name, concurrency=CONCURRENCY_DEFAULT, quiet=False):
    """OPT-IN: remove cached items that were deleted on the server. Cheap by design — a single count request
    decides whether a full id sweep is even needed (see section note). MUST run after a normal sync so the
    cache is a superset of the server. Network-touching; not for --offline."""
    p = db_path(proj_id)
    if not p.exists():
        if not quiet:
            print(f"[{proj_id} {name}] no cache to reconcile.")
        return {"deleted": 0, "method": "none"}
    con = open_db(proj_id)
    try:
        local_ids = {r[0] for r in con.execute("SELECT id FROM items")}
    finally:
        con.close()
    server_count = _server_item_count(proj_id)  # one cheap request
    if len(local_ids) <= server_count:
        if not quiet:
            print(f"[{proj_id} {name}] no deletions to prune (local {len(local_ids)} <= server {server_count}).")
        return {"deleted": 0, "method": "count"}
    if not quiet:
        print(f"[{proj_id} {name}] local {len(local_ids)} > server {server_count} -> sweeping ids to find "
              f"{len(local_ids) - server_count} deletion(s)...")
    server_ids = _fetch_all_ids(proj_id, concurrency)
    deleted = local_ids - server_ids
    if not deleted:  # count differed but every local id still exists (e.g. a concurrent add mid-sweep)
        if not quiet:
            print(f"[{proj_id} {name}] no stale ids after sweep; nothing pruned.")
        return {"deleted": 0, "method": "sweep"}
    res = _apply_deletions(proj_id, deleted, quiet=quiet)
    if not quiet:
        print(f"[{proj_id} {name}] pruned {res['items']} deleted item(s) "
              f"({res['chunks']} vector chunk(s) removed).")
    return {"deleted": res["items"], "chunks": res["chunks"], "method": "sweep"}


# ============================ downloads: China-first mirrors + live speed test + progress ============================
# SPEC: every NON-Jama download (pip deps + the embedding model) prefers a China mirror, falls back to the
# international source, and ABORTS if neither is fast enough. Jama API traffic (api_get) is NOT touched.
# A short speed probe (download a ~1MB sample) ranks candidates: China mirrors are tried first; the first
# one sustaining >= MIN_KBPS wins; if every China mirror is too slow we try the international source; if
# that is also too slow/unreachable we stop and tell the user the network looks broken.
PYPI_MIRRORS = [  # (label, simple-index base, is_china); China entries first, international last
    ("tuna-tsinghua", "https://pypi.tuna.tsinghua.edu.cn/simple", True),
    ("aliyun",        "https://mirrors.aliyun.com/pypi/simple",   True),
    ("tencent",       "https://mirrors.cloud.tencent.com/pypi/simple", True),
    ("pypi.org",      "https://pypi.org/simple",                  False),
]
HF_MIRRORS = [  # (label, HF endpoint, is_china); hf-mirror.com is the standard China HuggingFace mirror
    ("hf-mirror.com", "https://hf-mirror.com",  True),
    ("huggingface.co", "https://huggingface.co", False),
]
# The HF repo fastembed actually pulls for BAAI/bge-base-en-v1.5 (cache dir models--qdrant--...-onnx-q),
# used both to speed-test the mirror and to detect "already downloaded".
HF_MODEL_REPO = "qdrant/bge-base-en-v1.5-onnx-q"
HF_MODEL_FILE = "model_optimized.onnx"
PROBE_BYTES = 1 << 20   # speed-probe sample size (1 MiB)
PROBE_TIMEOUT = 12      # seconds per probe


def min_kbps():
    """'Too slow' cutoff for the download speed test, in KB/s. Override with $JAMA_MIN_KBPS. Default 150
    KB/s (~24 min worst case for the 210MB model); healthy mirrors run far faster and are picked first."""
    try:
        return float(os.environ.get("JAMA_MIN_KBPS", "150"))
    except ValueError:
        return 150.0


def _probe_kbps(url, sample=PROBE_BYTES, timeout=PROBE_TIMEOUT):
    """Download up to `sample` bytes from url and return throughput in KB/s, or None on any failure/timeout.
    urllib follows 3xx automatically, so HF LFS resolve->CDN redirects are handled. We stop at `sample`
    bytes or `timeout` seconds regardless of whether the server honours the Range header."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "jama-offline/4.2",
                                                   "Range": f"bytes=0-{sample - 1}"})
        t0 = time.time()
        got = 0
        with urllib.request.urlopen(req, timeout=timeout) as r:
            while got < sample and time.time() - t0 <= timeout:
                chunk = r.read(65536)
                if not chunk:
                    break
                got += len(chunk)
        el = time.time() - t0
        if got <= 0 or el <= 0:
            return None
        return (got / 1024.0) / el
    except Exception:
        return None


def choose_mirror(candidates, probe_url, what):
    """Pick a download source by live speed test. `candidates` = (label, base, is_china) with China first.
    Probe each in order; return (label, base) of the FIRST whose speed >= min_kbps(), else None (=> the
    caller aborts because neither China nor international was fast enough). `probe_url` maps base -> a real
    sample URL on that host."""
    need = min_kbps()
    for label, base, is_cn in candidates:
        kbps = _probe_kbps(probe_url(base))
        where = "China" if is_cn else "international"
        if kbps is None:
            print(f"[net] {what}: {label} ({where}) unreachable -> trying next…", file=sys.stderr)
        elif kbps >= need:
            print(f"[net] {what}: {label} ({where}) {kbps:.0f} KB/s OK -> using it", file=sys.stderr)
            return label, base
        else:
            print(f"[net] {what}: {label} ({where}) only {kbps:.0f} KB/s (< {need:.0f}) -> trying next…",
                  file=sys.stderr)
    return None


def _stream_subprocess(cmd, label, interval=2.0):
    """Run `cmd`, streaming a throttled one-line status to stderr every `interval`s (latest output line +
    elapsed seconds) so long pip steps give periodic feedback. Returns (returncode, full_output)."""
    import subprocess
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            bufsize=1, encoding="utf-8", errors="replace")
    st = {"line": "", "done": False}
    log = []

    def reader():
        for ln in proc.stdout:
            ln = ln.rstrip()
            if ln:
                log.append(ln)
                st["line"] = ln
        st["done"] = True

    th = threading.Thread(target=reader, daemon=True)
    th.start()
    t0 = last = time.time()
    while not st["done"]:
        time.sleep(0.2)
        now = time.time()
        if now - last >= interval:
            last = now
            sys.stderr.write(f"\r  {label}  {int(now - t0):4d}s  {st['line'][:78]:<80}")
            sys.stderr.flush()
    proc.wait()
    th.join(timeout=2)
    sys.stderr.write(f"\r  {label}  done in {int(time.time() - t0)}s{' ' * 86}\n")
    sys.stderr.flush()
    return proc.returncode, "\n".join(log)


class _DirGrowthProgress(threading.Thread):
    """Periodic stderr progress for an opaque download (the model files) by watching a directory grow:
    reports MB written + elapsed (+ % when est_mb is known). Robust — independent of the downloader's own
    output, so it works whether fastembed prints a bar or not."""

    def __init__(self, path, label, est_mb=None, interval=2.0):
        super().__init__(daemon=True)
        self.path, self.label, self.est_mb, self.interval = Path(path), label, est_mb, interval
        self._stop = threading.Event()
        self._base = self._size_mb()
        self._t0 = time.time()

    def _size_mb(self):
        total = 0
        try:
            for f in self.path.rglob("*"):
                try:
                    if f.is_file():
                        total += f.stat().st_size
                except OSError:
                    pass
        except OSError:
            pass
        return total / 1048576.0

    def run(self):
        while not self._stop.wait(self.interval):
            mb = max(0.0, self._size_mb() - self._base)
            el = int(time.time() - self._t0)
            if self.est_mb:
                pct = min(99, int(mb * 100 / self.est_mb))
                bar = "#" * (pct // 5) + "-" * (20 - pct // 5)
                sys.stderr.write(f"\r  {self.label} [{bar}] {pct:3d}%  {mb:4.0f}/{self.est_mb}MB  {el:4d}s ")
            else:
                sys.stderr.write(f"\r  {self.label}  {mb:4.0f}MB  {el:4d}s ")
            sys.stderr.flush()

    def stop(self):
        self._stop.set()
        mb = max(0.0, self._size_mb() - self._base)
        sys.stderr.write(f"\r  {self.label}  done {mb:.0f}MB in {int(time.time() - self._t0)}s{' ' * 40}\n")
        sys.stderr.flush()


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
# Streaming (re)build wave size: items pulled + chunked + embedded + inserted per wave (see build_vectors /
# refresh_vectors). Bounds peak memory to ~one wave of chunks+vectors regardless of project size; small
# enough that progress ticks often, large enough to keep fastembed batches efficient, and well under
# SQLite's IN(...) parameter limit for the per-wave id fetch.
VEC_ITEM_WAVE = 200
VEC_SCHEMA = "2"  # v2: chunked index — vec is keyed by chunk_id, chunk_map folds chunk->item
# Chunking (instead of truncating) so a long test case's full name+description+steps is embedded and
# searchable end to end. 1500-char windows with 200-char overlap keep each chunk under bge's 512-token
# (~2k char) limit while preserving cross-boundary context. MAX_EMBED_CHARS is a pathological safety
# ceiling only (the largest real item here is ~31k chars, well under it) — effectively no truncation.
CHUNK_CHARS = 1500
CHUNK_OVERLAP = 200
MAX_EMBED_CHARS = 50000
VEC_MAX_DISTANCE = 0.30  # vector hits must have cosine distance <= this (i.e. cosine similarity >= 0.70)
LEG_CANDIDATES = 200     # FTS/LIKE per-leg candidate depth fed into RRF fusion. BM25's tail is near-zero
                         # noise (multi-word OR can match 40-70% of the corpus); 200 covers the real signal
                         # and the RRF tail weight 1/(60+200) is negligible. The FUSED output stays uncapped.
# Parallel queries can briefly hold the vec file open, so refresh_vectors's atomic swap may fail with a
# file-lock conflict (especially on Windows). Retry the swap with exponential backoff; if it still won't
# settle after the max tries, SKIP the refresh and let the search run on the existing index (offline-style
# fallback) instead of raising. vec_watermark is left unchanged, so the next query just retries the catch-up.
VEC_SWAP_RETRIES = 8
VEC_SWAP_BASE_DELAY = 0.1   # seconds; backoff = 0.1, 0.2, 0.4, ... capped at VEC_SWAP_MAX_DELAY
VEC_SWAP_MAX_DELAY = 2.0


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


def _pip_cmd(index_url, packages):
    cmd = [sys.executable, "-m", "pip", "install", "--disable-pip-version-check"]
    if index_url:  # mirror chosen by the speed test (else pip uses its own configured default)
        cmd += ["--index-url", index_url]
    return cmd + list(packages)


def _pip_install(packages):
    """pip install `packages` with periodic progress, preferring a China mirror chosen by the live speed
    test (China-first, international fallback, abort if both too slow). Honours a user-set PIP_INDEX_URL."""
    if os.environ.get("PIP_INDEX_URL"):  # user already configured an index -> just install + show progress
        rc, out = _stream_subprocess(_pip_cmd(None, packages), "[deps] pip install")
        if rc == 0:
            return
        sys.exit(f"pip install failed (PIP_INDEX_URL={os.environ['PIP_INDEX_URL']}):\n{out[-800:]}")
    chosen = choose_mirror(PYPI_MIRRORS, lambda b: f"{b}/numpy/", "pip deps")
    if chosen is None:
        sys.exit("网络异常：所有 PyPI 镜像（中国站点与国外站点）均无法以可用速度访问，已中止依赖安装。\n"
                 "Network error: no PyPI mirror (China or international) was fast enough; aborting dependency "
                 "install. Check the connection, set PIP_INDEX_URL, or lower JAMA_MIN_KBPS.")
    # try the speed-winner; if it errors mid-install (and it was a China mirror), fall back to pypi.org once
    order, intl = [chosen], (PYPI_MIRRORS[-1][0], PYPI_MIRRORS[-1][1])
    if chosen[1] != intl[1]:
        order.append(intl)
    last = ""
    for label, base in order:
        rc, out = _stream_subprocess(_pip_cmd(base, packages), f"[deps] pip install via {label}")
        if rc == 0:
            return
        last = out
        print(f"[deps] install via {label} failed -> trying next index…", file=sys.stderr)
    sys.exit(f"Could not auto-install vector deps. Last pip output:\n{last[-800:]}\n"
             f"Run manually: {sys.executable} -m pip install fastembed sqlite-vec")


def ensure_vectors():
    """Vectors are REQUIRED — no graceful degradation. If fastembed + sqlite-vec are absent, auto
    `pip install` them once (China-mirror-first, with progress) and retry. Exits only if install fails."""
    global _vectors_ready
    if _vectors_ready:
        return
    if _vectors_importable():
        _vectors_ready = True
        return
    print("[deps] vector libraries missing -> installing fastembed + sqlite-vec (one-time)...", file=sys.stderr)
    _pip_install(["fastembed", "sqlite-vec"])
    import importlib
    importlib.invalidate_caches()
    if not _vectors_importable():
        sys.exit("Vector deps still unavailable after install.")
    _vectors_ready = True


_embedder = None


def _model_cached():
    """True if the embedding model's ONNX weights are already in the cache (so no download will happen).
    Matches the fastembed/huggingface_hub cache layout: models--qdrant--bge-base-en-v1.5-onnx-q/.../*.onnx."""
    try:
        for d in model_cache_dir().glob("models--*bge-base-en-v1.5*"):
            if any(d.rglob(HF_MODEL_FILE)):
                return True
    except OSError:
        pass
    return False


def _configure_hf_endpoint():
    """Point huggingface_hub at a China mirror (speed-tested) BEFORE fastembed is imported. No-op when
    HF_ENDPOINT is already set, or the model is already cached (no download -> no need to probe/abort).
    Aborts if neither the China mirror nor huggingface.co is fast enough (spec)."""
    if os.environ.get("HF_ENDPOINT") or _model_cached():
        return
    probe = lambda base: f"{base}/{HF_MODEL_REPO}/resolve/main/{HF_MODEL_FILE}"
    chosen = choose_mirror(HF_MIRRORS, probe, "embedding model")
    if chosen is None:
        sys.exit("网络异常：HuggingFace 镜像（中国站点 hf-mirror.com 与官方 huggingface.co）均无法以可用速度"
                 "访问，已中止模型下载。\nNetwork error: neither the China HF mirror nor huggingface.co was "
                 "fast enough to download the embedding model; aborting. Check the connection or lower "
                 "JAMA_MIN_KBPS.")
    os.environ["HF_ENDPOINT"] = chosen[1]
    print(f"[model] HuggingFace endpoint -> {chosen[1]}", file=sys.stderr)


def get_embedder():
    """Lazy singleton TextEmbedding pinned to a persistent cache dir and 80%-CPU threads. On the first run
    (model not yet cached) it picks a China-first HF mirror by speed test and shows a download progress
    bar (MB / %) driven by watching the cache dir grow."""
    global _embedder
    if _embedder is None:
        os.environ.setdefault("OMP_NUM_THREADS", str(embed_threads()))  # belt-and-suspenders w/ threads=
        _configure_hf_endpoint()  # China-first HF mirror, set before importing fastembed/huggingface_hub
        from fastembed import TextEmbedding
        mk = lambda: TextEmbedding(model_name=EMBED_MODEL, threads=embed_threads(),
                                   cache_dir=str(model_cache_dir()))
        if _model_cached():
            _embedder = mk()
        else:
            print(f"[model] downloading {EMBED_MODEL} (~210MB, one-time)…", file=sys.stderr)
            hb = _DirGrowthProgress(model_cache_dir(), "[model] downloading", est_mb=210)
            hb.start()
            try:
                _embedder = mk()
            finally:
                hb.stop()
    return _embedder


def _item_text(name, description, steps):
    """Compose an item's full embeddable text: name, then description, then test-case steps. The name is
    joined to the description with '. '; steps are appended with a space. No length cap here — chunking
    (item_chunks) handles long items, so nothing is truncated."""
    n, d, s = (name or "").strip(), (description or "").strip(), (steps or "").strip()
    core = (n + ". " + d) if (n and d) else (n or d)
    if s:
        core = (core + " " + s) if core else s
    return core.strip()


def _chunk_text(text):
    """Split text into overlapping windows of CHUNK_CHARS (overlap CHUNK_OVERLAP) covering it completely.
    Short text -> a single chunk; empty -> []. Adjacent chunks share CHUNK_OVERLAP chars so a match never
    falls in a seam."""
    t = (text or "").strip()
    if not t:
        return []
    size, overlap = CHUNK_CHARS, CHUNK_OVERLAP
    if len(t) <= size:
        return [t]
    step = size - overlap
    out, start = [], 0
    while True:
        out.append(t[start:start + size])
        if start + size >= len(t):
            break
        start += step
    return out


def item_chunks(name, description, steps):
    """Compose (name+description+steps) then chunk it. MAX_EMBED_CHARS caps only pathological items."""
    return _chunk_text(_item_text(name, description, steps)[:MAX_EMBED_CHARS])


def _chunk_units(rows, start=0):
    """Expand item rows into per-chunk embedding units. Returns three parallel lists
    (chunk_ids, item_ids, texts); chunk_id is a CONTIGUOUS sequence beginning at start+1 (pass the running
    high-water mark so successive STREAMING waves never collide on the vec PK) and item_id its owning item.
    Every item yields >= 1 chunk, so no item is dropped from the index."""
    chunk_ids, item_ids, texts = [], [], []
    cid = start
    for r in rows:
        chunks = item_chunks(r["name"], r["description"], r["stepsText"]) or [(r["name"] or "")]
        for c in chunks:
            cid += 1
            chunk_ids.append(cid)
            item_ids.append(r["id"])
            texts.append(c)
    return chunk_ids, item_ids, texts


def _lensort(chunk_ids, item_ids, texts):
    """Reorder a wave's parallel (chunk_ids, item_ids, texts) by ascending text length so each fixed-size
    fastembed batch is length-homogeneous (it pads to the longest member, so mixing a 512-token monster with
    short docs wastes ~3x compute). The chunk<->item<->text alignment is preserved."""
    order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
    return ([chunk_ids[i] for i in order], [item_ids[i] for i in order], [texts[i] for i in order])


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
                    f"chunk_id INTEGER PRIMARY KEY, embedding float[{EMBED_DIM}] distance_metric=cosine)")
        # chunk_map folds a chunk hit back to its owning item (one item -> many chunks)
        con.execute("CREATE TABLE IF NOT EXISTS chunk_map(chunk_id INTEGER PRIMARY KEY, item_id INTEGER)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_chunk_item ON chunk_map(item_id)")
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


class _CountProgress(threading.Thread):
    """Time-based stderr progress for a long streaming count job (the wave-by-wave embed). The worker bumps
    `.done`; THIS thread is the sole writer, re-rendering 'label [bar] pct done/total rate ETA elapsed' every
    `interval`s — so feedback keeps ticking even while a single wave is mid-flight, and stdout stays clean for
    results. Single writer => no interleave with the worker."""

    def __init__(self, label, total, interval=3.0):
        super().__init__(daemon=True)
        self.label, self.total, self.interval = label, max(0, int(total)), interval
        self.done = 0
        self._stop = threading.Event()
        self._t0 = time.time()

    def _render(self):
        total = self.total or 1
        pct = min(100, int(self.done * 100 / total))
        el = time.time() - self._t0
        rate = self.done / el if el > 0 else 0
        eta = (self.total - self.done) / rate if (rate > 0 and self.total) else 0
        bar = "#" * (pct // 5) + "-" * (20 - pct // 5)
        sys.stderr.write(f"\r  {self.label} [{bar}] {pct:3d}%  {self.done}/{self.total}  "
                         f"{rate:.0f}/s  ETA {eta:4.0f}s  el {el:4.0f}s ")
        sys.stderr.flush()

    def run(self):
        while not self._stop.wait(self.interval):
            self._render()

    def stop(self):
        self._stop.set()
        self._render()
        sys.stderr.write("\n")
        sys.stderr.flush()


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
    if (m.get("embed_model") != EMBED_MODEL or m.get("dim") != str(EMBED_DIM)
            or m.get("vec_schema") != VEC_SCHEMA):  # v1 (item-keyed) -> rebuild as chunked v2
        return "stale", m
    return "ready", m


def build_vectors(proj_id, quiet=False):
    """Full (re)build of the project's CHUNKED vector index from the main cache, STREAMED wave-by-wave so
    peak memory stays bounded (~one VEC_ITEM_WAVE of chunks + their 768-d vectors) no matter how large the
    project is — instead of materializing every chunk and every vector at once. Per wave: fetch that wave's
    item rows from the main cache (a brief read lock only — never held across the long embed), chunk them,
    length-sort, embed, INSERT + COMMIT into a per-process temp vec DB, then release. The model is loaded/
    downloaded up-front (its own progress bar); a time-based heartbeat then reports embedding progress on
    stderr. Atomic swap at the end; a crash/Ctrl-C just discards the temp and leaves any previous index
    intact."""
    ensure_vectors()  # required: auto-installs fastembed/sqlite-vec if missing
    main = open_db(proj_id)
    try:  # read just the id list + watermark up-front (tiny) so we never hold a read lock during the embed
        ids = [r[0] for r in main.execute("SELECT id FROM items ORDER BY id")]
        wmrow = main.execute("SELECT value FROM meta WHERE key='watermark'").fetchone()
    finally:
        main.close()
    n_items = len(ids)
    wm = (wmrow[0] if wmrow else "") or ""
    get_embedder()  # trigger model load/download now (own bar) BEFORE the embed heartbeat -> no interleave
    print(f"[vectors] embedding {n_items} items in streaming waves of {VEC_ITEM_WAVE} — {EMBED_MODEL}, "
          f"{embed_threads()} threads (one-time, ~35-45 min for 10k items on CPU; later syncs only "
          f"re-embed changed items)", file=sys.stderr)
    p = vec_db_path(proj_id)
    tmp = _unique_tmp(p, "vec.db")
    tmp.unlink(missing_ok=True)
    con = _vec_connect(tmp, create=True)
    cid, n_chunks, t = 0, 0, time.time()
    hb = _CountProgress("embed", n_items)
    hb.start()
    try:
        for wave_ids in _batches(ids, VEC_ITEM_WAVE):
            main = open_db(proj_id)
            try:
                q = ",".join("?" * len(wave_ids))
                rows = main.execute(f"SELECT id, name, description, stepsText FROM items WHERE id IN ({q})",
                                    wave_ids).fetchall()
            finally:
                main.close()
            chunk_ids, item_ids, texts = _chunk_units(rows, start=cid)  # contiguous ids above the last wave
            cid += len(chunk_ids)
            chunk_ids, item_ids, texts = _lensort(chunk_ids, item_ids, texts)
            vecs = embed_corpus(texts, quiet=True)  # the heartbeat shows progress; no inner per-call bar
            con.executemany("INSERT INTO vec(chunk_id, embedding) VALUES (?, ?)",
                            ((c, v.tobytes()) for c, v in zip(chunk_ids, vecs)))
            con.executemany("INSERT INTO chunk_map(chunk_id, item_id) VALUES (?, ?)",
                            zip(chunk_ids, item_ids))
            con.commit()  # flush this wave so neither SQLite's page cache nor our row/vector lists accumulate
            n_chunks += len(chunk_ids)
            hb.done += len(rows)
            rows = chunk_ids = item_ids = texts = vecs = None  # release before fetching the next wave
        meta = {"embed_model": EMBED_MODEL, "dim": str(EMBED_DIM), "vec_count": str(n_chunks),
                "item_count": str(n_items), "main_watermark": wm, "vec_watermark": wm,
                "built_at": repr(time.time()), "vec_schema": VEC_SCHEMA, "engine_version": ENGINE_VERSION}
        con.executemany("INSERT OR REPLACE INTO vmeta(key,value) VALUES (?,?)", list(meta.items()))
        con.commit()
    except BaseException:
        con.close()
        tmp.unlink(missing_ok=True)
        hb.stop()
        raise
    con.close()
    hb.stop()
    _atomic_swap(tmp, p)
    if not quiet:
        print(f"[vectors] built {n_chunks} chunk vectors ({n_items} items) in {time.time()-t:.0f}s "
              f"-> {p.name} ({p.stat().st_size/1048576:.1f} MB)")
    return {"vec_count": n_chunks, "item_count": n_items, "build_s": round(time.time() - t, 1)}


def refresh_vectors(proj_id, quiet=True):
    """Bring the EXISTING vec index up to date with the main cache (copy+swap), STREAMED so memory stays
    bounded even when many items changed (e.g. a big `update`). Re-embeds every item whose modifiedDate is
    >= the index's vec_watermark, reading the text LOCALLY from the main cache (no network). Sourcing the set
    from the main cache (not just what the last incremental pulled) is what stops the vector index drifting
    stale when a vectors-less command — e.g. `query` — synced changed items into the main cache. The changed
    items' OLD chunks are dropped up-front (cheap), then their fresh chunks are embedded + inserted wave-by-
    wave. No-op if the index is absent/stale (caller does a full build) or nothing changed."""
    state, vmeta = vec_index_state(proj_id)
    if state != "ready":
        return None  # absent/stale -> the caller's build_vectors() (re)builds the whole index instead
    wm = vmeta.get("vec_watermark") or vmeta.get("main_watermark") or ""
    if not wm:
        return None  # indeterminate watermark -> leave it to a full rebuild rather than re-embed everything
    main = open_db(proj_id)
    try:  # inclusive >= mirrors incremental_sync: catches an item written in the watermark's exact ms. Read
        ids = [r[0] for r in  # only the changed ids up-front (tiny) -> no read lock held during the embed
               main.execute("SELECT id FROM items WHERE modifiedDate >= ? ORDER BY id", (wm,))]
        wmrow = main.execute("SELECT value FROM meta WHERE key='watermark'").fetchone()
    finally:
        main.close()
    if not ids:
        return None
    new_wm = (wmrow[0] if wmrow else wm) or wm
    ensure_vectors()
    get_embedder()  # model up-front (own bar) before the embed heartbeat -> no interleave
    p = vec_db_path(proj_id)
    tmp = _unique_tmp(p, "vec.db")
    tmp.unlink(missing_ok=True)
    shutil.copy2(p, tmp)
    con = _vec_connect(tmp)
    hb = _CountProgress(f"re-embed {len(ids)} item(s)", len(ids))
    hb.start()
    try:
        # drop every existing chunk of the changed items first (an item's chunk count may change), then
        # re-add fresh chunks with ids above the surviving max so they never collide with what remains.
        for batch in _batches(ids):
            qb = ",".join("?" * len(batch))
            old = [r[0] for r in
                   con.execute(f"SELECT chunk_id FROM chunk_map WHERE item_id IN ({qb})", batch).fetchall()]
            for ob in _batches(old):
                cm = ",".join("?" * len(ob))
                con.execute(f"DELETE FROM vec WHERE chunk_id IN ({cm})", ob)
                con.execute(f"DELETE FROM chunk_map WHERE chunk_id IN ({cm})", ob)
        cid = con.execute("SELECT COALESCE(MAX(chunk_id), 0) FROM chunk_map").fetchone()[0]
        con.commit()
        for wave_ids in _batches(ids, VEC_ITEM_WAVE):  # embed the changed items wave-by-wave (bounded memory)
            main = open_db(proj_id)
            try:
                q = ",".join("?" * len(wave_ids))
                rows = main.execute(f"SELECT id, name, description, stepsText FROM items WHERE id IN ({q})",
                                    wave_ids).fetchall()
            finally:
                main.close()
            cids, iids, texts = _chunk_units(rows, start=cid)  # contiguous ids above the surviving max
            cid += len(cids)
            cids, iids, texts = _lensort(cids, iids, texts)
            vecs = embed_corpus(texts, quiet=True)  # heartbeat shows progress; no inner per-call bar
            con.executemany("INSERT INTO vec(chunk_id, embedding) VALUES (?, ?)",
                            ((c, v.tobytes()) for c, v in zip(cids, vecs)))
            con.executemany("INSERT INTO chunk_map(chunk_id, item_id) VALUES (?, ?)", zip(cids, iids))
            con.commit()
            hb.done += len(rows)
            rows = cids = iids = texts = vecs = None  # release before the next wave
        vc = con.execute("SELECT COUNT(*) FROM vec").fetchone()[0]
        ic = con.execute("SELECT COUNT(DISTINCT item_id) FROM chunk_map").fetchone()[0]
        con.executemany("INSERT OR REPLACE INTO vmeta(key,value) VALUES (?,?)",
                        [("built_at", repr(time.time())), ("vec_count", str(vc)), ("item_count", str(ic)),
                         ("vec_watermark", new_wm)])
        con.commit()
    except BaseException:
        con.close()
        tmp.unlink(missing_ok=True)
        hb.stop()
        raise
    con.close()
    hb.stop()
    # Retry the swap on a transient file-lock conflict from parallel access; on exhaustion, skip the
    # refresh (use the existing index — offline-style fallback) rather than crash the query.
    for _attempt in range(VEC_SWAP_RETRIES):
        try:
            os.replace(str(tmp), str(p))
            return {"updated": len(ids)}
        except OSError:
            if _attempt == VEC_SWAP_RETRIES - 1:
                tmp.unlink(missing_ok=True)
                if not quiet:
                    print(f"[vectors] vec file busy from parallel access after {VEC_SWAP_RETRIES} tries -> "
                          f"skipped refresh, using existing index", file=sys.stderr)
                return None
            time.sleep(min(VEC_SWAP_MAX_DELAY, VEC_SWAP_BASE_DELAY * (2 ** _attempt)))


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
    """ALL (item_id, cosine_score) within the distance threshold, nearest first — no count cap. The index
    is CHUNKED: KNN runs over chunks, then each chunk is folded back to its item (chunk_map) keeping the
    NEAREST chunk per item. vec0 needs a LIMIT, so we ask for the whole chunk set then threshold."""
    _ensure_vector_index(proj_id)
    _, vmeta = vec_index_state(proj_id)
    # vec0 KNN requires a LIMIT and caps it at 4096 chunks. An item's best chunk is its nearest, so 4096
    # chunks comfortably covers every item within a sensible similarity threshold (>=0.70).
    k = min(int(vmeta.get("vec_count") or 4096), 4096)
    qv = list(get_embedder().query_embed([query]))[0].astype("float32")  # bge query-side prefix applied
    con = _vec_connect(vec_db_path(proj_id))
    try:
        knn = con.execute(
            "SELECT m.item_id, k.distance FROM "
            "(SELECT chunk_id, distance FROM vec WHERE embedding MATCH ? ORDER BY distance LIMIT ?) k "
            "JOIN chunk_map m ON m.chunk_id = k.chunk_id ORDER BY k.distance",
            (qv.tobytes(), k)).fetchall()
    finally:
        con.close()
    best = {}  # item_id -> nearest distance (knn is distance-sorted, so first seen is nearest)
    for iid, dist in knn:
        if dist <= max_distance and iid not in best:
            best[iid] = dist
    return [(iid, round(1 - dist, 3)) for iid, dist in best.items()]  # distance = 1 - cosine sim


# ============================ boolean keyword expressions (search --expr / --keyword) ============================
# A tiny boolean mini-language over keywords for `search`: AND / OR / NOT + parentheses, e.g.
#   (upload or download) and (encrypt or compress) and not deprecated
# Operators are case-insensitive words (and/or/not) OR symbols (& && | || !); full-width parens （） are
# accepted; commas separate terms. Adjacent terms with no operator mean AND ("answer call" in quotes is
# ONE phrase term). The SAME parsed AST drives the FTS5 leg (native AND/OR/NOT) and the LIKE leg (nested
# SQL); the vector leg uses the positive leaf terms (or --query), since meaning-search can't honour
# boolean logic. Parsing is best-effort: a malformed expression falls back to a flat keyword list.
_TOK_LP, _TOK_RP, _TOK_AND, _TOK_OR, _TOK_NOT, _TOK_TERM = "LP", "RP", "AND", "OR", "NOT", "TERM"


def _tokenize_bool(s):
    s = (s or "").replace("（", "(").replace("）", ")")
    toks, i, n = [], 0, len(s)
    while i < n:
        c = s[i]
        if c.isspace() or c == ",":
            i += 1
        elif c == "(":
            toks.append((_TOK_LP, None)); i += 1
        elif c == ")":
            toks.append((_TOK_RP, None)); i += 1
        elif c == '"':                                  # quoted phrase = one TERM
            j = s.find('"', i + 1)
            if j < 0:
                j = n
            toks.append((_TOK_TERM, s[i + 1:j])); i = j + 1
        elif c == "&":
            toks.append((_TOK_AND, None)); i += 2 if s[i:i + 2] == "&&" else 1
        elif c == "|":
            toks.append((_TOK_OR, None)); i += 2 if s[i:i + 2] == "||" else 1
        elif c == "!":
            toks.append((_TOK_NOT, None)); i += 1
        else:                                           # bare word: term, or a word-operator (and/or/not)
            j = i
            while j < n and not s[j].isspace() and s[j] not in '(),&|!"':
                j += 1
            w, lw = s[i:j], s[i:j].lower()
            toks.append((_TOK_AND, None) if lw == "and" else
                        (_TOK_OR, None) if lw == "or" else
                        (_TOK_NOT, None) if lw == "not" else
                        (_TOK_TERM, w))
            i = j
    return toks


def parse_bool_expr(s):
    """Parse a boolean keyword expression into an AST: ('AND'|'OR', [children]) | ('NOT', child) |
    ('TERM', word). Raises ValueError on malformed input (caller falls back to a flat keyword list)."""
    toks = _tokenize_bool(s)
    if not toks:
        raise ValueError("empty expression")
    pos = [0]

    def peek():
        return toks[pos[0]][0] if pos[0] < len(toks) else None

    def take():
        t = toks[pos[0]]; pos[0] += 1; return t

    def p_or():
        nodes = [p_and()]
        while peek() == _TOK_OR:
            take(); nodes.append(p_and())
        return ("OR", nodes) if len(nodes) > 1 else nodes[0]

    def p_and():
        nodes = [p_not()]
        while peek() in (_TOK_AND, _TOK_NOT, _TOK_TERM, _TOK_LP):  # explicit AND or implicit adjacency
            if peek() == _TOK_AND:
                take()
            nodes.append(p_not())
        return ("AND", nodes) if len(nodes) > 1 else nodes[0]

    def p_not():
        if peek() == _TOK_NOT:
            take(); return ("NOT", p_not())
        return p_atom()

    def p_atom():
        t = peek()
        if t == _TOK_LP:
            take(); node = p_or()
            if peek() != _TOK_RP:
                raise ValueError("unbalanced parenthesis")
            take(); return node
        if t == _TOK_TERM:
            return ("TERM", take()[1])
        raise ValueError(f"unexpected token {t}")

    tree = p_or()
    if pos[0] != len(toks):
        raise ValueError("trailing tokens")
    return tree


def bool_to_fts(node):
    """Compile the AST to an FTS5 MATCH string. FTS5 has no UNARY NOT, so NOT is only emitted as the right
    side of an AND (X NOT Y); a NOT that can't be placed that way raises -> the FTS leg is dropped (LIKE +
    vector still run)."""
    typ = node[0]
    if typ == "TERM":
        return '"' + node[1].replace('"', '""') + '"'
    if typ == "OR":
        return "(" + " OR ".join(bool_to_fts(c) for c in node[1]) + ")"  # OR-of-NOT child raises (intended)
    if typ == "AND":
        pos = [c for c in node[1] if c[0] != "NOT"]
        neg = [c[1] for c in node[1] if c[0] == "NOT"]
        if not pos:
            raise ValueError("FTS needs at least one positive term in an AND")
        expr = "(" + " AND ".join(bool_to_fts(c) for c in pos) + ")"
        for ng in neg:
            expr += " NOT " + bool_to_fts(ng)
        return expr
    raise ValueError("NOT not expressible in FTS at this position")  # bare/top-level NOT


def bool_to_like(node, col):
    """Compile the AST to a SQL boolean over `col LIKE '%term%'` (full AND/OR/NOT + parens)."""
    typ = node[0]
    if typ == "TERM":
        return f"{col} LIKE {sql_lit('%' + node[1] + '%')}"
    if typ == "OR":
        return "(" + " OR ".join(bool_to_like(c, col) for c in node[1]) + ")"
    if typ == "AND":
        return "(" + " AND ".join(bool_to_like(c, col) for c in node[1]) + ")"
    if typ == "NOT":
        return "(NOT " + bool_to_like(node[1], col) + ")"
    raise ValueError("bad node")


def bool_terms(node, positive_only=True):
    """Leaf terms of the AST; skips NOT-negated terms when positive_only (used to seed the vector leg)."""
    t = node[0]
    if t == "TERM":
        return [node[1]]
    if t == "NOT":
        return [] if positive_only else bool_terms(node[1], positive_only)
    return [x for c in node[1] for x in bool_terms(c, positive_only)]


def ast_from_keywords(keywords, match):
    """Build a flat AST from a comma keyword list + --match (any=OR, all=AND). None if no keywords."""
    if not keywords:
        return None
    leaves = [("TERM", k) for k in keywords]
    if len(leaves) == 1:
        return leaves[0]
    return ("AND" if match == "all" else "OR", leaves)


def _fts_ids(con, ast, field, dates=None):
    """Top-LEG_CANDIDATES FTS (keyword/BM25) matches for the boolean AST — the relevant head; the long BM25
    tail is noise. Returns [] when the AST is None or not expressible in FTS5 (other legs still run).
    `dates` (optional) further restricts to a created/modified range, pushed into the SQL for recall."""
    if ast is None:
        return []
    try:
        m = bool_to_fts(ast)
    except ValueError:
        return []  # e.g. a top-level NOT -> not expressible in FTS5; rely on LIKE + vector
    if field == "name":
        m = "name : (" + m + ")"
    where = "fts MATCH ?" + "".join(" AND " + c for c in _date_conds(dates, "i."))
    try:
        rows = con.execute(f"SELECT i.id FROM fts JOIN items i ON i.id = fts.rowid WHERE {where} "
                           "ORDER BY bm25(fts) LIMIT ?", (m, LEG_CANDIDATES)).fetchall()
        return [r[0] for r in rows]
    except sqlite3.OperationalError:
        return []  # odd chars -> fts5 syntax error: drop this leg, the others still run


def _like_ids(con, ast, field, dates=None):
    """Up-to-LEG_CANDIDATES substring (LIKE) matches for the boolean AST, in document order (deterministic).
    Need EVERY item containing a term? That's a SQL `query`, not search. `dates` adds a created/modified
    range filter."""
    if ast is None:
        return []
    col = ("(name || ' ' || COALESCE(description,'') || ' ' || COALESCE(stepsText,''))"
           if field == "all" else "name")
    where = bool_to_like(ast, col) + "".join(" AND " + c for c in _date_conds(dates, ""))
    rows = con.execute(f"SELECT id FROM items WHERE {where} "
                       f"ORDER BY CASE WHEN globalSortOrder IS NULL THEN 1 ELSE 0 END, globalSortOrder "
                       f"LIMIT ?", (LEG_CANDIDATES,)).fetchall()
    return [r[0] for r in rows]


def _join_items(proj_id, ids, type_arg, dates=None):
    """Map id -> item row for the given ids, applying an optional --type filter and created/modified date
    range. This is also where the vector leg (which has no SQL pre-filter) gets the date/type filter."""
    if not ids:
        return {}
    main = open_db(proj_id)
    try:
        where = f"id IN ({','.join('?' * len(ids))})"
        tclause = type_clause(type_arg)
        if tclause:
            where += " AND " + tclause
        where += "".join(" AND " + c for c in _date_conds(dates, ""))
        rows = main.execute(f"SELECT id, documentKey, typeKey, sequence, name FROM items WHERE {where}",
                            ids).fetchall()
    finally:
        main.close()
    return {r["id"]: r for r in rows}


def semantic_search(proj_id, query, top=0, type_arg=None, max_distance=VEC_MAX_DISTANCE, dates=None):
    """Pure vector search: every item within the cosine-distance threshold, nearest first. top=0 = no cap.
    `dates` (created/modified range) is applied when folding chunk hits back to items."""
    hits = _semantic_ids(proj_id, query, max_distance)  # distance <= max_distance
    by_id, cos, out = _join_items(proj_id, [i for i, _ in hits], type_arg, dates), dict(hits), []
    for iid, _ in hits:
        if iid in by_id:
            if top and len(out) >= top:
                break
            r = by_id[iid]
            out.append({"id": iid, "documentKey": r["documentKey"], "typeKey": r["typeKey"],
                        "sequence": r["sequence"], "score": cos[iid], "name": r["name"]})
    return out


def hybrid_search(proj_id, query_text, ast, field="all", top=0, type_arg=None,
                  max_distance=VEC_MAX_DISTANCE, dates=None):
    """DEFAULT content search: UNION of FTS (keyword/BM25) + LIKE (substring) + semantic (vector, thresholded
    at cosine distance <= max_distance), fused by Reciprocal Rank Fusion and de-duped by item id. The FTS +
    LIKE legs honour the boolean `ast` (AND/OR/NOT + parens); the vector leg uses `query_text` (meaning-
    search can't express boolean logic). An optional `dates` (created/modified range) filters all legs.
    Each leg returns ALL its matches (no cap); top=0 returns the whole fused union. `via` = which legs
    matched."""
    con = open_db(proj_id)
    try:
        legs = {"fts": _fts_ids(con, ast, field, dates),
                "like": _like_ids(con, ast, field, dates)}
    finally:
        con.close()
    legs["vec"] = [i for i, _ in _semantic_ids(proj_id, query_text, max_distance)] if query_text else []
    fused, srcs = {}, {}
    for leg, ids in legs.items():
        for rank, iid in enumerate(ids):
            fused[iid] = fused.get(iid, 0.0) + 1.0 / (60 + rank + 1)  # RRF, k=60
            srcs.setdefault(iid, set()).add(leg)
    if not fused:
        return []
    by_id = _join_items(proj_id, list(fused.keys()), type_arg, dates)
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
        if vec_index_state(proj_id)[0] == "ready":            # fastembed. semantic/search call this before
            refresh_vectors(proj_id, quiet=quiet)             # every query -> the index is never stale, incl.
        else:                                                 # items a vectors-less `query` synced into main.
            build_vectors(proj_id, quiet=quiet)               # absent/stale -> full (re)build
    if not quiet:
        if res.get("upserted"):
            print(f"[{proj_id} {name}] synced: {res['upserted']} item(s) updated")
        else:
            print(f"[{proj_id} {name}] up to date (no changes)")
    return res


# ============================ query preflight: offline-first, fail-fast gate ============================
# search / semantic / query NEVER silently kick off a long download/build. Before serving, preflight_query
# verifies the prerequisites EXIST and that any pending change is SMALL; otherwise it STOPS with an exact,
# actionable message (run `init` or `update`). A small delta (<= DELTA_LIMIT) is auto-synced (bounded,
# streamed). This is the behavioural heart of v4.3: a query on a not-yet-built / badly-stale project tells
# the user what to do instead of blocking for ~50 min on a full build.
def delta_limit():
    """Max changed/lagging items a query will auto-sync before it STOPS and asks the user to `update`
    explicitly. Default 200; override with $JAMA_DELTA_LIMIT (handy for tests, or a faster/slower link)."""
    try:
        return max(0, int(os.environ.get("JAMA_DELTA_LIMIT", "200")))
    except ValueError:
        return 200


def _modified_since_count(proj_id, watermark):
    """Cheap server-side count of items modified at/after `watermark` (one request, maxResults=1) — the size
    of the pending incremental delta (new + changed). None if the watermark is unusable. The boundary item(s)
    at exactly the watermark are included, so a fully-synced cache returns a small non-zero number."""
    if not watermark:
        return None
    enc = urllib.parse.quote(watermark, safe="")
    d = api_get(f"/rest/v1/abstractitems?project={proj_id}&modifiedDate={enc}&startAt=0&maxResults=1")
    return int(d["meta"]["pageInfo"]["totalResults"])


def _vec_stale_count(proj_id):
    """How many cached items are not yet reflected in the vector index (modifiedDate >= vec_watermark) —
    pure-local SQL, no network. Returns the count, or None if there is no ready index to compare against."""
    state, vmeta = vec_index_state(proj_id)
    if state != "ready":
        return None
    vwm = vmeta.get("vec_watermark") or vmeta.get("main_watermark") or ""
    con = open_db(proj_id)
    try:
        if vwm:
            return con.execute("SELECT COUNT(*) FROM items WHERE modifiedDate >= ?", (vwm,)).fetchone()[0]
        return con.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    finally:
        con.close()


def _stop_need_init(proj_id, name, need_vectors, reason):
    extra = " + a vector index" if need_vectors else ""
    sys.exit(f"[{proj_id} {name}] {reason}. Initialize it first (one-time full download{extra}):\n"
             f"    jama_offline.py init --project {proj_id}\n"
             f"（没有缓存文件，请先运行 init 初始化下载，再查询。）")


def preflight_query(proj_id, name, need_vectors, *, offline=False, force=False,
                    concurrency=CONCURRENCY_DEFAULT, quiet=True):
    """Gate a search/semantic/query per the OFFLINE-FIRST policy and, on success, do the bounded sync the
    query needs. NEVER silently triggers a long build. Order of checks:
      1. main cache present + readable               (else STOP -> init / rebuild)
      2. need_vectors: vector index + model present  (else STOP -> update)
      3. --offline: serve the existing cache as-is (no network)  |  --force: rebuild then serve
      4. server delta (modified since watermark) <= DELTA_LIMIT   (else STOP -> update)
      5. need_vectors: local vector lag <= DELTA_LIMIT            (else STOP -> update)
      6. PASS -> bounded incremental_sync (+ refresh_vectors) right here, then return.
    Every STOP is a sys.exit with an exact, bilingual, copy-pasteable next command."""
    info = cache_state(proj_id)

    # 1) main cache must exist and be readable
    if info["state"] == "missing":
        _stop_need_init(proj_id, name, need_vectors, "no offline cache yet")
    if info["state"] == "corrupt":
        sys.exit(f"[{proj_id} {name}] the cache is unreadable or built by an older schema. Rebuild it:\n"
                 f"    jama_offline.py rebuild --project {proj_id}\n"
                 f"（缓存文件损坏或版本过旧，请先 rebuild 重建，再查询。）")

    # 2) vector index + embedding model must exist for vector queries (search/semantic)
    if need_vectors:
        vstate, _ = vec_index_state(proj_id)
        if vstate == "absent":
            sys.exit(f"[{proj_id} {name}] no vector index ({vec_db_path(proj_id).name}) yet — build it once:\n"
                     f"    jama_offline.py update --project {proj_id}\n"
                     f"（没有向量缓存文件，请先 update 建立向量索引，再查询。）")
        if vstate == "stale":
            sys.exit(f"[{proj_id} {name}] the vector index was built with a different embedding model — "
                     f"rebuild it:\n    jama_offline.py update --project {proj_id}\n"
                     f"（向量索引与当前模型不一致，请先 update 重建向量，再查询。）")
        if not _model_cached():
            sys.exit(f"[{proj_id} {name}] the embedding model is not downloaded yet (~210MB) — fetch it once:\n"
                     f"    jama_offline.py update --project {proj_id}\n"
                     f"（没有模型文件，请先 update 下载嵌入模型，再查询。）")

    # 3) escape hatches: --offline serves as-is (no network); --force rebuilds (explicit user opt-in)
    if offline:
        if not quiet:
            print(f"[{proj_id} {name}] --offline -> using the existing cache as-is (no sync)")
        return
    if force:
        ensure_synced(proj_id, name, force=True, concurrency=concurrency,
                      want_vectors=need_vectors, quiet=quiet)
        return

    # 4) main-cache difference vs the server (one cheap count). Too big -> ask the user to `update`.
    lim = delta_limit()
    server_delta = _modified_since_count(proj_id, info["meta"].get("watermark") or "")
    if server_delta is None or server_delta > lim:
        howmany = "an unknown number of" if server_delta is None else str(server_delta)
        sys.exit(f"[{proj_id} {name}] {howmany} item(s) changed on the server since the last cache update "
                 f"(over the {lim}-item auto-sync limit). Update the cache first, then re-run your query:\n"
                 f"    jama_offline.py update --project {proj_id}\n"
                 f"（服务器自上次缓存以来变化超过 {lim} 条，请先 update 更新缓存，再查询。）")

    # 5) vector-index difference vs the main cache (vector queries only). Too big -> ask the user to `update`.
    if need_vectors:
        vstale = _vec_stale_count(proj_id)
        if vstale is not None and vstale > lim:
            sys.exit(f"[{proj_id} {name}] {vstale} cached item(s) are not yet in the vector index "
                     f"(over the {lim}-item limit). Update it first, then re-run your query:\n"
                     f"    jama_offline.py update --project {proj_id}\n"
                     f"（向量索引落后缓存超过 {lim} 条，请先 update 更新向量，再查询。）")

    # 6) passed -> bounded incremental sync (+ vector refresh) for the small delta, then serve
    res = incremental_sync(proj_id, name, concurrency)
    if res is None:  # watermark slipped away between cache_state and here -> point the user at rebuild
        sys.exit(f"[{proj_id} {name}] the cache has no usable watermark. Rebuild it:\n"
                 f"    jama_offline.py rebuild --project {proj_id}")
    if need_vectors and vec_index_state(proj_id)[0] == "ready":
        refresh_vectors(proj_id, quiet=quiet)


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


# Dates are stored as ISO-8601 text (e.g. 2026-06-18T09:59:23.000+0000), so lexicographic compare ==
# chronological compare. Filtering is inclusive on both ends. A bare YYYY-MM-DD upper bound is widened with
# a 'T99' sentinel (> any real 'THH..' time) so "--*-before 2026-06-30" includes ALL of the 30th.
_DATE_INPUT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([T ].*)?$")


def _date_conds(dates, alias=""):
    """SQL conditions for an optional created/modified range. `dates` = (created_after, created_before,
    modified_after, modified_before) already normalized, or None. `alias` (e.g. 'i.') prefixes the column
    when the query joins items under an alias. Returns a list of SQL strings (each a single condition)."""
    if not dates:
        return []
    ca, cb, ma, mb = dates
    out = []
    if ca:
        out.append(f"{alias}createdDate >= {sql_lit(ca)}")
    if cb:
        out.append(f"{alias}createdDate <= {sql_lit(cb)}")
    if ma:
        out.append(f"{alias}modifiedDate >= {sql_lit(ma)}")
    if mb:
        out.append(f"{alias}modifiedDate <= {sql_lit(mb)}")
    return out


def norm_dates(a):
    """Build the (created_after, created_before, modified_after, modified_before) tuple from CLI args,
    validating the format and widening bare-date upper bounds to include the whole day. None if unset."""
    raw = {k: getattr(a, k, None) for k in
           ("created_after", "created_before", "modified_after", "modified_before")}
    for k, v in raw.items():
        if v and not _DATE_INPUT_RE.match(v):
            sys.exit(f"Invalid --{k.replace('_', '-')} {v!r}: expected YYYY-MM-DD (optionally 'THH:MM:SS').")
    day_end = lambda v: v + "T99" if (v and re.fullmatch(r"\d{4}-\d{2}-\d{2}", v)) else v
    dates = (raw["created_after"], day_end(raw["created_before"]),
             raw["modified_after"], day_end(raw["modified_before"]))
    return dates if any(dates) else None


def keywords_of(values):
    return [k for v in (values or []) for k in (x.strip() for x in v.split(",")) if k]


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


def cmd_sync(a, force=False, gate=None):
    """Build / update a cache (+ its vector index). `gate` shapes the per-project policy:
      None     -> sync : build if missing, else incremental update (init + update in one).
      'init'   -> only build if MISSING/corrupt; if a cache already exists, skip with a hint (don't silently
                  rebuild — a full rebuild is the ~50-min path and must be asked for via `rebuild`).
      'update' -> only act if a cache EXISTS; if there is none yet, tell the user to `init` first.
    `force` (rebuild/refresh) ignores `gate` and does a clean full re-download. Vectors are built/maintained
    by default (--no-vectors to skip); --prune-deleted also removes server-side deletions."""
    projs = resolve_projects(a.project)
    force = force or a.force
    mode = ("REBUILD (full re-download)" if force else
            {"init": "INIT (first-time full build)", "update": "UPDATE (incremental)"}.get(gate, "sync"))
    print(f"[{mode}] resolved {len(projs)} project(s): " +
          ", ".join(f"{p['id']}={p['name']}" for p in projs))
    want_vectors = not a.no_vectors  # init/update/sync/rebuild build+maintain the vector index by default
    summary = []
    for pr in projs:
        if not force and gate in ("init", "update"):
            st = cache_state(pr["id"])["state"]
            if gate == "init" and st == "present":
                print(f"[{pr['id']} {pr['name']}] already initialized (cache present) — use `update` to refresh "
                      f"or `rebuild` to re-download from scratch. Skipping.")
                continue
            if gate == "update" and st == "missing":
                print(f"[{pr['id']} {pr['name']}] no cache yet — run `init --project {pr['id']}` first. Skipping.")
                continue
        t = time.time()
        s = ensure_synced(pr["id"], pr["name"], force=force,
                          with_links=a.with_links, link_cap=a.link_cap, concurrency=a.concurrency,
                          want_vectors=want_vectors)
        s = s or {"proj_id": pr["id"], "name": pr["name"], "items": "(no change)"}
        if getattr(a, "prune_deleted", False):  # OPT-IN: also remove server-side deletions (cache + vectors)
            rec = reconcile_deletions(pr["id"], pr["name"], concurrency=a.concurrency, quiet=a.json)
            s["pruned_deleted"] = rec.get("deleted", 0)
            if rec.get("chunks"):
                s["pruned_chunks"] = rec["chunks"]
        s["total_ms"] = int((time.time() - t) * 1000)
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
                vinfo = (f"{vstate}({vmeta.get('vec_count','?')} chunks/{vmeta.get('item_count','?')} items,"
                         f" {vp.stat().st_size/1048576:.1f}MB)")
            line.update(name=m.get("project_name"), items=m.get("item_count"),
                        links=m.get("relationship_count"),
                        last_sync=_fmt(m.get("last_sync_at")),
                        watermark=(m.get("watermark") or "")[:19],  # newest modifiedDate captured
                        size_mb=round(info["db"].stat().st_size / 1048576, 2),
                        vectors=vinfo)
        print("  " + "  ".join(f"{k}={v}" for k, v in line.items()))
    print(f"cache dir: {user_data_dir()}")


def cmd_search(a):
    # DEFAULT = hybrid (LIKE + keyword/FTS + semantic, RRF-fused, de-duped) per spec point 4. The FTS+LIKE
    # legs honour a boolean keyword AST: --expr "(a or b) and not c" (explicit), or --keyword a,b + --match
    # (flat OR/AND). The vector leg uses --query (or the positive leaf terms) for meaning-based recall.
    keywords = keywords_of(a.keyword)
    expr = " ".join(a.expr) if getattr(a, "expr", None) else None
    if expr:
        try:
            ast = parse_bool_expr(expr)
        except ValueError as e:
            sys.exit(f'Could not parse --expr "{expr}": {e}.  Example: "(upload or download) and not deprecated".')
    else:
        if not keywords and a.query:
            keywords = keywords_of(a.query)  # feed the FTS/LIKE legs from the query words too (as before)
        ast = ast_from_keywords(keywords, a.match)
    # vector-leg text: explicit --query, else the positive leaf terms of the AST
    query_text = " ".join(a.query) if a.query else (" ".join(bool_terms(ast)) if ast is not None else "")
    if ast is None and not query_text:
        sys.exit('search needs --keyword a,b , --expr "(a or b) and c" , or --query "natural language text".')
    md = a.max_distance if a.max_distance is not None else VEC_MAX_DISTANCE
    dates = norm_dates(a)  # optional created/modified range filter (applied to every leg)
    pr = single_project(a.project)
    # offline-first gate: STOP (with guidance) if the cache / vector index / model is missing or the pending
    # delta is over the limit; otherwise do the small bounded sync + vector refresh, then serve.
    preflight_query(pr["id"], pr["name"], need_vectors=True, offline=a.offline, force=a.force,
                    concurrency=a.concurrency)
    t = time.time()
    rows = hybrid_search(pr["id"], query_text, ast, field=a.field,
                         top=a.top, type_arg=a.type, max_distance=md, dates=dates)
    ms = int((time.time() - t) * 1000)
    emit_rows(rows, a.json, f"{len(rows)} hybrid match(es) [FTS+LIKE+vector; vec sim>={1-md:.2f}] in {ms} ms   "
                            f"[offline cache: {pr['id']} {pr['name']}]")


def cmd_query(a):
    if not a.sql:
        sys.exit("--sql required, e.g. --sql 'SELECT typeKey, COUNT(*) c FROM items GROUP BY typeKey'")
    if not re.match(r"(?is)^\s*(SELECT|WITH)\b", a.sql):
        sys.exit("Only read-only SELECT/WITH queries are allowed.")
    pr = single_project(a.project)
    # offline-first gate: needs the cache present and the server delta small (no vectors for SQL); else STOPs
    # with guidance to run init/update. A small delta is auto-synced before the query runs.
    preflight_query(pr["id"], pr["name"], need_vectors=False, offline=a.offline, force=a.force,
                    concurrency=a.concurrency)
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
        sys.exit('semantic needs --query "natural language text", e.g. --query "user can\'t upload a large file"')
    md = a.max_distance if a.max_distance is not None else VEC_MAX_DISTANCE
    dates = norm_dates(a)  # optional created/modified range filter
    pr = single_project(a.project)
    # offline-first gate: needs the cache, vector index AND embedding model present; STOPs with guidance if
    # anything is missing or the pending delta is over the limit, else does the small bounded sync + refresh.
    preflight_query(pr["id"], pr["name"], need_vectors=True, offline=a.offline, force=a.force,
                    concurrency=a.concurrency)
    t = time.time()
    rows = semantic_search(pr["id"], q, top=a.top, type_arg=a.type, max_distance=md, dates=dates)
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
    "init": lambda a: cmd_sync(a, gate="init"),       # first-time full build (data + vectors + model)
    "update": lambda a: cmd_sync(a, gate="update"),   # incremental update of an existing cache + vectors
    "sync": cmd_sync,                                  # build-if-missing else incremental (init+update in one)
    "rebuild": lambda a: cmd_sync(a, force=True),      # force a clean FULL re-download (drops deletions)
    "refresh": lambda a: cmd_sync(a, force=True),      # alias of rebuild
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
                        help="init/update/sync/rebuild: skip building/updating the semantic vector index")
        sp.add_argument("--prune-deleted", action="store_true", dest="prune_deleted",
                        help="init/update/sync/rebuild: also detect & remove items deleted on the server "
                             "(cache + vectors); OFF by default, cheap unless deletions exist")
        sp.add_argument("--with-links", action="store_true", dest="with_links")
        sp.add_argument("--link-cap", type=int, default=0, dest="link_cap")
        sp.add_argument("--all", action="store_true")
        if name == "search":
            sp.add_argument("--keyword", action="append", help="keyword(s), comma-separated")
            sp.add_argument("--expr", action="append",
                            help='boolean keyword expression for the FTS+LIKE legs: AND/OR/NOT + parentheses,'
                                 ' e.g. "(upload or download) and (encrypt or compress) and not deprecated"')
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
        if name in ("search", "semantic"):  # created/modified date-range filters (inclusive)
            sp.add_argument("--created-after", dest="created_after", default=None, metavar="DATE",
                            help="keep items with createdDate >= DATE (YYYY-MM-DD or full ISO)")
            sp.add_argument("--created-before", dest="created_before", default=None, metavar="DATE",
                            help="keep items with createdDate <= DATE (a bare date includes the whole day)")
            sp.add_argument("--modified-after", dest="modified_after", default=None, metavar="DATE",
                            help="keep items with modifiedDate >= DATE")
            sp.add_argument("--modified-before", dest="modified_before", default=None, metavar="DATE",
                            help="keep items with modifiedDate <= DATE (a bare date includes the whole day)")
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
