---
name: jama-offline
description: >-
  Fast OFFLINE search of Jama Connect (https://example.jamacloud.com). Downloads one or a FEW
  specific projects into local SQLite caches (one .db per project, persistent + auto incremental sync of
  changed items each use), then searches them
  in milliseconds — full-text (FTS5/BM25, with stemming) and substring (LIKE) over an item's name,
  description AND test-case steps, plus status, custom field, and ad-hoc SQL. USE THIS WHEN: the user names a specific project (e.g. "ProjectA") and wants to find / list
  / count / filter / cross-reference its requirements, features, test cases, or any items — especially
  repeated queries. DO NOT USE FOR: a one-off always-live lookup, browsing the requirement tree, or
  traceability across the whole instance — use the sibling `jama-query` skill. Cross-platform
  (Windows / Linux / macOS), pure Python standard library, no third-party packages. Triggers on "Jama",
  "jamacloud", project names in this instance (ProjectA, ...), and requests to search/cache/download Jama
  requirements offline.
---

# Jama Offline

Download Jama project(s) → local SQLite → query offline in milliseconds, with **full-text search**. Pure
Python (standard library only), runs on Windows / Linux / macOS. This file tells you EXACTLY what to run —
follow the steps in order; when in doubt, copy a template and change only the project and the query.

One script: **`jama_offline.py`** (commands: `projects, sync, rebuild, status, search, query, purge`).

---

## ⚡ TL;DR — the commands you need 99% of the time

```bash
# Replace SKILL_DIR with this skill's folder (the harness gives it as the "Base directory"). Use python3 on Linux/mac.
python "SKILL_DIR/jama_offline.py" login --base https://example.jamacloud.com --client-id <ID> --client-secret <SECRET>  # once
python "SKILL_DIR/jama_offline.py" projects --project projecta
python "SKILL_DIR/jama_offline.py" search   --project 12345 --keyword docking --type REQ
python "SKILL_DIR/jama_offline.py" semantic --project 12345 --query "headset won't charge on the cradle"
python "SKILL_DIR/jama_offline.py" query    --project 12345 --sql "SELECT typeKey, COUNT(*) c FROM items GROUP BY typeKey"
```
- **`login` once** — credentials are saved to a user-level file and reused automatically (no re-login). If
  creds are already set (env or a prior login), skip it.
- You normally do NOT run `sync` yourself — `search`/`query` download on first use then incrementally sync
  changed items before every run (seconds). `search` is HYBRID (keyword + substring + semantic, fused).
- **First `search`/`semantic`** auto-installs vector deps (`fastembed`/`sqlite-vec`) + builds the index
  (~30-35 min one-time, progress bar). `semantic` = the pure-vector variant. `projects`/`query` need no extras.
  Deps + the model download **China-mirror-first with a live speed test + progress** (see "Downloads" below).

---

## 🚦 Decision flow (do this every time)

> 💡 **STEP 0 — pick the tool that fits the goal (a guideline, not a hard ban):**
> - **Exact statistics** — counts/breakdowns by `typeKey`/status/date, "list ALL satisfying <exact
>   condition>", group-by, %, average/min/max, a number for a report → **prefer `query` (SQL)**: exact,
>   reproducible, auditable. A fused `search`/`semantic` hit-count is an incidental union that shifts with
>   the threshold/wording — fine as a rough gauge, but SQL is the reliable tool for a precise total.
> - **Semantic statistics** — "how many items are *about* X (by meaning)" → `semantic`/`search` is the way
>   (SQL can't judge meaning); just note the count depends on `--max-distance`, so report it as approximate.
> - **Finding / exploring** — "what's there about X", unsure of the term → `search` (hybrid) or `semantic`.

1. **Find the project id.** Take the project NAME from the user's request and run
   `jama_offline.py projects --project <name>`. It prints lines like `12345   PA   ProjectA`. First number = **id**.
   - **DON'T guess a project id.** Always resolve it with `projects` first.
   - **STOP and ask the user** if `projects` returns **0 matches**, or **more than 5**, or you genuinely
     **can't tell which project** they mean. Show candidates; never silently pick one.
2. **Search.** `search --project <id> --keyword <words>` (or `--query "natural language"`). This is a
   **HYBRID** search — keyword (FTS5/BM25) **+** substring (LIKE) **+** semantic (vector), fused by RRF and
   de-duped, so one query returns the most complete set. The `via` column shows which legs matched; `score`
   is the fused rank. Add `--type REQ` (or FEAT/TC…) to limit kinds. (First use auto-installs the vector
   deps + builds the index — one-time, see the semantic section.)
3. **Stats / aggregation → usually `query` (SQL)** for exact, reproducible numbers (`COUNT` / `GROUP BY` /
   exact `WHERE`). For *semantic* counts ("how many are about X"), `semantic` works too — its count is
   `--max-distance`-dependent, so report it as approximate. (See "Finding data vs counting data".)
4. **Report** the `documentKey` + `name` (+ sequence) rows back to the user.

Data auto-updates: each search/query first syncs items changed since last time. To also drop server-side
deletions: "清掉已删项" → `sync --project <id> --prune-deleted` (lightweight, keeps the cache); "重建 /
完全刷新" → `rebuild --project <id>` (full clean re-download).

---

## Finding data vs counting data — pick the tool that fits (no hard ban)

`search`/`semantic` (FTS + LIKE + vector) are **discovery / semantic** tools; `query` (SQL) is the **exact
measurement** tool. Either is allowed for any goal — just know the trade-off:

- **Exact numbers** (counts, group-by, %, exact-condition sets) → **`query` (SQL)** is the reliable choice:
  exact, reproducible, auditable. A fused hit-count is an incidental UNION (FTS ∪ LIKE ∪ vector) that moves
  with `--max-distance` / wording, so don't present it as a precise population total. E.g.
  `query --sql "SELECT typeKey, COUNT(*) c FROM items WHERE name LIKE '%dock%' GROUP BY typeKey"`.
- **Semantic "how many about X"** → `semantic`/`search` is the only way (SQL can't judge meaning). Report
  the count as **approximate** and state the `--max-distance` used; tighten/loosen it to see how it shifts.
- **Combine both:** `search`/`semantic` to discover the relevant `typeKey`s / terms / subtrees → then
  `query` SQL for the precise, reproducible number.
- ⚠️ Before counting, pick a dedup rule: this instance has **duplicate-named items** (one requirement →
  many `TSTRN` runs), so `GROUP BY name` or filter `typeKey='REQ'` to avoid double-counting.

---

## How to run (paths & interpreter)

- Run with Python 3.8+: `python` (Windows) or `python3` (Linux/macOS). Form:
  `python "SKILL_DIR/jama_offline.py" <command> [options]`
- **`SKILL_DIR`** = the folder THIS SKILL.md lives in; the harness gives it as the skill's
  "Base directory". Use THAT exact path, verbatim. It differs on every machine/user/OS (drive, username,
  home dir, `/` vs `\`), so **never hard-code a path or copy one from an example or another machine.**
- Do **not** write the literal text `SKILL_DIR` — substitute the real Base directory.
- **Core** (projects/search/query/sync/status) = Python **standard library only**, no pip. **Only**
  `semantic` (vector search) needs extras: `pip install fastembed sqlite-vec` + a one-time model download.

---

## Downloads: China-first mirrors + progress (auto, no flags)

Every **non-Jama** download (the `fastembed`/`sqlite-vec` pip deps and the ~210 MB embedding model) is
fetched **China-mirror-first**, decided by a quick **live speed test** before downloading:

- pip deps → tries Tsinghua / Aliyun / Tencent (then falls back to `pypi.org`); the embedding model →
  `hf-mirror.com` (then `huggingface.co`). The **first source ≥ the speed threshold wins**.
- If every China mirror is too slow it switches to the international source; **if that is also too
  slow/unreachable it ABORTS** with a `网络异常 / Network error` message instead of hanging.
- Tune the cutoff with **`JAMA_MIN_KBPS`** (default 150 KB/s). A user-set `PIP_INDEX_URL` / `HF_ENDPOINT`
  is respected as-is. **Jama API traffic is never rerouted.**
- **Progress feedback** streams to **stderr** for every long step — pip install, model download (MB / %),
  the full + incremental cache downloads, and vector (re)embedding — so a one-time build is never silent.

(These are automatic; you don't pass any flag. If a run prints `网络异常`, the network/mirrors are the issue.)

---

## Commands (quick reference)

| Command | What it does |
|---------|--------------|
| `login --base <url> --client-id <id> --client-secret <s>` | Save credentials once to a user-level file (validated by fetching a token). `logout` removes them. |
| `projects --project <name/regex>` | List matching projects → get the **id**. Step 1, always. |
| `search --project <id> --keyword a,b` | **Main tool. HYBRID** = FTS + LIKE + semantic, RRF-fused & de-duped. Auto-syncs + auto-builds vectors. Supports `--expr "(a or b) and not c"` (boolean) and `--created-/--modified-after/-before` date filters. |
| `semantic --project <id> --query "…"` | **Meaning-based** (vector) search; finds paraphrases/synonyms. Needs vector extras (see below). |
| `query --project <id> --sql "SELECT ..."` | Read-only SQL for counts/filters/joins. Auto-syncs. |
| `status` | Show what's cached: state, last-sync, watermark, size, vector-index state. |
| `rebuild --project <id>` | Force a clean FULL re-download (also drops deleted items). (aliases: `refresh`, `update`) |
| `sync --project <id>` | Build/incrementally-update a cache (+ its vector index). `--no-vectors` to skip vectors. `--prune-deleted` to also remove server-deleted items. |
| `purge --project <id>` / `--all` | Delete cache file(s) (incl. the vector index). |

Every `search`/`semantic`/`query` ALWAYS checks the cache first and syncs the latest changes before
returning (with a download progress bar if a full download is needed) — results are never stale. Add
**`--offline`** to skip the sync and use the existing cache as-is (no network/credentials; for no-signal
or air-gapped use). It errors if there's no cache yet (that first build needs the network).

Options use `--flag value`. Lists are comma-separated: `--keyword dock,cradle`, `--type REQ,FEAT`.

---

## `search` — templates to copy

`search` runs THREE legs and fuses them (RRF) into one de-duped list — you don't pick a mode. The FUSED
output is **uncapped by default** (`--top N` to limit), but each leg feeds a bounded candidate set in:
All three legs search an item's **name + description + test-case steps** (the authored `testCaseSteps`,
i.e. action / expectedResult / notes).
- **keyword (FTS5/BM25)** — ranked, stemmed (`dock` matches `docking`/`docked`). **Top ~200 by relevance**
  (a multi-word OR can BM25-match 40–70% of the corpus; the tail is noise). To enumerate EVERY item
  containing a term, that's a counting/exact-set job → use **`query`** (`WHERE name LIKE '%x%'`), not search.
- **substring (LIKE)** — partial words / codes / fragments stemming misses (`undock`, `$89009`). **Up to ~200**, in document order.
- **semantic (vector)** — meaning/paraphrase matches sharing no literal words. The index is **chunked**:
  each item's full text (name + description + steps) is embedded in overlapping windows (no truncation),
  so a long test case is searchable end to end; chunk hits fold back to one row per item. Only items with
  **cosine similarity >= 0.70** (distance <= 0.30) by default — below that they're dropped.

Rules:
- `--keyword a,b` (comma = OR; `--match all` = AND) and/or `--query "natural language"` (drives the vector
  leg). At least one is required; give `--query` for fuzzy intent, `--keyword` for specific terms.
- **Boolean expressions — `--expr "…"`** for AND/OR/NOT + parentheses, e.g.
  `--expr "(dock or cradle) and (charge or power) and not legacy"`. Operators are case-insensitive words
  (`and`/`or`/`not`) or symbols (`&`/`|`/`!`); full-width `（）` are accepted; adjacent terms = AND; quote a
  phrase (`"answer call"`). The expression drives the **keyword (FTS) + substring (LIKE)** legs exactly; the
  vector leg still runs on `--query` (or the expression's positive terms) for meaning-based recall. (A
  pure/top-level `not …` can't be expressed in FTS, so only the LIKE+vector legs run for it.)
- **Date-range filters** (inclusive): `--created-after`, `--created-before`, `--modified-after`,
  `--modified-before`, value `YYYY-MM-DD` (or a full ISO timestamp). A bare date upper bound includes the
  whole day. Applies to ALL legs. For an EXACT date-bounded count, prefer `query` SQL on `createdDate`/`modifiedDate`.
- Quote a keyword with a space: `--keyword "answer call"`. `--field name` restricts FTS/LIKE to the name.
- `--type REQ[,FEAT]` limits kinds. `--top N` caps rows (default 0 = all). `--max-distance D` tunes the
  vector threshold (default 0.30; lower = stricter, e.g. `0.20`; higher = looser). Output: `score via name`.

```bash
python "SKILL_DIR/jama_offline.py" search --project 12345 --keyword docking --type REQ
python "SKILL_DIR/jama_offline.py" search --project 12345 --query "headset won't charge on the cradle"
python "SKILL_DIR/jama_offline.py" search --project 12345 --expr "(dock or cradle) and not legacy" --type REQ
python "SKILL_DIR/jama_offline.py" search --project 12345 --keyword battery --created-after 2026-01-01 --created-before 2026-06-30
python "SKILL_DIR/jama_offline.py" search --project 12345 --keyword mute --max-distance 0.25 --top 50
```
Output columns: `id  documentKey  typeKey  sequence  name`. Report `documentKey` + `name` to the user.

### Common item types for `--type`
| typeKey | meaning | | typeKey | meaning |
|---|---|---|---|---|
| `REQ` | requirement | | `TSTRN` | test run |
| `FEAT` | feature | | `FLD` | folder |
| `TC` | test case | | `ATT` | attachment |

Not sure which types exist? `query --project <id> --sql "SELECT typeKey, typeName, COUNT(*) c FROM items GROUP BY typeKey ORDER BY c DESC"`

---

## `query` — read-only SQL (when search isn't enough)

Only `SELECT`/`WITH` allowed. Tables: **`items`** (main), **`fields_kv`** (every other field),
**`picklist`** (id→label), **`fts`** (FTS5; `SELECT rowid FROM fts WHERE fts MATCH 'foo'`, rowid = item id).
Key `items` columns: `id, documentKey, typeKey, typeName, name, description, status, statusName, priority,
priorityName, sequence, parentItem, createdDate, modifiedDate`.

> ⚠️ **`status`/`priority` hold numeric picklist ids** (e.g. `156410`), so `WHERE status='Approved'`
> finds NOTHING. Filter on the resolved text column: `WHERE statusName='Approved'` (or `LIKE '%approv%'`).

```bash
python "SKILL_DIR/jama_offline.py" query --project 12345 --sql "SELECT typeKey, COUNT(*) c FROM items GROUP BY typeKey ORDER BY c DESC"
python "SKILL_DIR/jama_offline.py" query --project 12345 --sql "SELECT documentKey, statusName, name FROM items WHERE typeKey='REQ' AND statusName LIKE '%approved%'"
python "SKILL_DIR/jama_offline.py" query --project 12345 --sql "SELECT key, value FROM fields_kv WHERE itemId=(SELECT id FROM items WHERE documentKey='PRJ-REQ-1234')"
```

---

## `semantic` — meaning-based (vector) search

Finds items by **meaning**, not literal words — catches paraphrases/synonyms that FTS misses (e.g.
"won't charge on the cradle" → docking-power requirements that never say "charge").

```bash
python "SKILL_DIR/jama_offline.py" semantic --project 12345 --query "headset won't charge on the cradle"
python "SKILL_DIR/jama_offline.py" semantic --project 12345 --query "noise cancellation on calls" --type REQ
python "SKILL_DIR/jama_offline.py" semantic --project 12345 --query "battery life" --max-distance 0.45  # looser
python "SKILL_DIR/jama_offline.py" semantic --project 12345 --query "battery life" --modified-after 2026-01-01
```
`semantic` also accepts the same `--created-after/-before` and `--modified-after/-before` date filters as `search`.
Output columns: `id  documentKey  typeKey  sequence  score  name` (score = cosine similarity 0–1).
Returns **every** item with **cosine similarity >= 0.70** (distance <= 0.30) — items below the threshold
are dropped, and there's no count cap (`--top N` to limit). Tune the cutoff with **`--max-distance D`**
(default 0.30; `0.20` stricter, `0.45` looser). `semantic` is the pure-vector variant; `search` is hybrid.

- **Vectors are required & auto-installed:** if `fastembed`/`sqlite-vec` are missing, the first run
  **auto-runs `pip install fastembed sqlite-vec`** (one-time) — no degraded fallback.
- **One-time index build:** the first `search`/`semantic`/`sync`/`rebuild` splits every item's
  name+description+steps into overlapping chunks and embeds each with `BAAI/bge-base-en-v1.5` (768-d) →
  a separate `jama-proj-<id>.vec.db`. This is the slow part: **downloads
  a ~210 MB model once** (China-mirror-first, speed-tested — see "Downloads"), **then ~30–35 min for a
  10k-item project on CPU** (a progress bar with %/ETA shows on stderr). Uses ~80% of CPU threads. After
  that, incremental syncs only re-embed *changed* items (seconds), and each query is milliseconds.

---

## Caching: persistent + auto incremental sync

- Caches live under a per-user dir (override with `JAMA_OFFLINE_DIR`):
  Windows `%LOCALAPPDATA%\jama-offline` · macOS `~/Library/Application Support/jama-offline` ·
  Linux `$XDG_DATA_HOME/jama-offline` or `~/.local/share/jama-offline`. Per project: `jama-proj-<id>.db`
  (main) + optional `jama-proj-<id>.vec.db` (vectors). Saved credentials: `credentials.json` in the same
  dir; the embedding model: `models/`.
- **No expiry, never auto-deleted.** A cache is built once (full download), then **every `search` /
  `semantic` / `query` ALWAYS checks the cache first and pulls just the items changed since last time**
  (by `modifiedDate`) before returning — a tiny delta (~1–3 s when little changed, vs ~60 s full). Forced
  by default, so results are never stale; pass **`--offline`** to skip it and read the existing cache with
  no network/credentials (errors if no cache exists yet).
- **Catches new + changed items. Does NOT catch server-side DELETIONS by default.** Two ways to drop
  deleted items: **`sync --project <id> --prune-deleted`** (lightweight, opt-in — keeps the cache, just
  removes items deleted on the server from both the cache AND the vector index; cheap because it first
  compares a single server item-count and only sweeps ids when a deletion is detected), or
  `rebuild --project <id>` (full clean re-download). `status` shows each cache's last-sync + watermark.
- For a guaranteed-live single lookup, use the **jama-query** skill.

---

## Running on another machine / OS (portability)

Copy the whole `jama-offline` folder. Then:
1. **Python 3.8+** on Windows / Linux / macOS. `projects`/`query`/`status` use only the standard library;
   `search`/`semantic` need `fastembed` + `sqlite-vec`, which the tool **auto-installs on first use**
   (one-time pip). First vector build also downloads a ~200 MB model.
2. **Credentials** — needed to download/refresh (every query syncs first, so they're effectively required).
   Easiest:
   **`jama_offline.py login --base <url> --client-id <id> --client-secret <secret>`** once → saved to a
   user-level `credentials.json` (independent of this folder; non-roaming on Windows) and reused forever;
   `logout` clears it. Lookup order: (a) env `JAMA_BASE`/`JAMA_CLIENT_ID`/`JAMA_CLIENT_SECRET`,
   (b) user-level `credentials.json`, (c) `config.local.json` next to the script, (d) `config.local.ps1`
   (this folder or sibling **jama-query**). Else `Missing credentials`. **Never commit/share secrets.**
3. Caches + saved credentials + the embedding model are machine-local (not portable); each machine logs in
   and rebuilds on first use.

---

## If you hit an error

| Message contains… | Meaning | Do this |
|-------------------|---------|---------|
| `resolves to N projects (max 5)` | Name matched too many | STOP. Show candidates, ask user to pick/narrow. |
| `No project name matches /…/` | Name matched nothing | Try a different word, or ask for the exact name. |
| `needs exactly ONE project` | search/query got >1 project | Re-run with a single id. |
| `Only read-only SELECT…allowed` | Non-SELECT SQL passed to `query` | Rewrite as `SELECT`/`WITH`. |
| `fts5: syntax error` (internal) | Odd characters in a keyword | The FTS leg is auto-skipped; LIKE+vector still run. Simplify keywords if needed. |
| `Missing credentials` | No Jama API keys found | Run `login` once (see Portability), or set env vars. |
| `Could not auto-install vector deps` | pip blocked/offline | Install manually: `pip install fastembed sqlite-vec`. |
| `网络异常 / Network error … no mirror … fast enough` | Both the China mirror AND the international source were too slow/unreachable for a pip-deps or model download | Check the connection; retry; or lower the bar with `JAMA_MIN_KBPS` (e.g. `50`), or set `PIP_INDEX_URL` / `HF_ENDPOINT`. |
| `Invalid --created-after/…` | Bad date value | Use `YYYY-MM-DD` (or a full ISO timestamp). |
| `Could not parse --expr` | Malformed boolean expression | Balance the parens; use `and`/`or`/`not`; quote phrases with spaces. |
| `Login FAILED` | Wrong base/id/secret (token call 401) | Re-check the three values; nothing is saved on failure. |

First `search`/`semantic` on a NEW project also builds the vector index (~30–35 min, progress bar) + a
one-time ~200 MB model download + full data download. Expected, not a hang. After that each
run does only a small incremental sync (seconds) before returning.

---

## Advanced: schema internals (only if writing complex SQL)

Cache (`jama-proj-<id>.db`) — SQLite, every field flattened (no JSON blob):
- **`items`** — one row/item; common fields promoted to columns (above). `description` = HTML-stripped
  plain text. `stepsText` = HTML-stripped plain text of a test case's authored steps (`testCaseSteps` →
  action / expectedResult / notes; per-run `testRunSteps` are NOT included). `statusName`/`priorityName`
  = labels resolved from the numeric `status`/`priority` ids.
- **`fields_kv(itemId, key, value)`** — EVERY `fields.*` entry as text, incl. custom per-type keys like
  `verifying_teams_new$89009`, `testRunSteps`. (Raw HTML of `description` is not duplicated here.)
- **`fts`** — FTS5 external-content index over `items.name` + `items.description` + `items.stepsText`
  (rowid = item id), used by `search`. Query: `SELECT rowid FROM fts WHERE fts MATCH 'docking' ORDER BY bm25(fts)`.
- **`picklist(id, name, pickList)`** — id→label map (status/priority and any other option ids).
- **`relationships(...)`** — only when synced with `--with-links` (traceability is OFF by default).
- **`meta(key, value)`** — `last_sync_at`, `watermark` (= newest `modifiedDate` captured; the incremental
  sync's high-water mark), `fetched_at`, counts, `schema_version`, versions.

Vector index (`jama-proj-<id>.vec.db`, only if `semantic` was used) — a SEPARATE SQLite file so the main
cache stays pure-stdlib-openable. **Chunked**: one item → one-or-more chunks. Needs the `sqlite-vec`
extension to read:
- **`vec`** — `vec0(chunk_id INTEGER PRIMARY KEY, embedding float[768] distance_metric=cosine)`;
  `SELECT chunk_id, distance FROM vec WHERE embedding MATCH :qvec ORDER BY distance LIMIT k`.
- **`chunk_map(chunk_id, item_id)`** — folds a chunk hit back to its owning item (join on `chunk_id`,
  then keep the nearest chunk per item).
- **`vmeta(key, value)`** — `embed_model`, `dim`, `vec_count` (chunks), `item_count`, `built_at`,
  `vec_watermark` (newest `modifiedDate` whose vectors are current; each `search`/`semantic` re-embeds
  items changed past it, so the index never lags — even items a vectors-less `query` synced). Readable
  without the extension.

List a project's custom fields: `query --project <id> --sql "SELECT DISTINCT key FROM fields_kv ORDER BY key"`.

## jama-offline vs jama-query
- **jama-offline (this):** named project(s); repeated/complex queries; keyword (FTS/substring) **and semantic (vector)** search; status/custom-field/SQL; persistent cache auto-synced each use (deletions need `rebuild`); cross-platform Python.
- **jama-query (sibling):** single always-live lookup, or online navigation (`tree`, `children`, `trace`) across the instance.
