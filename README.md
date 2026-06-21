# jama-offline

A Copilot CLI skill for **fast offline search of Jama Connect** — download a
project to a local SQLite cache, then query it in milliseconds with keyword
(FTS5/BM25), substring, and semantic (vector) search across each item's **name,
description, and test-case steps**. Pure Python standard library for the core;
only vector search needs `fastembed` + `sqlite-vec` (auto-installed on first use).

## Features

- 🔎 **Hybrid search** — FTS5/BM25 + substring (LIKE) + semantic (vector),
  fused by Reciprocal Rank Fusion (RRF) and de-duplicated. All three legs cover
  an item's **name, description, and test-case steps**.
- 🧮 **Boolean expressions** — `--expr "(dock or cradle) and (charge or power) and not legacy"`
  (AND/OR/NOT + parentheses; full-width `（）` and symbol operators accepted) drives
  the keyword + substring legs precisely.
- 📅 **Date-range filters** — `--created-after/-before` and `--modified-after/-before`
  (inclusive) on `search` and `semantic`.
- 🧠 **Semantic search** — meaning-based matching (paraphrases / synonyms)
  via `BAAI/bge-base-en-v1.5` embeddings. Long text is embedded in **overlapping
  chunks (no truncation)**, so even long test cases are searchable end to end.
- 🗄️ **SQL queries** — read-only `SELECT` against the flattened cache for
  exact counts, filters, joins.
- ⚡ **Persistent cache, offline-first queries** — build once with `init`, then
  every query auto-syncs only a SMALL delta (≤200 changed items) and serves in
  milliseconds. If the cache / vector index / model is missing, or the project has
  drifted past 200 items, the query **STOPS with the exact `init` / `update` command
  to run** — it never silently kicks off a long download/build.
- 🧵 **Streaming, low-memory build** — the full data download, the incremental
  sync, **and the vector (re)build** all run **wave by wave** (fetch → process →
  commit → free each batch), so peak memory stays bounded regardless of project
  size, then the result is atomically swapped into place. Concurrent builds use
  distinct temp files and the last atomic swap wins — no clash, no torn cache.
- 🇨🇳 **China-first downloads + progress** — pip deps and the embedding model
  are fetched from a China mirror chosen by a **live speed test** (Tsinghua /
  Aliyun / Tencent · `hf-mirror.com`), falling back to the international source and
  aborting if neither is fast enough. Every long download/build streams **periodic
  progress** to stderr. (Jama API traffic is never rerouted.)
- 🌐 **Cross-platform** — Windows / Linux / macOS, Python 3.8+, standard
  library only for the core.

## Quick start

```bash
# 1. Save your Jama API credentials once (validated by fetching a token)
python jama_offline.py login --base https://example.jamacloud.com \
    --client-id <ID> --client-secret <SECRET>

# 2. Find the project id by name
python jama_offline.py projects --project projecta

# 3. Build the local cache ONCE (full download + vector index; ~30-50 min one-time)
python jama_offline.py init --project 12345

# 4. Hybrid search (keyword + substring + semantic)
python jama_offline.py search --project 12345 --keyword docking --type REQ

# 4b. Boolean expression + date range
python jama_offline.py search --project 12345 \
    --expr "(dock or cradle) and not legacy" --created-after 2026-01-01

# 5. Semantic search (meaning-based)
python jama_offline.py semantic --project 12345 --query "headset won't charge"

# 6. SQL query for exact stats
python jama_offline.py query --project 12345 \
    --sql "SELECT typeKey, COUNT(*) c FROM items GROUP BY typeKey"

# 7. Later, if a query says the cache drifted: incrementally update it
python jama_offline.py update --project 12345
```

## Commands

| Command | What it does |
|---------|--------------|
| `login` | Save credentials once to a user-level file. |
| `projects` | List matching projects → get the id. |
| `init` | First-time full build (data + vector index + model). Run once per project. |
| `update` | Incrementally update an existing cache + its vectors (streamed, low-memory, no size limit). |
| `search` | Hybrid search (FTS + LIKE + vector), RRF-fused. Offline-first (stops if not built / drifted >200). |
| `semantic` | Pure vector (meaning-based) search. Offline-first. |
| `query` | Read-only SQL for counts/filters/joins. Offline-first. |
| `status` | Show what's cached: state, last-sync, size, vector-index state. |
| `sync` | Build-if-missing else incremental (init + update in one). |
| `rebuild` | Force a clean full re-download (drops deletions). (alias: `refresh`) |
| `purge` | Delete cache file(s). |

`search` / `semantic` / `query` are **offline-first**: they never silently start a long
build. If the cache / vector index / model is missing, or the project has drifted by more
than 200 items, they STOP with the exact `init` / `update` command to run; a small delta is
auto-synced. Add `--offline` to skip the network and read the existing cache as-is (errors
if a needed file is missing), or `--force` to rebuild on the spot.

## Files

- `jama_offline.py` — the single script (all commands).
- `SKILL.md` — detailed usage docs for the Copilot CLI skill.

## Security

- Credentials are saved to a **user-level** `credentials.json` (outside this
  folder, non-roaming on Windows) and never committed.
- Caches and the embedding model are machine-local and excluded by
  `.gitignore`.
- **Never share your API secrets.** Use `logout` to clear saved credentials.

## Requirements

- Python 3.8+
- Core (`projects` / `query` / `status`): standard library only.
- Vector search (`search` / `semantic`): `fastembed` + `sqlite-vec`
  (auto-installed on the first build, China-mirror-first). `init` does a one-time
  ~210 MB model download (China-mirror-first, speed-tested) and builds the chunked
  index **streamed in low-memory waves** (~30–45 min for a 10k-item project on CPU);
  afterwards only changed items are re-embedded (seconds). Tune the mirror speed
  cutoff with `JAMA_MIN_KBPS`, and the query auto-sync limit with `JAMA_DELTA_LIMIT`
  (default 200).

## License

MIT — see [LICENSE](LICENSE).
