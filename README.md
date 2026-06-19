# jama-offline

A Copilot CLI skill for **fast offline search of Jama Connect** — download a
project to a local SQLite cache, then query it in milliseconds with keyword
(FTS5/BM25), substring, and semantic (vector) search. Pure Python standard
library for the core; only vector search needs `fastembed` + `sqlite-vec`
(auto-installed on first use).

## Features

- 🔎 **Hybrid search** — FTS5/BM25 + substring (LIKE) + semantic (vector),
  fused by Reciprocal Rank Fusion (RRF) and de-duplicated.
- 🧠 **Semantic search** — meaning-based matching (paraphrases / synonyms)
  via `BAAI/bge-base-en-v1.5` embeddings.
- 🗄️ **SQL queries** — read-only `SELECT` against the flattened cache for
  exact counts, filters, joins.
- ⚡ **Persistent + auto-syncing cache** — full download once, then every
  query pulls only items changed since last run (seconds).
- 🌐 **Cross-platform** — Windows / Linux / macOS, Python 3.8+, standard
  library only for the core.

## Quick start

```bash
# 1. Save your Jama API credentials once (validated by fetching a token)
python jama_offline.py login --base https://example.jamacloud.com \
    --client-id <ID> --client-secret <SECRET>

# 2. Find the project id by name
python jama_offline.py projects --project projecta

# 3. Hybrid search (keyword + substring + semantic)
python jama_offline.py search --project 12345 --keyword docking --type REQ

# 4. Semantic search (meaning-based)
python jama_offline.py semantic --project 12345 --query "headset won't charge"

# 5. SQL query for exact stats
python jama_offline.py query --project 12345 \
    --sql "SELECT typeKey, COUNT(*) c FROM items GROUP BY typeKey"
```

## Commands

| Command | What it does |
|---------|--------------|
| `login` | Save credentials once to a user-level file. |
| `projects` | List matching projects → get the id. |
| `search` | Hybrid search (FTS + LIKE + vector), RRF-fused. |
| `semantic` | Pure vector (meaning-based) search. |
| `query` | Read-only SQL for counts/filters/joins. |
| `status` | Show what's cached: state, last-sync, size. |
| `sync` | Build / incrementally update a cache. |
| `rebuild` | Force a clean full re-download (drops deletions). |
| `purge` | Delete cache file(s). |

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
  (auto-installed on first use; one-time ~200 MB model download).

## License

MIT — see [LICENSE](LICENSE).
