# LBIS Portfolio Engine

Python backend service that processes stock directives from FeedSense AI,
runs scoring via Perplexity `sonar-pro`, computes capital allocation, and
manages a full portfolio lifecycle against a Supabase (Postgres) database.

---

## Setup
 
### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env and fill in:
#   SUPABASE_URL
#   SUPABASE_SERVICE_KEY
#   PERPLEXITY_API_KEY
```

### 3. Start the engine
```bash
python main.py
```

---

## Running

| Command | What it does |
|---|---|
| `python main.py` | Start the engine (polls + scheduled rebalance) |
| `python main.py --rebalance` | One-shot manual rebalance, then exit |

---

## Architecture

```
main.py                     Entry point, starts scheduler + poll loop
config.py                   All constants (capital splits, tier %, cycles)

db/
  client.py                 All Supabase queries (single shared client)
  errors.py                 Persistent error tracking (engine_errors table)
  migrations/               SQL migration files

engine/
  processor.py              Polls for unprocessed directives, routes them
  handlers.py               One handler per directive type (12 total)
  scoring.py                Perplexity API call + Section 7 parser
  allocation.py             3-criterion ranking + capital allocation

scheduler/
  jobs.py                   APScheduler: 15-day rebalance + 60s poll

utils/
  retry.py                  Exponential backoff decorator (used by scoring)
```

## Directive Flow

```
FeedSense inserts row into stock_directives (processed_at = NULL)
        ↓
Engine polls every 60s
        ↓
BUY / BUY-IN-BUY  →  queued until rebalance day
All others         →  executed immediately
        ↓
On rebalance day:
  1. Process queued BUYs (score → allocate → buy)
  2. Re-score all existing units
  3. Reallocate capital across all units
  4. Generate ADJ BUY / ADJ SELL for resizing
  5. Update portfolio_state
```

## Key Rules (from spec)

- **98/2 split**: 98% investable, 2% buffer
- **Sell ladder**: L1→L4, each 25% of original holding, levels can be skipped
- **Partial sell**: independent of ladder, recovers initial investment, sets status=free
- **Bottom fishing**: 10% of L1–L4 proceeds, max 10 times, only after 100% sold
- **Scoring**: 15 parameters × 100 = max 1500. Green count max 9.
- **Allocation**: top 10% (min 22) gets 70%, rest gets 30%, proportional by raw value
- **Rebalance**: every 15 days. BUY directives queue. Everything else is immediate.
