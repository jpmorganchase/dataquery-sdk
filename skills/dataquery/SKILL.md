---
name: dataquery
description: >-
  Query JP Morgan DataQuery API v2 for financial market data. Use when the user mentions
  DataQuery, DQ, or wants to fetch datasets, instruments, time-series, grid data, bond yields,
  index returns, CSV exports, or CUSIPs/ISINs. Also use
  for computed analytics: moving averages, volatility, correlation, beta, regression,
  z-scores, spreads, rolling statistics, or RSI on financial time-series.
  Also use for the File Delivery API: listing published files, listing which files were
  published across a date range, checking file availability, downloading individual files,
  bulk date-range downloads, and live SSE file-watch.
  Trigger phrases: "dataquery", "DQ", "pull time-series", "fetch yields", "get bond data",
  "search datasets", "list instruments", "export to csv", "grid data",
  "heartbeat", "moving average", "volatility", "correlation", "beta", "z-score",
  "regression", "spread", "RSI", "function help", "list functions", "treasury rate",
  "swap rate", "download file", "download catalog", "bulk download",
  "file availability", "available files", "file delivery", "which files were published",
  "backfill files", "watch files", "list files", "subscribe to files".
disable-model-invocation: false
---

# JP Morgan DataQuery API v2

## Local Setup (One-Time, Done by the User Before Using This Skill)

This skill runs the `dataquery` CLI from the `dataquery-sdk` Python package on the user's machine. The user must install and configure the SDK locally before the skill can run any command. When preflight shows that setup is missing, point the user to these steps and wait for them to finish. Never ask the user to paste a client ID, client secret, or token into the chat.

### Step 1: Install uv

[uv](https://docs.astral.sh/uv/) installs the SDK and ships its own Python, so no separate Python install is needed.

macOS / Linux:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows (PowerShell):
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Windows only: group policy may block executables under AppData. Point uv's cache and tool directories at your user profile instead, then open a new terminal:
```powershell
setx UV_CACHE_DIR "%USERPROFILE%\.uv\cache"
setx UV_TOOL_DIR "%USERPROFILE%\.uv\tools"
```

### Step 2: Install the DataQuery SDK CLI

```bash
uv tool install dataquery-sdk
```
This puts the `dataquery` executable on PATH. If the command is not found afterwards, run `uv tool update-shell` and open a new terminal. To upgrade later, run `uv tool upgrade dataquery-sdk`. On a corporate network that blocks PyPI, add `--index-url <internal registry URL>`.

### Step 3: Configure credentials

The CLI authenticates with OAuth client credentials issued for DataQuery. To request credentials, contact `DataQuery_Sales@jpmorgan.com`.

Store them in the user-level config file `~/.dataquery/.env` (on Windows, `%USERPROFILE%\.dataquery\.env`). The CLI loads this file from any working directory:
```bash
mkdir -p ~/.dataquery
cat > ~/.dataquery/.env <<'ENV'
DATAQUERY_CLIENT_ID=<your client id>
DATAQUERY_CLIENT_SECRET=<your client secret>
ENV
chmod 600 ~/.dataquery/.env
```

Alternatives:
- For a fully commented template with every setting (proxy, timeouts, download directory), run `dataquery config template --output ~/.dataquery/.env`, then fill in the two values above.
- With a pre-issued bearer token instead of OAuth, set `DATAQUERY_OAUTH_ENABLED=false` and `DATAQUERY_BEARER_TOKEN=<token>`.
- Shell environment variables with the same names take precedence over the file. So does a file passed with `dataquery --env-file <path> <command>`.

Production endpoints are built in, so no URL settings are needed.

### Step 4: Verify

```bash
dataquery config validate   # prints "Configuration valid"
dataquery heartbeat         # prints "DataQuery is UP"
```
Once both pass, the skill is ready to use. Every command in this skill is invoked as `dataquery <command> [args]`.

## Quick Start

This skill provides natural-language access to DataQuery for traders and quantitative analysts. Once the user completes Local Setup, the `dataquery` CLI handles authentication, token refresh, and all API endpoints.

For best results, include specific group IDs (e.g., `FI_GO_NOTE_BOND`), instrument IDs, or DataQuery expressions in the query. Specific queries return faster and more accurate results.

## Preflight Checks (Run Once Per Session, Before Any Other Command)

Verify the environment is ready before running any DataQuery command, including `search`. Run this preflight once at the start of a session, and re-run it only if a later command surfaces an environment-related error (authentication failure, command-not-found, and similar). Do not re-run the preflight before every command.

Run the five checks below in order. If any check fails, resolve it before proceeding. For failures that need the user (installing software or entering credentials), send them to the matching step in Local Setup and stop until they confirm it is done.

### Check 1: Python and uv installed
```bash
uv --version
python --version
```
- If both print versions, continue.
- If `uv: command not found`, direct the user to Local Setup Step 1 and stop.
- If `python: command not found`, note that uv ships its own Python. Run `uv python install` and retry.

### Check 2: uv directories (Windows only)
Skip this check on macOS and Linux. On Windows, group policy may block executables under AppData, so `UV_CACHE_DIR` and `UV_TOOL_DIR` should point outside AppData (for example, under the user's home directory).

```bash
echo "UV_CACHE_DIR=$UV_CACHE_DIR"
echo "UV_TOOL_DIR=$UV_TOOL_DIR"
```
- If both resolve to non-empty paths, continue.
- If either is unset, export them and persist via the shell profile:
  ```bash
  export UV_CACHE_DIR="$HOME/.uv/cache"
  export UV_TOOL_DIR="$HOME/.uv/tools"
  ```

### Check 3: `dataquery` CLI on PATH
```bash
dataquery --help | head -1
```
- If it prints `usage: dataquery ...`, continue.
- If `command not found`, install the SDK CLI:
  ```bash
  uv tool install dataquery-sdk
  ```
  If installation fails with index or network errors, direct the user to Local Setup Step 2 (they may need `--index-url` for the corporate registry).

### Check 4: Credentials configured
This checks the local configuration without calling the API.
```bash
dataquery config validate
```
- If it prints `Configuration valid`, continue.
- If it prints `Configuration invalid` (for example, `CLIENT_ID is required when OAuth is enabled`), the user has not configured credentials. Direct them to Local Setup Step 3 and stop. Do not ask for the credential values, and do not create or edit the `.env` file with credentials yourself.

### Check 5: Service heartbeat
This confirms OAuth authentication works and the API is reachable end to end.
```bash
dataquery heartbeat
```
- If the summary line reads `DataQuery is UP`, the preflight is complete. Proceed to search.
- If it reads `DataQuery is DOWN` or returns a non-zero exit code, inspect the JSON envelope:
  - `401`: Authentication token expired or invalid. Re-run the command to refresh the token; if it persists, ask the user to check the client ID and secret in `~/.dataquery/.env` (Local Setup Step 3).
  - `403`: Account lacks DataQuery entitlement. Contact `DataQuery_Sales@jpmorgan.com`.
  - `503`: DataQuery maintenance window. Retry later and do not proceed.
  - Network or DNS error: verify VPN and corporate network connectivity.

After all five checks pass, proceed to the search step below. Treat the preflight as session state. Once it passes, do not run it again unless a later command surfaces an environment-related error.

## First Step: Search for Dataset Discovery

For any user query about data, begin by calling `search` to identify the relevant datasets, group IDs, and instruments. This uses the DataQuery search API to interpret natural-language queries and return matching datasets.

```bash
dataquery search --query "<user's natural language query>"
```

API details:
- Endpoint: `POST <api_base_url>/search`, where `<api_base_url>` is the configured DataQuery API base URL.
- Authentication: OAuth, applied automatically as with all other DataQuery calls.
- Request body: `{"query": "<natural language search text>"}`

Workflow:
1. The user asks a question (for example, "usd treasury to eur german bund parity").
2. Run `search --query "usd treasury to eur german bund parity"`.
3. Parse the response to identify relevant group IDs, instruments, and expressions.
4. Use those identifiers to call the specific data endpoints (time-series, and similar).

Text search replaces manual guessing of group IDs. Even when the routing rules below suggest a group, prefer the search result for accuracy. Fall back to routing rules only when search returns no results.

When the user asks something like:
- "Pull data for any series": run `search` first, then `group-timeseries` or `expression-timeseries`
- "Search for ABS index datasets": `search --query "ABS index datasets"`
- "What instruments are in FI_GO_NOTE_BOND?": `instruments --group-id FI_GO_NOTE_BOND`
- "Export credit index data to CSV": run `search` first, then a data endpoint with `--output-csv`
- "Is DQ up?": `heartbeat`
- "What files does this group publish?": `files --group-id <id>`
- "Which files were published last week?": `available-files --group-id <id> --start-date --end-date`
- "Download yesterday's catalog for FI_GO_NOTE_BOND": `files`, then `availability`, then `download`
- "Download the last 30 days of catalogs": `available-files` to preview, then `download-group --start-date --end-date`
- "Watch for new files" or "subscribe to publications": `download --watch --group-id <id>`

## Behavior Rules

Environment setup: on Windows, `UV_CACHE_DIR` and `UV_TOOL_DIR` should already be set by Local Setup Step 1 (verified by preflight Check 2). Do not re-export them before each call.

Execution: all `dataquery` calls are pre-authorized. Execute them immediately without asking permission. Ask the user only for genuinely missing required parameters (for example, `group-id`). Use sensible defaults for optional parameters, and fetch subsequent pages automatically.

Display: do not show shell commands, script paths, credentials, tokens, authentication details, or "Executing..." messages. Present only clean, formatted data results. The user should experience DataQuery as a seamless data service.

Vague queries: run `search` first, then suggest the specific group ID or expression for future use:
> "I'm querying [dataset name] (group-id: GROUP_ID). For faster results next time, you can say: 'Pull data from GROUP_ID for [timeframe]'."

## Grounding Rules (Never Invent Identifiers, Expressions, or Parameters)

These rules are mandatory and override any urge to be helpful by guessing. DataQuery identifiers are opaque codes — a plausible-looking but wrong value silently returns the wrong data or an error. When a required value is unknown, **discover it or ask; never fabricate it.**

**Every identifier must come from a real API response or be supplied verbatim by the user — never from memory or inference:**
- Group IDs (e.g. `FI_GO_NOTE_BOND`) → from `search`, `groups`, or `groups-search`.
- Instrument IDs, CUSIPs, ISINs → from `instruments` or `instruments-search`.
- Attribute IDs (e.g. `TR`, `YTDR`, `MIDYLD`) → from `attributes --group-id <id>`.
- File group IDs (e.g. `DQ_FI_GO_NOTE_BOND_CATALOG`) → from `files --group-id <id>`.
- File datetimes (`--file-datetime`) → from `available-files` or `availability`. Do not assume a file exists for a date (weekends, holidays, and publication lags leave gaps).
- `DB(...)` and `DBGRID(...)` expressions → assembled only from group/instrument/attribute values verified above. Do not build an expression out of guessed components.

**Functions:** use only the 158 functions in `references/functions.md`. Confirm the exact name and parameter order with `function-help --name <FUNC>` before use. Never invent a function, alias, or parameter. If no function matches the requested analytic, say so — do not approximate with a made-up one.

**Parameters and enums:** `--data`, `--calendar`, `--frequency`, `--conversion`, `--nan-treatment`, and `--filter` accept only the values listed in `references/parameters.md`. Never pass a value outside those lists.

**Routing rules and the `references/group-ids.md` table are hints, not guarantees.** Treat their IDs as candidates to confirm via `search` / `instruments`, not as verified answers. IDs the user provides are trusted and may be used directly.

**If discovery returns nothing, stop.** Tell the user no matching dataset or instrument was found and ask them to refine or provide the ID. Do not backfill with a guess.

**Present only real results.** Show values, dates, and counts exactly as the API returned them. Never fabricate, extrapolate, or "fill in" data. If a call fails or returns empty, report that plainly rather than synthesizing a plausible answer, and always echo the actual identifiers/expression used so the user can verify.

## Authentication and Configuration

Authentication is automatic via OAuth once the user has completed Local Setup. Do not ask for credentials, client IDs, or secrets in the chat; if they are missing, send the user to Local Setup Step 3. Run `dataquery <command> [args]` directly; the package handles token acquisition, caching, and automatic refresh.

Output format: a summary line first, then `--- JSON ---` followed by the raw JSON. Parse the JSON block for structured data.

All settings use built-in production defaults. Credentials and any overrides are read, in order of precedence, from shell environment variables, a file passed with `--env-file`, and `~/.dataquery/.env`.

## Routing Rules for Rates and Government Bonds

Apply these routing rules when directing queries to specific panels or groups.

### Interest Rate Swaps

Emerging Markets (EM) IRS:
- Direct to the `EM_CRV_IRS` panel in nearly all cases.
- EM currencies include MXN, BRL, ZAR, TRY, PLN, CZK, HUF, RUB, CLP, COP, PEN, THB, MYR, PHP, IDR, INR, KRW, TWD, and CNY.

Developed Markets (DM) IRS:
- First attempt: query the `GFI_SWAPS_GLOBAL_CLOSES` panel.
- If that fails, fall back to local or regional panels:
  - USD: `FI_SWP_PLAIN_VANILLA_YIELDS` or `FI_SW_SF_NA`
  - EUR: `FI_SW_SF_EA`
  - GBP, CHF, NOK, SEK, DKK: `FI_SW_SF_OE`
  - JPY: `FI_SW_SF_JP`
  - AUD, NZD: `FI_SW_SF_AA`

### Swaptions

Developed Markets (DM) swaptions:
- Direct to the relevant panel under the USFI or GFI asset-class groups.
- Search for swaption-specific panels in those groups first.

Emerging Markets (EM) swaptions:
- Check the `EM_CRV_SV` panel first.

### Benchmark Rate Selection (Rates Derivatives)

General rule:
- For any rates-derivatives query, look up the current default benchmark rate for the currency's market.
- Unless the user explicitly specifies a benchmark (for example, "SOFR", "3M LIBOR", "6M EURIBOR"), default to the currently active benchmark.
- Common current benchmarks:
  - USD: SOFR (post-2023), previously 3M LIBOR
  - EUR: €STR, EURIBOR
  - GBP: SONIA
  - CHF: SARON
  - JPY: TONAR (TONA)
  - AUD: AONIA, BBSW
  - NZD: NZIONA

### Government Bonds

US Treasury bonds:
- Direct to `FI_GO_NOTE_BOND`.
- Do not direct US Treasuries to `FI_GO_BO_OA`.

Canada government bonds:
- Direct only Canadian bonds to `FI_GO_BO_OA`.

Other government bonds:
- Euro Area: `FI_GO_BO_EA`
- UK: `FI_GO_BO_UK`
- Japan: `FI_GO_BO_JP`

Common group IDs: see `references/group-ids.md` for the full lookup table. When a user provides a group ID directly, execute immediately without searching.

## DataQuery Functions (Computed Expressions)

DataQuery supports an extensive library of built-in functions that can be applied to any time-series expression. Functions execute server-side by wrapping a `DB()` expression and passing the result to the `expression-timeseries` endpoint.

Syntax: `FUNCTION(params, DB(group, instrument, attribute, ...))`. The `DB()` expression is the input to the function.

Execution: use the `expression-timeseries` command.
```bash
dataquery expression-timeseries --expressions "FUNCTION(params, DB(...))" --start-date TODAY-1Y
```

Full function reference: see `references/functions.md` for all available functions, parameters, and formulas.

Quick syntax lookup: use `function-help --name VOL` to look up a function's exact syntax, or `function-help --list` to see all 158 available functions. This command runs locally and requires no API call.

### When to Use Functions

Match user requests to the appropriate DQ function and infer it from context rather than asking the user which function to use. Use only functions that exist in `references/functions.md`, and verify the exact name and parameter order with `function-help --name <FUNC>` before building an expression — never invent a function or its parameters (see Grounding Rules). Common functions:

| User asks for... | Function | Example |
|---|---|---|
| Moving average | `MOVAVG(NDays, expr)` | `MOVAVG(20, DB(FGB,T,0.5,11/15/2034,91282CLW6,MIDYLD))` |
| Volatility / vol | `VOL(NDays, expr)` | `VOL(30, DB(BIGI,ABS,Q10,TR,YTDR,LOC))` |
| Percent change | `PCTCHG(NDays, expr)` | `PCTCHG(1, DB(BIGI,ABS,Q10,TR,YTDR,LOC))` |
| Correlation | `CORR(NDays, exprY, exprX)` | `CORR(60, DB(...), DB(...))` |
| Beta / regression | `BETA(NDays, exprY, exprX)` | `BETA(1y, DB(...), DB(...))` |
| Z-score | `ZSCORE(NDays, expr)` | `ZSCORE(252, DB(BIGI,ABS,Q10,TR,YTDR,LOC))` |
| RSI | `RSI(NDays, expr)` | `RSI(14, DB(BIGI,ABS,Q10,TR,YTDR,LOC))` |
| Indexed to 100 | `INDEX(expr)` | `INDEX(DB(BIGI,ABS,Q10,TR,YTDR,LOC))` |
| Spread | arithmetic | `DB(...series1...) - DB(...series2...)` |
| Treasury rate | `TSYRATE(Fwd, Tenor)` | `TSYRATE(0Y, 10Y)` |

For the full 158-function mapping (EWMA, percentile, skew, kurtosis, DV01, YTM, MODDUR, ROLLING, RESIDUAL, etc.), see `references/functions.md` or run `function-help --name <FUNC>`.

### Advanced Expression Features

The following capabilities are supported. See the Usage Patterns section of `references/functions.md` for examples of each:
- Composition (nesting): wrap one function around another, with `DB()` as the innermost source, for example `ZSCORE(252, VOL(30, DB(...)))`.
- Aggregate (`AG*`) functions: combine multiple series cross-sectionally, for example `AGAVG(DB(...), DB(...))`.
- Frequency conversion: `MONTHLY(...)`, `WEEKLY(...)`, `QUARTERLY(...)`, `YEARLY(...)` as an alternative to `--frequency`.
- Arithmetic operators between series: `+`, `-` (spreads), `*`, `/`, and constants.
- Flexible NDays formats: integer business days, or `Ny` / `Nm` / `Nw` for years, months, and weeks.

Key operational note: functions execute server-side, and `--start-date` should bound the output window. The engine handles any additional lookback internally (for example, `VOL(30, ...)` with one year of output uses `--start-date TODAY-1Y`). Multiple `--expressions` flags can be passed in a single call.

## Workflows

### Workflow 1: Discovery (find what data exists)
Step 1: use `search` to identify datasets from the user's natural-language query:
```bash
dataquery search --query "<user query>"
```
Step 2: drill into specifics using the group IDs and instruments returned:
1. `instruments --group-id <id>`: list instruments in the identified dataset
2. `attributes --group-id <id>`: list available analytics and attributes
3. `filters --group-id <id>`: list currency and country filters

Legacy fallback (only if search returns no results):
1. `groups-search --keywords <term>`: keyword-based dataset search

### Workflow 2: Pull Time-Series Data
There are three ways to retrieve time-series. Select the one that best fits the user's input.

By group (bulk), best for pulling all instruments in a group:
```bash
dataquery group-timeseries --group-id IN_CR_USD_ABS --attributes TR,YTDR,LOC --filter "currency(USD)" --data ALL --start-date TODAY-1M
```

By instrument ID, best when the user knows specific instruments:
```bash
dataquery instrument-timeseries --instruments <ID1> --instruments <ID2> --attributes TR,YTDR --data ALL --start-date TODAY-5D
```

By DQ expression, best for users familiar with traditional DQ syntax:
```bash
dataquery expression-timeseries --expressions "DB(BIGI,ABS,Q10,TR,YTDR,LOC)" --start-date TODAY-5D
```

### Workflow 3: Computed Analytics (Functions)
Use this workflow when the user asks for analytics such as moving averages, volatility, correlations, spreads, or z-scores.

Step 1: identify the underlying data source. Use `search` if the user provides a natural-language description, or construct the `DB()` expression from a known group, instrument, and attribute.

Step 2: wrap the `DB()` expression with the appropriate function or functions from `references/functions.md`.

Step 3: execute via `expression-timeseries`:
```bash
# 20-day moving average of a bond yield
dataquery expression-timeseries --expressions "MOVAVG(20, DB(FGB,T,0.5,11/15/2034,91282CLW6,MIDYLD))" --start-date TODAY-6M

# 30-day realized volatility of an index
dataquery expression-timeseries --expressions "VOL(30, DB(BIGI,ABS,Q10,TR,YTDR,LOC))" --start-date TODAY-1Y

# Spread between two yields
dataquery expression-timeseries --expressions "DB(...series1...) - DB(...series2...)" --start-date TODAY-1Y

# Correlation between two series
dataquery expression-timeseries --expressions "CORR(60, DB(...series1...), DB(...series2...))" --start-date TODAY-1Y

# Multiple analytics in one call
dataquery expression-timeseries --expressions "VOL(30, DB(...))" --expressions "MOVAVG(20, DB(...))" --start-date TODAY-1Y
```

Note: when presenting results from function expressions, always show the full expression used (for example, `MOVAVG(20, DB(...))`) so users can reuse it.

### Workflow 4: CSV Export for Spreadsheets
Add `--output-csv <filename>` to any time-series command. This also works with function expressions.
```bash
# Group time-series to CSV
dataquery group-timeseries --group-id FI_GO_BO_CE --attributes AM_CAP_ACCR --data ALL --start-date TODAY-1M --output-csv bonds.csv

# Expression time-series to CSV
dataquery expression-timeseries --expressions "DB(BIGI,ABS,Q10,TR,YTDR,LOC)" --start-date TODAY-1Y --output-csv abs_returns.csv

# Computed analytics to CSV
dataquery expression-timeseries --expressions "VOL(30, DB(BIGI,ABS,Q10,TR,YTDR,LOC))" --start-date TODAY-1Y --output-csv vol_data.csv
```
The CSV contains: date, value, instrument_id, instrument_name, attribute_id, attribute_name, expression, label, last_published, group_id, group_name.

### Workflow 5: File Delivery API (Published Files)

The File Delivery API serves pre-published files (Parquet, CSV, daily catalogs, full-history snapshots) rather than time-series API responses. Common triggers include "download the catalog", "pull yesterday's file", "which files were published last week", "get me a month of daily snapshots", "backfill missing files", and "watch for new publications".

Choosing between the two APIs:
- Use the File Delivery API when the user asks for files, downloads, catalogs, snapshots, or a whole dataset for a date or date range, or when the result would be too large for time-series calls.
- Use the API v2 time-series commands (Workflows 2 to 4) when the user wants specific values, instruments, or computed analytics to view or export as CSV.
- If a dataset is only delivered as files (`files` lists file group IDs but time-series calls return nothing), switch to this workflow and tell the user.

Step 1: discover which file types a group publishes.
```bash
dataquery files --group-id FI_GO_NOTE_BOND --json
```
Returns the `file-group-id` and `file-type` values for that dataset. Use `--file-group-id` to narrow to one file type and `--limit` to cap results. Every file group ID used in later steps must come from this output or from the user.

Step 2: see which files were actually published for a date range.
```bash
dataquery available-files --group-id FI_GO_NOTE_BOND --start-date 20260501 --end-date 20260531 --json

# Restrict to a single file type
dataquery available-files --group-id FI_GO_NOTE_BOND --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG \
    --start-date 20260501 --end-date 20260531 --json
```
Each record has `file-group-id`, `file-datetime`, `is-available`, and `last-modified`. Use this step to answer "what was published?" questions, find the latest available date, spot gaps before a bulk download, and pick exact `--file-datetime` values for Step 3a.

Step 2b (single date): check one file on one date.
```bash
dataquery availability --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG --file-datetime 20260606 --json
```
Always pass `--json`; the text output only echoes the inputs and does not say whether the file is available.

Step 3a: download a single file.
```bash
dataquery download --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG --file-datetime 20260606 --destination ./downloads --json
```
`--file-datetime` accepts `YYYYMMDD`, `YYYYMMDDTHHMM`, or `YYYYMMDDTHHMMSS`. Use the exact value returned by `available-files` for intraday files. Tune chunking for very large files with `--num-parts 8 --chunk-size 4194304`.

Step 3b: bulk date-range download (best for "give me a month of daily catalogs" or backfills).
```bash
dataquery download-group --group-id FI_GO_NOTE_BOND --start-date 20260501 --end-date 20260531 --destination ./downloads --json

# Restrict to specific file-group-ids
dataquery download-group --group-id FI_GO_NOTE_BOND --start-date 20260501 --end-date 20260531 \
    --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG --destination ./downloads --max-concurrent 5 --num-parts 8 --json
```
Only files flagged available in the window are downloaded. Report the successful and failed counts from the result. For a failed file, retry it once with `download`, then report it to the user if it still fails. For windows longer than a few months, split the request into monthly `download-group` calls so one failure does not force a restart of the whole range.

Step 3c: live watch for new publications (SSE).
```bash
# Watch every new file in a group
dataquery download --watch --group-id FI_GO_NOTE_BOND --destination ./downloads

# Server-side filter to specific file-group-ids
dataquery download --watch --group-id FI_GO_NOTE_BOND --file-group-id DQ_CATALOG DQ_TRADES --destination ./downloads

# Discard persisted last-event-id and start fresh
dataquery download --watch --group-id FI_GO_NOTE_BOND --destination ./downloads --reset-event-id
```
Watch mode runs until it is stopped (Ctrl+C) and never returns on its own. Start it only when the user explicitly asks to watch or subscribe, run it as a background process, and tell the user how to stop it. On stop, it prints JSON stats for the session. The CLI keeps a last-event-id checkpoint across sessions so restarts resume cleanly; `--no-event-replay` disables that and falls back to an availability check on startup.

Output format note: the File Delivery API commands (`files`, `available-files`, `availability`, `download`, `download-group`) use the SDK's native output (text by default, or pure JSON with `--json`), not the `summary + --- JSON ---` envelope used by the API v2 commands above. Prefer `--json` and parse accordingly:
- Text mode produces lines such as `Found 12 files` and `Downloaded to ./downloads/<file>`.
- `--json` mode emits a single JSON document with no separator.

Common File Delivery API parameters:

| Flag | Used by | Purpose |
|---|---|---|
| `--group-id` | files, available-files, download-group, download --watch | Dataset identifier |
| `--file-group-id` | files (filter), available-files (filter), availability, download, download-group (filter), watch (server filter) | Specific file id, or list for watch/bulk filter |
| `--file-datetime` | availability, download | `YYYYMMDD[THHMM[SS]]` for the file's publication date |
| `--start-date / --end-date` | available-files, download-group | YYYYMMDD window (convert relative requests like "last week" to absolute dates) |
| `--destination` | download, download-group | Local directory (default `./downloads` for bulk) |
| `--num-parts` | download, download-group | Parallel HTTP range parts (default 5) |
| `--chunk-size` | download | Bytes per part (default 1 MiB) |
| `--max-concurrent` | download-group | Concurrent file downloads (default 3) |
| `--watch` | download | Switch to SSE notification mode |
| `--reset-event-id` | download --watch | Discard persisted last-event-id checkpoint |
| `--no-event-replay` | download --watch | Disable cross-process event replay |
| `--json` | files, available-files, availability, download, download-group | JSON-only output |

## All Endpoints Quick Reference

### API v2 (summary + `--- JSON ---` output)

| # | Command | What it does | Key params |
|---|---------|-------------|------------|
| 0 | `search` | Dataset discovery (use first) | `--query` |
| 1 | `groups` | List all datasets | `--limit --search` |
| 2 | `groups-search` | Search datasets (legacy keyword search) | `--keywords` |
| 3 | `instruments` | List instruments in a group | `--group-id` |
| 4 | `instruments-search` | Search instruments | `--group-id --keywords` |
| 5 | `filters` | Get currency/country filters | `--group-id` |
| 6 | `attributes` | Get analytics list | `--group-id` |
| 7 | `group-timeseries` | Bulk time-series | `--group-id --attributes` |
| 8 | `instrument-timeseries` | Time-series by instrument ID | `--instruments --attributes` |
| 9 | `expression-timeseries` | Time-series by DQ expression | `--expressions` |
| 10 | `grid-data` | Grid data by expression or grid ID | `--expr` or `--grid-id` |
| 11 | `heartbeat` | Service status | (none) |
| 12 | `function-help` | DQ function syntax (local lookup) | `--name` or `--list` |

### File Delivery API (text or `--json` output)

| # | Command | What it does | Key params |
|---|---------|-------------|------------|
| 13 | `files` | List file types a group publishes | `--group-id` |
| 14 | `available-files` | List published files across a date range | `--group-id --start-date --end-date` |
| 15 | `availability` | Check one file on one date | `--file-group-id --file-datetime` |
| 16 | `download` | Download a single file (or watch with `--watch`) | `--file-group-id --file-datetime` |
| 17 | `download-group` | Bulk date-range download | `--group-id --start-date --end-date` |

## Presenting Results

Always include the data source identifier (expression, or instrument and attribute) so users can reuse queries.

- Time-series: present a table with columns Date, Value, Instrument, and Expression. For large result sets, summarize the first and last few rows plus the total count.
- Groups and instruments: present a table with columns ID, Name, and Description.
- Heartbeat: report "DataQuery is UP" or "DataQuery is DOWN".
- If a `page` cursor is returned, fetch the next page automatically (the token expires after 30 minutes).
- If CSV was exported, confirm the filename and row count.
- File listings: present a table with columns File Group ID, File Type (or File Datetime), and Available. Call out missing dates explicitly.
- Downloads: report the local path of each file, plus successful and failed counts for bulk downloads. Never claim a file was downloaded unless the command reported it.

## Error Handling

| HTTP Status | Meaning | Suggested action |
|---|---|---|
| 400 | Bad Request | Check parameter values and format |
| 401 | Authentication Error | Auth token may have expired; re-run to refresh the token |
| 403 | Forbidden | Premium dataset; contact DataQuery_Sales@jpmorgan.com |
| 404 | Not Found | Verify the group ID or instrument ID exists; for downloads, the file may not be published for that date (check `available-files`) |
| 500 | Server Error | Retry in a few minutes |
| 503 | Service Down | DataQuery maintenance; retry later |

## Constraints
- Rate limit: 1 call per 200 ms per client ID
- Maximum URL length: 2,080 characters
- Instrument limit: 20 per call
- Page token: expires after 30 minutes

Refer to `references/parameters.md` for all enum values, `references/endpoints.md` for detailed examples, and `references/functions.md` for the complete DQ functions glossary.
