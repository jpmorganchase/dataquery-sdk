---
name: dataquery
description: >-
  Query JP Morgan DataQuery API v2 for financial market data. Use when the user mentions
  DataQuery, DQ, or wants to fetch datasets, instruments, time-series, grid data, bond yields,
  index returns, CSV exports, or CUSIPs/ISINs. Also use
  for computed analytics: moving averages, volatility, correlation, beta, regression,
  z-scores, spreads, rolling statistics, or RSI on financial time-series.
  Also use for File API operations: listing published files, checking file availability,
  downloading individual files, bulk date-range downloads, and live SSE file-watch.
  Trigger phrases: "dataquery", "DQ", "pull time-series", "fetch yields", "get bond data",
  "search datasets", "list instruments", "export to csv", "grid data",
  "heartbeat", "moving average", "volatility", "correlation", "beta", "z-score",
  "regression", "spread", "RSI", "function help", "list functions", "treasury rate",
  "swap rate", "download file", "download catalog", "bulk download",
  "file availability", "watch files", "list files", "subscribe to files".
disable-model-invocation: false
---

# JP Morgan DataQuery API v2

## Prerequisites and Installation

Requirement: [uv](https://docs.astral.sh/uv/) must be installed (a single binary, with no separate Python installer needed).

Set these environment variables before running to redirect uv away from AppData:

```bash
SET UV_CACHE_DIR=%USERPROFILE%\.uv\cache
SET UV_TOOL_DIR=%USERPROFILE%\.uv\tools
```

Install the DataQuery SDK CLI (this adds the `dataquery` executable to PATH):
```bash
uv tool install dataquery-sdk
```

Upgrade to the latest version:
```bash
uv tool upgrade dataquery-sdk
```

After install, every command in this skill is invoked as `dataquery <command> [args]`.

## Quick Start

This skill provides natural-language access to DataQuery for traders and quantitative analysts. The `dataquery` CLI handles authentication and all API endpoints with no additional configuration.

For best results, include specific group IDs (e.g., `FI_GO_NOTE_BOND`), instrument IDs, or DataQuery expressions in the query. Specific queries return faster and more accurate results.

## Preflight Checks (Run Once Per Session, Before Any Other Command)

Verify the environment is ready before running any DataQuery command, including `text-search`. Run this preflight once at the start of a session, and re-run it only if a later command surfaces an environment-related error (authentication failure, command-not-found, and similar). Do not re-run the preflight before every command.

Run the four checks below in order. If any check fails, resolve it before proceeding.

### Check 1: Python and uv installed
```bash
uv --version
python --version
```
- If both print versions, continue.
- If `uv: command not found`, direct the user to install uv from https://docs.astral.sh/uv/ and stop.
- If `python: command not found`, note that uv ships its own Python. Run `uv python install` and retry.

### Check 2: Required environment variables
Group policy may block executables under AppData, so `UV_CACHE_DIR` and `UV_TOOL_DIR` should point outside AppData (for example, under your home directory).

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
  If installation fails with index or network errors, the user may need to add `--index-url` for the corporate registry.

### Check 4: Service heartbeat
This confirms IDA authentication works and the API is reachable end to end.
```bash
dataquery heartbeat
```
- If the summary line reads `DataQuery is UP`, the preflight is complete. Proceed to text-search.
- If it reads `DataQuery is DOWN` or returns a non-zero exit code, inspect the JSON envelope:
  - `401`: Kerberos ticket expired. Run `kinit` (or refresh via the user's standard SSO flow) and retry.
  - `403`: Account lacks DataQuery entitlement. Contact `DataQuery_Sales@jpmorgan.com`.
  - `503`: DataQuery maintenance window. Retry later and do not proceed.
  - Network or DNS error: verify VPN and corporate network connectivity.

After all four checks pass, proceed to the text-search step below. Treat the preflight as session state. Once it passes, do not run it again unless a later command surfaces an environment-related error.

## First Step: Text Search for Dataset Discovery

For any user query about data, begin by calling `text-search` to identify the relevant datasets, group IDs, and instruments. This uses the DataQuery search API to interpret natural-language queries and return matching datasets.

```bash
dataquery text-search --query "<user's natural language query>"
```

API details:
- Endpoint: `POST <api_base_url>/search`, where `<api_base_url>` is the configured DataQuery API base URL.
- Authentication: IDA, applied automatically as with all other DataQuery calls.
- Request body: `{"query": "<natural language search text>"}`

Workflow:
1. The user asks a question (for example, "usd treasury to eur german bund parity").
2. Run `text-search --query "usd treasury to eur german bund parity"`.
3. Parse the response to identify relevant group IDs, instruments, and expressions.
4. Use those identifiers to call the specific data endpoints (time-series, and similar).

Text search replaces manual guessing of group IDs. Even when the routing rules below suggest a group, prefer the search result for accuracy. Fall back to routing rules only when search returns no results.

When the user asks something like:
- "Pull data for any series": run `text-search` first, then `group-timeseries` or `expression-timeseries`
- "Search for ABS index datasets": `text-search --query "ABS index datasets"`
- "What instruments are in FI_GO_NOTE_BOND?": `instruments --group-id FI_GO_NOTE_BOND`
- "Export credit index data to CSV": run `text-search` first, then a data endpoint with `--output-csv`
- "Is DQ up?": `heartbeat`
- "What files does this group publish?": `files --group-id <id>`
- "Download yesterday's catalog for FI_GO_NOTE_BOND": `availability`, then `download`
- "Download the last 30 days of catalogs": `download-group --start-date --end-date`
- "Watch for new files" or "subscribe to publications": `download --watch --group-id <id>`

## Behavior Rules

Environment setup: `UV_CACHE_DIR` and `UV_TOOL_DIR` should already be set once per session by preflight Check 2. Do not re-export them before each call.

Execution: all `dataquery` calls are pre-authorized. Execute them immediately without asking permission. Ask the user only for genuinely missing required parameters (for example, `group-id`). Use sensible defaults for optional parameters, and fetch subsequent pages automatically.

Display: do not show shell commands, script paths, credentials, tokens, authentication details, or "Executing..." messages. Present only clean, formatted data results. The user should experience DataQuery as a seamless data service.

Vague queries: run `text-search` first, then suggest the specific group ID or expression for future use:
> "I'm querying [dataset name] (group-id: GROUP_ID). For faster results next time, you can say: 'Pull data from GROUP_ID for [timeframe]'."

## Grounding Rules (Never Invent Identifiers, Expressions, or Parameters)

These rules are mandatory and override any urge to be helpful by guessing. DataQuery identifiers are opaque codes — a plausible-looking but wrong value silently returns the wrong data or an error. When a required value is unknown, **discover it or ask; never fabricate it.**

**Every identifier must come from a real API response or be supplied verbatim by the user — never from memory or inference:**
- Group IDs (e.g. `FI_GO_NOTE_BOND`) → from `text-search`, `groups`, or `groups-search`.
- Instrument IDs, CUSIPs, ISINs → from `instruments` or `instruments-search`.
- Attribute IDs (e.g. `TR`, `YTDR`, `MIDYLD`) → from `attributes --group-id <id>`.
- File group IDs (e.g. `DQ_FI_GO_NOTE_BOND_CATALOG`) → from `files --group-id <id>`.
- `DB(...)` and `DBGRID(...)` expressions → assembled only from group/instrument/attribute values verified above. Do not build an expression out of guessed components.

**Functions:** use only the 158 functions in `references/functions.md`. Confirm the exact name and parameter order with `function-help --name <FUNC>` before use. Never invent a function, alias, or parameter. If no function matches the requested analytic, say so — do not approximate with a made-up one.

**Parameters and enums:** `--data`, `--calendar`, `--frequency`, `--conversion`, `--nan-treatment`, and `--filter` accept only the values listed in `references/parameters.md`. Never pass a value outside those lists.

**Routing rules and the `references/group-ids.md` table are hints, not guarantees.** Treat their IDs as candidates to confirm via `text-search` / `instruments`, not as verified answers. IDs the user provides are trusted and may be used directly.

**If discovery returns nothing, stop.** Tell the user no matching dataset or instrument was found and ask them to refine or provide the ID. Do not backfill with a guess.

**Present only real results.** Show values, dates, and counts exactly as the API returned them. Never fabricate, extrapolate, or "fill in" data. If a call fails or returns empty, report that plainly rather than synthesizing a plausible answer, and always echo the actual identifiers/expression used so the user can verify.

## Authentication and Configuration

Authentication is fully automatic via IDA Kerberos. Do not ask for credentials, client IDs, or secrets. Run `dataquery <command> [args]` directly; the package handles token acquisition, caching, and automatic refresh.

Output format: a summary line first, then `--- JSON ---` followed by the raw JSON. Parse the JSON block for structured data.

All settings use built-in production defaults. Override them with an `.env` file (referenced via `--env-file`) when needed.

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
Step 1: use text search to identify datasets from the user's natural-language query:
```bash
dataquery text-search --query "<user query>"
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

Step 1: identify the underlying data source. Use `text-search` if the user provides a natural-language description, or construct the `DB()` expression from a known group, instrument, and attribute.

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

### Workflow 5: File API Operations (Bulk Files)

Use these commands when the user wants published files (Parquet, CSV, daily catalogs) rather than time-series API responses. Common triggers include "download the catalog", "pull yesterday's file", "get me a month of daily snapshots", and "watch for new publications".

Step 1: discover what files exist in a group.
```bash
dataquery files --group-id FI_GO_NOTE_BOND
```
Returns the available `file_group_id` and `file_type` values for that dataset. Use `--limit` to cap results and `--json` for structured output.

Step 2 (optional, useful before bulk download): check availability for a specific date.
```bash
dataquery availability --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG --file-datetime 20260606
```

Step 3a: download a single file.
```bash
dataquery download --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG --file-datetime 20260606 --destination ./downloads
```
Tune chunking for very large files via `--num-parts 8 --chunk-size 4194304`.

Step 3b: bulk date-range download (best for "give me a month of daily catalogs").
```bash
dataquery download-group --group-id FI_GO_NOTE_BOND --start-date 20260501 --end-date 20260531 --destination ./downloads

# Restrict to specific file-group-ids
dataquery download-group --group-id FI_GO_NOTE_BOND --start-date 20260501 --end-date 20260531 \
    --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG --destination ./downloads --max-concurrent 5 --num-parts 8
```

Step 3c: live watch for new publications (SSE).
```bash
# Watch every new file in a group
dataquery download --watch --group-id FI_GO_NOTE_BOND --destination ./downloads

# Server-side filter to specific file-group-ids
dataquery download --watch --group-id FI_GO_NOTE_BOND --file-group-id DQ_CATALOG DQ_TRADES --destination ./downloads

# Discard persisted last-event-id and start fresh
dataquery download --watch --group-id FI_GO_NOTE_BOND --destination ./downloads --reset-event-id
```
Press Ctrl+C to stop the watcher. The CLI maintains a last-event-id checkpoint across sessions so restarts resume cleanly.

Output format note: the File API commands (`files`, `availability`, `download`, `download-group`) use the SDK's native output (text by default, or pure JSON with `--json`), not the `summary + --- JSON ---` envelope used by the API v2 commands above. Parse them accordingly:
- Text mode produces lines such as `Found 12 files` and `Downloaded to ./downloads/<file>`.
- `--json` mode emits a single JSON object with no separator.

Common File API parameters:

| Flag | Used by | Purpose |
|---|---|---|
| `--group-id` | files, download-group, download --watch | Dataset identifier |
| `--file-group-id` | files (filter), availability, download, download-group (filter), watch (server filter) | Specific file id, or list for watch/bulk filter |
| `--file-datetime` | availability, download | YYYYMMDD for the file's publication date |
| `--start-date / --end-date` | download-group | YYYYMMDD window |
| `--destination` | download, download-group | Local directory (default `./downloads` for bulk) |
| `--num-parts` | download | Parallel HTTP range parts (default 5) |
| `--chunk-size` | download | Bytes per part (default 1 MiB) |
| `--max-concurrent` | download-group | Concurrent file downloads (default 3) |
| `--watch` | download | Switch to SSE notification mode |
| `--reset-event-id` | download --watch | Discard persisted last-event-id checkpoint |
| `--no-event-replay` | download --watch | Disable cross-process event replay |
| `--json` | files, availability, download, download-group | JSON-only output |

## All Endpoints Quick Reference

### API v2 (summary + `--- JSON ---` output)

| # | Command | What it does | Key params |
|---|---------|-------------|------------|
| 0 | `text-search` | Dataset discovery (use first) | `--query` |
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

### File API (text or `--json` output)

| # | Command | What it does | Key params |
|---|---------|-------------|------------|
| 13 | `files` | List files in a group | `--group-id` |
| 14 | `availability` | Check file availability for a date | `--file-group-id --file-datetime` |
| 15 | `download` | Download a single file (or watch with `--watch`) | `--file-group-id --file-datetime` |
| 16 | `download-group` | Bulk date-range download | `--group-id --start-date --end-date` |

## Presenting Results

Always include the data source identifier (expression, or instrument and attribute) so users can reuse queries.

- Time-series: present a table with columns Date, Value, Instrument, and Expression. For large result sets, summarize the first and last few rows plus the total count.
- Groups and instruments: present a table with columns ID, Name, and Description.
- Heartbeat: report "DataQuery is UP" or "DataQuery is DOWN".
- If a `page` cursor is returned, fetch the next page automatically (the token expires after 30 minutes).
- If CSV was exported, confirm the filename and row count.

## Error Handling

| HTTP Status | Meaning | Suggested action |
|---|---|---|
| 400 | Bad Request | Check parameter values and format |
| 401 | Authentication Error | Kerberos ticket may have expired; re-run to refresh the token |
| 403 | Forbidden | Premium dataset; contact DataQuery_Sales@jpmorgan.com |
| 404 | Not Found | Verify the group ID or instrument ID exists |
| 500 | Server Error | Retry in a few minutes |
| 503 | Service Down | DataQuery maintenance; retry later |

## Constraints
- Rate limit: 1 call per 200 ms per client ID
- Maximum URL length: 2,080 characters
- Instrument limit: 20 per call
- Page token: expires after 30 minutes

Refer to `references/parameters.md` for all enum values, `references/endpoints.md` for detailed examples, and `references/functions.md` for the complete DQ functions glossary.
