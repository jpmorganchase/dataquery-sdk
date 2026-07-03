# DataQuery API v2 - Endpoint Reference (Trading Desk)

Invocation: `dataquery <command> [args]`

Authentication is automatic via IDA Kerberos — no auth flags needed.

---

## Dataset Discovery

### 0. Text Search (MANDATORY FIRST STEP)
Natural-language dataset discovery — use before any data pull when group IDs are not already known.
```bash
dataquery text-search --query "usd treasury to eur german bund parity"
dataquery text-search --query "emerging market interest rate swaps"
dataquery text-search --query "ABS index total return USD"
```
Returns: matching group IDs, instrument IDs, and expressions ranked by relevance.

### 1. List All Datasets
```bash
dataquery groups
dataquery groups --page <token>   # next page
```
Returns: group-id, group-name, description, taxonomy, population counts, top instruments.

### 2. Search Datasets (legacy keyword search)
```bash
dataquery groups-search --keywords treasury
dataquery groups-search --keywords "credit default"
dataquery groups-search --keywords 912810QK7    # search by CUSIP
```
Tip: Prefer `text-search` (command 0) — use `groups-search` only as a fallback when text-search returns no results.

### 3. List Instruments
```bash
dataquery instruments --group-id IN_CR_USD_ABS
dataquery instruments --group-id FI_GO_NOTE_BOND
dataquery instruments --group-id FI_GO_BO_CE --instrument-id 42588b17d59a1e4f06033d187d98f11f-DQGNMTBNDFIM
```
Returns: instrument-id, instrument-name, country, currency, CUSIP, ISIN. Max 20 instrument IDs per call.

### 4. Search Instruments
```bash
dataquery instruments-search --group-id IN_CR_USD_ABS --keywords AAA
dataquery instruments-search --group-id FI_GO_BO_CE --keywords "3/4"
```

### 5. Get Filters
```bash
dataquery filters --group-id IN_CR_USD_ABS
```
Returns currency/country filters you can use with `--filter` on time-series calls.

### 6. Get Attributes
```bash
dataquery attributes --group-id IN_CR_USD_ABS
dataquery attributes --group-id FI_GO_BO_CE --instrument-id 42588b17d59a1e4f06033d187d98f11f-DQGNMTBNDFIM
```
Returns: attribute-id (e.g., TR, YTDR, MIDYLD, AM_CAP_ACCR), attribute-name, expression.

---

## Time-Series (Core Trading Desk Usage)

### 7. Group Time-Series (bulk pull)
Best for: pulling all instruments in a group at once with filters.
```bash
# ABS index returns - USD only
dataquery group-timeseries --group-id IN_CR_USD_ABS --attributes TR,YTDR,LOC --filter "currency(USD)" --data ALL --start-date TODAY-1M

# Central European govt bonds - last 5 days
dataquery group-timeseries --group-id FI_GO_BO_CE --attributes AM_CAP_ACCR --filter "currency(EUR)" --data ALL --start-date TODAY-5D

# Export to CSV for Excel
dataquery group-timeseries --group-id FI_GO_BO_CE --attributes AM_CAP_ACCR --data ALL --start-date TODAY-1M --output-csv bonds_data.csv
```

### 8. Instrument Time-Series (specific instruments)
Best for: when you know exactly which instruments you need.
```bash
# Single instrument
dataquery instrument-timeseries --instruments 2f4835580ae2f8973886f44004823c0b-DQAGGRABSSEI --attributes TR,YTDR,LOC --data REFERENCE_DATA

# Multiple instruments + time-series
dataquery instrument-timeseries --instruments 2f4835580ae2f8973886f44004823c0b-DQAGGRABSSEI --instruments 42588b17d59a1e4f06033d187d98f11f-DQGNMTBNDFIM --attributes AM_CAP_ACCR --data ALL --start-date TODAY-5D

# Weekly frequency with CSV
dataquery instrument-timeseries --instruments <ID> --attributes TR --data ALL --start-date TODAY-1Y --frequency FREQ_WEEK --output-csv weekly.csv
```

### 9. Expression Time-Series (traditional DQ syntax)
Best for: users who know DataQuery expression format `DB(group,instrument,attribute)`.
```bash
# ABS index YTD return
dataquery expression-timeseries --expressions "DB(BIGI,ABS,Q10,TR,YTDR,LOC)"

# Euro area govt bond dirty mid price
dataquery expression-timeseries --expressions "DB(MTE,SPGB EUR 1.000 31-Oct-2050 LON,,ES0000012G00,DIRTY_MIDPRC)" --start-date TODAY-5D

# Multiple expressions at once
dataquery expression-timeseries --expressions "DB(BIGI,ABS,Q10,TR,YTDR,LOC)" --expressions "DB(FHRA,05Y,MIDYLD)" --data ALL --start-date TODAY-1M
```

### Common Time-Series Options
| Flag | Default | Values |
|---|---|---|
| `--data` | REFERENCE_DATA | REFERENCE_DATA, NO_REFERENCE_DATA, ALL |
| `--start-date` | TODAY-1D | YYYYMMDD, TODAY-nD/W/M/Y |
| `--end-date` | TODAY | YYYYMMDD, TODAY-nD/W/M/Y |
| `--calendar` | CAL_USBANK | See parameters.md |
| `--frequency` | FREQ_DAY | FREQ_INTRA, FREQ_DAY, FREQ_WEEK, FREQ_MONTH, FREQ_QUARTER, FREQ_ANN |
| `--conversion` | CONV_LASTBUS_ABS | See parameters.md |
| `--nan-treatment` | NA_NOTHING | NA_NOTHING, NA_LAST, NA_NEXT, NA_INTERP |
| `--output-csv` | (none) | filename.csv or - for stdout |

---

## Grid Data

### 10. Grid Data (cross-sectional grids)
Best for: volatility surfaces and other 2-D grids, by `DBGRID(...)` expression or a saved grid ID.
```bash
# By grid expression (e.g. an equity vol surface)
dataquery grid-data --expr "DBGRID(EQTY,2823 HK,ABS_REL,ATMF,CLOSE,VOL)"

# By saved grid ID, for a specific date
dataquery grid-data --grid-id <GRID_ID> --date 20260606

# Export the grid to CSV
dataquery grid-data --expr "DBGRID(...)" --output-csv grid.csv
```
Provide either `--expr` or `--grid-id` (one is required). Returns one or more series, each with its own records.

---

## Service & Local Lookups

### 11. Heartbeat
```bash
dataquery heartbeat
```
Returns service status. Quick way to verify connectivity and auth are working.

### 12. Function Help (local, no API call)
Look up DQ function syntax without hitting the API. Full glossary in `references/functions.md`.
```bash
dataquery function-help --name VOL     # syntax for one function
dataquery function-help --list         # all 158 functions
```

---

## File API (Bulk Data Files)

These commands work with **published files** (parquet, CSV, etc.) rather than time-series API responses. Use them when the user wants raw bulk data — daily catalogs, full-history snapshots, or any pre-published file.

### 13. List Files in a Group
```bash
dataquery files --group-id FI_GO_NOTE_BOND
dataquery files --group-id FI_GO_NOTE_BOND --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG
dataquery files --group-id FI_GO_NOTE_BOND --limit 10 --json
```
Returns: file_group_id, file_type, description. Use this to discover what files a dataset publishes.

### 14. Check File Availability
```bash
dataquery availability --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG --file-datetime 20260606
dataquery availability --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG --file-datetime 20260606 --json
```
Returns: whether the file is ready for the given date. Useful before bulk download.

### 15. Download a Single File
```bash
# Direct download
dataquery download --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG --file-datetime 20260606 --destination ./downloads

# Custom chunking for very large files
dataquery download --file-group-id DQ_BIG_FILE --file-datetime 20260606 --destination ./downloads --num-parts 8 --chunk-size 4194304

# JSON output for scripting
dataquery download --file-group-id FG --file-datetime 20260606 --destination ./downloads --json
```
Streams via parallel HTTP range requests. Returns the local path on success.

#### Live File Watch (SSE) — `download --watch` mode
Subscribe to the notification stream and auto-download new files as they're published.
```bash
# Watch all new files for a group
dataquery download --watch --group-id FI_GO_NOTE_BOND --destination ./downloads

# Server-side filter to specific file-group-ids
dataquery download --watch --group-id FI_GO_NOTE_BOND --file-group-id DQ_CATALOG DQ_TRADES --destination ./downloads

# Fresh subscription (discard persisted last-event-id)
dataquery download --watch --group-id FI_GO_NOTE_BOND --destination ./downloads --reset-event-id
```
Ctrl+C to stop. Uses server-side filtering when `--file-group-id` is set.

### 16. Bulk Date-Range Download
Best for: pulling every file in a group across a date window (e.g., a full month of daily catalogs).
```bash
# All files in the group, one month
dataquery download-group --group-id FI_GO_NOTE_BOND --start-date 20260501 --end-date 20260531 --destination ./downloads

# Filter to one or more file-group-ids
dataquery download-group --group-id FI_GO_NOTE_BOND --start-date 20260501 --end-date 20260531 --file-group-id DQ_FI_GO_NOTE_BOND_CATALOG --destination ./downloads

# Tune concurrency and parallelism
dataquery download-group --group-id FI_GO_NOTE_BOND --start-date 20260501 --end-date 20260531 --destination ./downloads --max-concurrent 5 --num-parts 8
```

### File API Output Format Note
Unlike the API v2 endpoints above, file-API commands print results in their own format:
- **Text mode (default)**: human-readable lines (e.g. `Found 12 files`, `Downloaded to ./downloads/file.parquet`).
- **`--json` mode**: pure JSON only (no `--- JSON ---` separator).

Parse accordingly — these commands do not emit the `summary + --- JSON ---` envelope.
