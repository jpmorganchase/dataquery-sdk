# DataQuery API v2 - Parameter Reference (Trading Desk)

> **Grounding:** These enum tables are exhaustive. Pass only values listed here — do not invent or guess parameter values, calendars, frequencies, or filter names.

## Date Parameters

### start-date
- **Default:** `TODAY-1D`
- **Formats:** `YYYYMMDD`, `TODAY`, `TODAY-nX` where X is D/W/M/Y
- **Trading desk examples:**
  - `TODAY-5D` - last 5 business days (quick check)
  - `TODAY-1M` - last month
  - `TODAY-3M` - last quarter
  - `TODAY-1Y` - last year
  - `20240101` - from a specific date

### end-date
- **Default:** `TODAY`
- **Formats:** Same as start-date

## data (what to return)
| Value | Description | When to use |
|---|---|---|
| `REFERENCE_DATA` | Reference data only **(default)** | Checking instrument metadata |
| `NO_REFERENCE_DATA` | Time-series values only | Fastest for pure data pulls |
| `ALL` | Both reference data and values | **Most common for trading desk** |

**Tip:** Always use `--data ALL` when you want actual time-series values plus instrument details.

## calendar (business day convention)

### Most used by trading desks:
| Value | Description |
|---|---|
| `CAL_USBANK` | U.S. Bank **(default)** - use for USD products |
| `CAL_UK` | UK - use for GBP products |
| `CAL_EURO` | Euro - use for EUR products |
| `CAL_JAPAN` | Japan - use for JPY products |
| `CAL_NYSE` | NYSE - use for US equities |
| `CAL_ALLDAYS` | All Days - use for crypto/24hr markets |
| `CAL_WEEKDAYS` | All Weekdays - skip weekends only |
| `CALTGT` | TARGET - ECB settlement calendar |

### Full list:
| Value | Description |
|---|---|
| `CAL_USBANK` | U.S. Bank **(default)** |
| `CAL_ALLDAYS` | All Days |
| `CAL_WEEKDAYS` | All Weekdays |
| `CAL_AUSTRALIA` | Australia |
| `CAL_BELGIUM` | Belgium |
| `CAL_CANADA` | Canada |
| `CAL_DENMARK` | Denmark |
| `CAL_EURO` | Euro |
| `CAL_FINLAND` | Finland |
| `CAL_FRANCE` | France |
| `CAL_GERMANY` | Germany |
| `CAL_HONGKONG` | Hong Kong |
| `CAL_IRELAND` | Ireland |
| `CAL_ITALY` | Italy |
| `CAL_JAPAN` | Japan |
| `CAL_MALAYSIA` | Malaysia |
| `CAL_NETHERLANDS` | Netherlands |
| `CAL_NEWZEALAND` | New Zealand |
| `CAL_NYSE` | NYSE |
| `CAL_PORTUGAL` | Portugal |
| `CAL_SAFRICA` | South Africa |
| `CAL_SINGAPORE` | Singapore |
| `CAL_SPAIN` | Spain |
| `CAL_SWEDEN` | Sweden |
| `CAL_SWITZERLAND` | Switzerland |
| `CAL_USEXCH` | USEXCH |
| `CAL_UK` | UK |
| `CALSOF` | Eurex DE FI |
| `CALTGT` | TARGET |
| `CALLIF` | Liffe UK Fin |
| `CAL_EUR_UKFIN` | TGT+UK Fin |
| `CAL_UK_TGT` | TGT+UK |

## frequency
| Value | Description | Typical use |
|---|---|---|
| `FREQ_DAY` | Daily **(default)** | Standard daily data |
| `FREQ_INTRA` | Intraday | Tick/intraday data |
| `FREQ_WEEK` | Weekly | Weekly aggregation |
| `FREQ_MONTH` | Monthly | Monthly reporting |
| `FREQ_QUARTER` | Quarterly | Quarterly reviews |
| `FREQ_ANN` | Annually | Annual summaries |

## conversion (how to aggregate when downsampling)
| Value | Description |
|---|---|
| `CONV_LASTBUS_ABS` | Last Business Day of Absolute Period **(default)** |
| `CONV_FIRSTBUS_ABS` | First Business Day of Absolute Period |
| `CONV_LASTBUS_REL` | Last Business Day of Relative Period |
| `CONV_FIRSTBUS_REL` | First Business Day of Relative Period |
| `CONV_SUM_ABS_SDT` | Sum of Absolute Period (Start Date) |
| `CONV_SUM_ABS_EDT` | Sum of Absolute Period (End Date) |
| `CONV_SUM_REL_SDT` | Sum of Relative Period (Start Date) |
| `CONV_SUM_REL_EDT` | Sum of Relative Period (End Date) |
| `CONV_AVG_ABS_SDT` | Average of Absolute Period (Start Date) |
| `CONV_AVG_ABS_EDT` | Average of Absolute Period (End Date) |
| `CONV_AVG_REL_SDT` | Average of Relative Period (Start Date) |
| `CONV_AVG_REL_EDT` | Average of Relative Period (End Date) |

**Tip:** `CONV_LASTBUS_ABS` (default) is what you want for end-of-period snapshots. Use `CONV_AVG_ABS_EDT` for average prices over a period.

## nan-treatment (missing data handling)
| Value | Description | When to use |
|---|---|---|
| `NA_NOTHING` | Leave as N/A **(default)** | For accurate gap detection |
| `NA_LAST` | Forward-fill with last valid value | **Common for trading models** |
| `NA_NEXT` | Back-fill with next valid value | For alignment checks |
| `NA_INTERP` | Linear interpolation | For smooth curves |

**Tip:** Use `NA_LAST` when feeding data into pricing models that can't handle gaps.

## filter (for group time-series)
- **Syntax:** `currency(XXX)` or `country(YYY)`
- **Examples:** `currency(USD)`, `country(USA)`, `currency(EUR)`
- **Tip:** Use `filters` endpoint first to see valid values for a group

## instrument-id
- Max **20** instrument IDs per API call
- Format: `instrument-id=x&instrument-id=y`
- Example: `42588b17d59a1e4f06033d187d98f11f-DQGNMTBNDFIM`
- **Tip:** Use `instruments` or `instruments-search` to find IDs first

## page
- Pagination token returned in API responses
- **30-minute expiry** before the token becomes invalid
- Pass the token from a previous response to get the next page

## Environment

The base URL and IDA client credentials are read from environment variables (or a `.env`
file passed via `--env-file`). Run `dataquery config template` to generate a `.env.template`
listing every supported variable, then copy it to `.env` and fill in the values. Built-in
defaults target the production DataQuery kong proxy, so no configuration is required for
standard use, and there is no `--environment` flag.
