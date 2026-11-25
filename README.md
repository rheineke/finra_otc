# finra_otc

Capture FINRA OTC market data as raw files, then build Parquet tables from them.

The data is kept in two layers. The **raw** layer holds every response
exactly as FINRA sent it, plus a manifest. The **Parquet** layer is derived
from the raw layer and can be rebuilt from it at any time, for example after a
parser fix. FINRA's API keeps only rolling windows of most datasets, so the raw
layer also becomes the only copy of history that FINRA has since dropped.

## Datasets

### FINRA Query API, group `otcMarket` (`capture api`)

Public; no credentials needed. Row counts are from October 2026.

| Dataset | What | Partitions | History | Rows |
|---|---|---|---|---|
| `weeklySummary` | ATS and non-ATS (OTC) weekly volume by firm and symbol, plus totals (OTC Transparency) | week × tier (T1, T2, OTCE, NA, NMS) | from 2021-12-06 to now | 58M |
| `weeklySummaryHistoric` | The same data in a separate store: a rolling 4 years, ending a year back | week × tier | 2021-10-25 to 2025-10-20 | about 80M (estimated) |
| `monthlySummary` | Non-ATS (OTC) monthly volume by firm and symbol | month × tier (NMS, OTCE) | from 2014-05 | 5.8M |
| `blocksSummary` | ATS block-trade statistics by ATS and size bucket | month | from 2016-06 | 24K |
| `otcBlocksSummary` | Non-ATS (OTC) block-trade statistics by firm and size bucket | month | from 2016-06 | 108K |
| `weeklyDownloadDetails`, `monthlyDownloadDetails`, `ATSBlocksDownloadDetails`, `OTCBlocksDownloadDetails` | When FINRA last updated each weekly, monthly and blocks report | none | all | under 6K each |
| `regShoDaily` | Reg SHO daily short-sale volume by symbol and reporting facility | trade date | rolling year | 6.9M |
| `thresholdList` | Reg SHO and Rule 4320 threshold securities | trade date | from 2016 | 61K |
| `consolidatedShortInterest` | Short interest in exchange-listed and OTC securities, twice a month | settlement date | from 2017-12 | 4.0M |
| `equityShortInterest` | OTC short interest, the older dataset | settlement date | 2016-06 to 2022-09 | 1.3M |
| `otcDailyList` | OTC daily list: symbol and name changes, corporate actions, dividends | calendar day | from 2016-01 | 169K |

The two weekly datasets overlap from 2021-12-06 to 2025-10-20 (829 week × tier
partitions), so don't union them: use `weeklySummary`, and
`weeklySummaryHistoric` only for weeks before 2021-12-06 (and for weeks after
FINRA drops them from `weeklySummary`, which so far it hasn't).

The API pages synchronous queries at 5,000 rows, and offsets stop at 500,000,
so each partition is captured on its own. A partition's pages are saved
together, and only after every page has arrived and been checked: the same
header on each page, the expected number of rows, and the same `record-total`
throughout (it changes when a republication mid-capture changes the row count;
one that keeps the count is caught by the next run's republish check). A
partition that comes back empty after holding rows isn't saved: that's
reported as a failure, so a passing glitch at FINRA can't blank out history
(`--accept-empty` saves it when FINRA really emptied it). A partitions answer
that lists nothing for a partitioned dataset is a failure too, not a quiet run
with nothing to do. Pages come back as CSV, about 2.5 times smaller than JSON.

### FINRA CDN, Reg SHO files (`capture regsho-daily`, `capture regsho-monthly`)

| Source | What | History | Size |
|---|---|---|---|
| `regsho_daily` | Daily short-sale volume files, one per trade date and facility: CNMS (consolidated NMS), FNQC (Nasdaq TRF Chicago), FNSQ (Nasdaq TRF Carteret), FNYX (NYSE TRF), FNRA (ADF) and FORF (ORF) | from 2009-08 (CNMS and FNQC from 2018-08) | about 0.5 MB a file |
| `regsho_monthly` | Monthly short-sale transaction files: every short sale, one ZIP per month and facility. FNSQ months come in parts (`_1` to `_4`). | from 2009-08 | 222 GB of ZIPs in all (1,123 files, October 2026), about 3 GB a month now; FNSQ is 197 GB of it |

The API's `regShoDaily` keeps a year; the daily files go back to 2009. The CDN
has no index, so the tool tries every weekday (and month) and records misses
in `absent.jsonl`. A miss is a 403 carrying the storage bucket's
`AccessDenied` error; any other refusal counts as a failure, so a bot block
can't mark files missing. A daily file missing on a trading day (a date the
API's `regShoDaily` lists) is also reported as a failure, so a CDN that stops
serving files makes the run fail instead of going quiet. That list comes from
the API capture, so schedule `capture all` (or `capture api regShoDaily`
too); `capture regsho-daily` warns when the list is missing or over 5 days
old. A miss isn't asked for again once the file is overdue (a
week for daily files, 45 days after month end for monthly ones), unless you
pass `--recheck-absent`. Recent files (the last 10 days, the last 2 months) are
checked again by ETag, because FINRA sometimes reposts them.

### Old OTC Transparency site, 2014 to 2018 (`capture legacy`)

The original `retrieve.py` scraped weekly ATS and OTC files from
otctransparency.finra.org grids. That site is gone, and the API's weekly
history only goes back to late 2021, so the files it saved are the only copy of
that history. `capture legacy <folder>` imports them (a folder of
`<year>/*.csv` files) into the raw store as-is.

### Not covered

- FINRA publishes the short-interest files (`cdn.finra.org/equity/otcmarket/biweekly/`)
  with the same columns as `consolidatedShortInterest`, so they aren't captured separately.
- The ADF and ORF trade-reporting APIs aren't part of the FINRA API platform.
- No source covers weekly OTC Transparency data from mid-2018 to late 2021.

## Using the tables

Several tables repeat the same volume at different levels, so a plain `sum`
overcounts:

- **`weeklySummary`, `weeklySummaryHistoric` and `monthlySummary`.** Each
  week (or month) and tier holds three levels, told apart by
  `summaryTypeCode`: by symbol and firm (`ATS_W_SMBL_FIRM`,
  `OTC_W_SMBL_FIRM`; `OTC_M_*` monthly), by symbol (`*_SMBL`) and by firm
  (`*_FIRM`). Each level adds up to the same total (to within rounding), so
  pick one. `ATS_*` rows are ATS volume and `OTC_*` rows non-ATS volume; those
  don't overlap.
- **The tiers.** Weekly tiers T1, T2 and OTCE don't overlap. The weekly NMS
  tier (17 weeks in 2023, and the 2022 historic weeks) is T1 and T2 combined,
  so don't add it to them. The NA tier holds market-wide totals
  (`*_W_VOL_STATS`, by `productTypeCode`), not firms. Monthly data has just
  two tiers, NMS and OTCE, which don't overlap.
- **The two weekly datasets** overlap; see above.
- **`blocksSummary` and `otcBlocksSummary`.** There is one row per firm and
  size bucket (`summaryTypeCode`). The firm's overall columns
  (`totalShareQuantity`, `totalTradeCount` and the rest) repeat in every
  bucket's row; only the block columns differ.
- **Reg SHO daily volume.** `CNMS` is the consolidated file: exactly the sum of
  FNQC, FNSQ and FNYX (and the ADF), so add up either CNMS or the others,
  not both. `api/regShoDaily` holds the same numbers as the CDN files for the
  last year, with its own facility codes: `NCTRF` = FNQC, `NQTRF` = FNSQ,
  `NYTRF` = FNYX and `ORF` = FORF. It has no CNMS or ADF rows. Use
  `regsho_daily` for history and avoid unioning the two.
- **Short interest.** `equityShortInterest` (OTC securities, to 2022-09)
  overlaps the OTC rows of `consolidatedShortInterest` from 2017-12 to
  2022-09.
- **The legacy weekly files** (2014-2018) don't overlap the API's weeks.

## Layout

```
data/raw/<source>/<key>/<captured_at>/<file>[.gz]   files exactly as received
data/raw/<source>/manifest.jsonl                    one line per complete capture
data/raw/<source>/absent.jsonl                      CDN files found missing
data/raw/<source>/checks.jsonl                      API re-fetches that found no change
data/parquet/api/<dataset>/<partition>.parquet
data/parquet/cdn/regsho_daily/<facility>/<YYYY-MM>.parquet
data/parquet/cdn/regsho_monthly/<facility>sh<YYYYMM>[_<part>].parquet
data/parquet/legacy/weekly/<YYYY>.parquet
```

Sources are `api/<dataset>`, `api_catalog/<dataset>` (the dataset's metadata
and partition list, saved when they change), `cdn/regsho_daily`,
`cdn/regsho_monthly` and `legacy/weekly`.

Text is gzipped on disk, and ZIPs are stored as-is. Nothing is overwritten:
when a partition or file changes, the new version is saved next to the old one,
and the manifest says which is newest. Files are written and synced to disk
(F_FULLFSYNC on macOS) first and the manifest line last, so an interrupted
capture leaves nothing half-recorded, and a record torn by a crash mid-append
is dropped at the next append. Each capture command deletes unfinished folders
and downloads more than a day old. Finished folders that no manifest line
names (a crash just before the manifest append, or a manifest restored from
an older backup) are never touched; `status` lists them. Each is a complete
capture and keeps a copy of its manifest line (`.capture.json`), so
`finra-otc status --recover` checks its files against it and records it
again. Captures can run alongside each other, since manifest appends are
locked; a `build` started while another runs on the same data folder exits
with status 1. Reads check every file against the SHA-256 in the manifest. A damaged file is
replaced the next time the same data is fetched: on any run for recent data,
or with `--refresh-since` for older data (monthly ZIPs are only size-checked
at capture; the build verifies their hash). Daily CDN files are checked
against their row-count footer before they're saved.

Each Parquet file is built from the newest capture of its inputs and records a
fingerprint of them and of its builder's version, so `build` only redoes what
changed (`--force` rebuilds everything, say after a pyarrow upgrade). Read a
table as a directory:

```python
import pyarrow.parquet as pq

weekly = pq.read_table("data/parquet/api/weeklySummary")
```

Every file of an API dataset has the same columns, every field in any saved
version of its metadata, so reading the directory loses no column when FINRA
adds one. A field whose type changes fails the dataset's build. Types follow
the metadata: dates become `date32` (timestamps for `otcDailyList`, whose dates carry times),
and numbers become `int64` for counts, ranks, IDs and CRD numbers, otherwise
`float64` (FINRA reports fractional shares; float64 keeps about 15 significant
digits). Empty values become null, and so does the NUL or space padding in old
monthly files' `LinkIndicator`. Values are checked as they are converted: a
fractional count, a date with a time of day, an infinite number or text in a
number column fails the build of that file instead of being changed. When a
file can't be built, its old Parquet file is removed and the error reported,
so a table never mixes in data the raw store has since replaced.

## Usage

```shell
export FINRA_OTC_DATA=/path/to/data       # default: ./data (set it under cron)

finra-otc capture all                     # every API dataset, then regsho-daily
finra-otc capture api weeklySummary --since 2026-01-01   # and its details
finra-otc capture api regShoDaily --refresh-since 2026-10-01   # fetch again
finra-otc capture regsho-daily --since 2009-08-03
finra-otc capture regsho-monthly --since 2026-01 --dry-run     # sizes only
finra-otc capture regsho-monthly --since 2026-01
finra-otc capture legacy ~/projects/personal/finra_otc/data
finra-otc build                           # api, regsho-daily, regsho-monthly, legacy
finra-otc build --dataset weeklySummary   # just that API dataset
finra-otc status                          # keys, captures and MB per source
```

Rerunning a capture is cheap: it fetches partitions not captured yet, ones
that FINRA republished, and recent partitions of the daily datasets, and it
saves only what changed. A partition counts as republished when the
`*DownloadDetails` datasets date its last update on or after the day it was
last fetched. Partitions with no details rows (monthly summaries before 2017,
OTC blocks before late 2019, the oldest historic weeks) are never fetched
again unless you ask with `--refresh-since`. The check tolerates FINRA
updating the details a day before the data; a longer lag would be missed until
the next republication or a `--refresh-since`. A full first API
capture is a few GB and takes hours. `weeklySummaryHistoric` is the slow part,
at about 3 seconds a page; `--workers` (default 4) sets how many partitions
are fetched at once, and Ctrl-C stops them at their next page or retry wait. A failed partition, file or dataset (including dropped
connections, after retries) is reported and skipped, and the command exits
with status 1. `build` also prints warnings (even with `-q`) for things it
leaves out without failing: unreadable metadata versions, and partitions
captured under an older partitioning, whose rows would duplicate the new
keys'.

## Setup

```zsh
conda env create -f environment.yaml
conda activate finra-otc
pre-commit install
```

or

```shell
pip install -e ".[dev,test]"
```

## Tests

```shell
pytest
```

CI runs ruff (lint and format), mypy, codespell and the tests. pre-commit runs
the same checks except the tests.
