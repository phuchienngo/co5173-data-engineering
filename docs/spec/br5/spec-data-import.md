# BR5 Spec: Data Import and Start-up Modes

Part of [spec.md](spec.md).

## 1. What import means

Import loads the raw booking file (`data/br5/hotel_bookings.csv`) into the data store (ClickHouse). The data is loaded **as-is** (only the texts "NULL" and "NA" are turned into real empty values); no rows are dropped. It is stored twice: once exactly as in the file, and once organized as a star schema (one booking table plus small lookup tables for dates, hotels, countries, room types, channels and booking profiles) for fast analysis. Cleaning happens in each run, so every run can report what it excluded and why.

Runs read from the data store, not from the CSV file.

## 2. Two start-up modes

| | **Fresh mode** (for the report) | **Normal mode** (everyday use, demos) |
|---|---|---|
| What it does | Removes existing BR5 data, imports the file, then launches the app and monitoring | Launches the app and monitoring only |
| Data afterwards | Freshly imported | Whatever is already stored |
| Typical use | Clean, repeatable starting point for measurements | Quick start; data is already there |

| ID | Requirement | Priority |
|---|---|---|
| D1 | One command for each mode (for example `make br5-fresh` and `make br5-up`; exact names decided in the plan) | Must |
| D2 | Fresh mode removes **BR5 data only**, never other BRs' data or the monitoring history | Must |
| D3 | Normal mode with no data stored does not fail: the app opens and the Data screen says "no data yet, import now" | Must |

## 3. Importing on demand

| ID | Requirement | Priority |
|---|---|---|
| D4 | Import can be started separately by a command (for example `make br5-import`) | Must |
| D5 | Import can also be started from the **Data** screen with a button | Must |
| D6 | If data already exists, import asks for confirmation and then **replaces** it; the old data stays until the new import succeeds | Should |
| D7 | While importing, the screen shows progress; if it fails, the old data is untouched and the error is shown | Must |
| D8 | Only one import or run at a time; the app shows "busy" otherwise | Must |

## 4. Keeping the data between launches

| ID | Requirement | Priority |
|---|---|---|
| D9 | Imported data survives stopping and starting the stack (stored in the existing Docker volume). It is only lost by an explicit import/fresh start or by deleting volumes | Must |
| D10 | Existing start scripts clear fact tables on every launch. BR5's data must be **excluded** from that behavior | Must |

## 5. Data screen (status)

Shows: whether data is loaded, number of rows, when it was imported, how long the import took, and the file it came from. Buttons: Import / Re-import.

## 6. Optional: data multiplier

| ID | Requirement | Priority |
|---|---|---|
| D11 | Import can repeat the dataset N times (for example ×1, ×10, ×50) to simulate bigger data, so optimization effects become visible. Clearly labeled as synthetic; default ×1; the multiplier is shown in the run summary | Could |

## 7. Reference data

| ID | Requirement | Priority |
|---|---|---|
| D12 | A small country-code → country-name list is loaded with the data so the app can show names | Should |
