# BR6 Spec: Data Import and Start-up Modes

Part of [spec.md](spec.md).

## 1. What import means

Import loads the raw check-in files (`data/br6/<city>_checkins.csv`, 15 files, about 2.63 million check-ins) into the data store (ClickHouse). The data is loaded **as-is**: no rows are dropped and empty values stay empty. It is stored twice: once exactly as in the files, and once organized for fast analysis (one check-in table plus small lookup tables for places, place categories and place types, cities and visitors). Cleaning happens in each run, so every run can report what it excluded and why.

Runs read from the data store, not from the CSV files. If the "place statistics at import" setting is chosen (config spec, section 3), import also prepares the popularity of every place and the habits of every visitor, and its run-history row records this.

## 2. Two start-up modes

| | **Fresh mode** (for the report) | **Normal mode** (everyday use, demos) |
|---|---|---|
| What it does | Removes existing BR6 data, imports the files, then launches the app and monitoring | Launches the app and monitoring only |
| Data afterwards | Freshly imported | Whatever is already stored |
| Typical use | Clean, repeatable starting point for measurements | Quick start; data is already there |

| ID | Requirement | Priority |
|---|---|---|
| D1 | One command for each mode (for example `make br6-fresh` and `make br6-up`; exact names decided in the plan) | Must |
| D2 | Fresh mode removes **BR6 data only**, never other BRs' data or the monitoring history | Must |
| D3 | Normal mode with no data stored does not fail: the app opens and the Data screen says "no data yet, import now" | Must |
| D3b | Both modes launch the **shared app** (the BR5 app, main spec section 2) together with BR6's monitoring. The BR5 pages stay available, and BR5's own start-up modes keep working as they do today | Must |

## 3. Importing on demand

| ID | Requirement | Priority |
|---|---|---|
| D4 | Import can be started separately by a command (for example `make br6-import`) | Must |
| D5 | Import can also be started with a button from the **Data** screen of Route Finder (menu *Data*, then the *Route Finder* panel; see the main spec, section 2) | Must |
| D6 | If data already exists, import asks for confirmation and then **replaces** it; the old data stays until the new import succeeds | Should |
| D7 | While importing, the screen shows progress; if it fails, the old data is untouched and the error is shown | Must |
| D8 | Only one import or run at a time; the app shows "busy" otherwise | Must |
| D9 | Import can be limited to **chosen cities** (default: all 15), so a quick demo or a smaller measurement does not need the whole data. The city list in the Route Finder shows only the imported cities | Should |

## 4. Keeping the data between launches

| ID | Requirement | Priority |
|---|---|---|
| D10 | Imported data survives stopping and starting the stack (stored in the existing Docker volume). It is only lost by an explicit import/fresh start or by deleting volumes | Must |
| D11 | Existing start scripts clear fact tables on every launch. BR6's data must be **excluded** from that behavior | Must |

## 5. Data screen (status)

Shows: whether data is loaded, which cities, number of check-ins per city, when it was imported, how long the import took, and the files it came from. Buttons: Import / Re-import (with the city choice from D9).

## 6. Optional: data multiplier

| ID | Requirement | Priority |
|---|---|---|
| D12 | Import can repeat the data N times (for example ×1, ×10, ×50) to simulate bigger data, so optimization effects become visible. Each copy gets its own visitors but the same places, so popularity rankings stay the same. Clearly labeled as synthetic; default ×1; the multiplier is shown in the run summary | Could |

## 7. Reference data

| ID | Requirement | Priority |
|---|---|---|
| D13 | The category → place type table (spec 3.4) is kept as a file in the repository that the team can edit, loaded with the data, and applied by every run, so an edit takes effect on the next run without a re-import | Must |
| D14 | A small country-code → country-name list so the app can show the country of each city | Could |
