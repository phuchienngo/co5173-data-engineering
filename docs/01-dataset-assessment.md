# 01 · Dataset assessment

This assessment records measurements made on the downloaded data on 17 September 2026. Reproduce the source profile with `./scripts/run_job.sh profile`; its detailed output is in `data/reports/profile_sources.md`. The source data supports the selected BR9 and BR10 definitions, subject to the limitations below.

## 1. Coverage checklist

| Requirement | Measured evidence |
|---|---|
| Flights with minute-level times | 7,043,316 BTS flights across 12 months; all 12 monitored airports present. |
| Airport time zones | All 358 airports in the selected BTS scope have time zones after three documented overrides. |
| Historical baseline | 11 months before the replay month, roughly 48 observations for each weekday/slot. |
| Accommodation capacity | `accommodates` present in all 12 selected listing snapshots. |
| Nightly calendars | Roughly 1.5–8.6 million calendar rows per snapshot. |
| Time overlap for BR9 | March 2026 Airbnb snapshots cover BTS months March–June 2026. Measured flight windows from April onward joined to occupancy in the original smoke test. |
| Guest identity for BR10 | 4,100,637 distinct reviewers across the six destinations. |
| Repeat and multi-destination guests | 448,949 reviewers with at least two trips to the same destination; 189,230 reviewers across at least two selected destinations. |

## 2. BTS On-Time Performance

The 12 downloaded monthly CSV files cover July 2025 through June 2026. June 2026 is the replay month in the original setup; earlier months are the baseline history.

| Month | Flights | Cancelled | Arrivals delayed at least 15 minutes |
|---|---:|---:|---:|
| 2025-07 | 631,428 | 2.45% | 28.89% |
| 2025-08 | 602,378 | 1.08% | 22.56% |
| 2025-09 | 562,439 | 0.51% | 16.63% |
| 2025-10 | 605,844 | 0.53% | 20.32% |
| 2025-11 | 570,550 | 2.49% | 20.56% |
| 2025-12 | 582,304 | 1.55% | 26.77% |
| 2026-01 | 544,003 | 4.71% | 20.78% |
| 2026-02 | 515,037 | 2.25% | 19.76% |
| 2026-03 | 612,102 | 2.90% | 24.32% |
| 2026-04 | 597,919 | 0.90% | 20.20% |
| 2026-05 | 611,735 | 0.92% | 21.43% |
| 2026-06 | 607,577 | 1.65% | 25.71% |
| **Total** | **7,043,316** | | |

### Monitored airports

| Airport | Destination | Arrivals/day | Active 15-minute windows/year |
|---|---|---:|---:|
| LAS | Las Vegas | 477.7 | 27,610 |
| SFO | San Francisco | 402.2 | 26,892 |
| LGA | New York City | 373.6 | 24,078 |
| EWR | New York City | 338.0 | 28,396 |
| BNA | Nashville | 298.1 | 26,186 |
| JFK | New York City | 281.4 | 27,394 |
| HNL | Hawaii | 159.4 | 21,839 |
| MSY | New Orleans | 135.3 | 21,591 |
| OGG | Hawaii | 70.2 | 15,598 |
| KOA | Hawaii | 43.9 | 11,721 |
| LIH | Hawaii | 43.1 | 12,294 |
| ITO | Hawaii | 18.2 | 5,997 |

JFK, LGA and EWR share BTS CityMarketID 31703, supporting their grouping as New York City. Hawaii's smaller airports have sparse 15-minute windows; the city serving view aggregates all five airports. The baseline denominator has a five-flight floor to reduce false alarms in sparse slots.

### Time conversion validation

OpenFlights lacked valid time zones for three small BTS airports (BIH, EAR and XWA), affecting 230 relevant flights; explicit overrides in `jobs/common/sources.py` cover them. Among 941,790 arrivals checked, 941,735 derived arrival times matched the original BTS `ArrTime` to the minute (99.99%). Another 54,429 arrivals fell on a different local date from `FlightDate`, showing why the departure date cannot simply be paired with arrival clock time.

The source contained 3,712 rows with `ArrTime = 2400`, four with `CRSArrTime = 2400`, two missing scheduled elapsed time, and one non-cancelled, non-diverted flight with missing arrival delay. The pipeline derives arrival from departure and elapsed time, and drops events whose derived timestamp is null.

## 3. Inside Airbnb

The project uses two listing/calendar snapshots for each destination, in March and June 2026. Reviews and neighbourhood GeoJSON are downloaded from the later snapshot only.

| Destination | March listings | June listings | March active guest capacity | June active guest capacity |
|---|---:|---:|---:|---:|
| Las Vegas / Clark County | 9,049 | 20,234 | 44,344 | 63,425 |
| Hawaii | 29,695 | 38,156 | 95,382 | 110,531 |
| New Orleans | 6,218 | 7,189 | 23,261 | 22,949 |
| Nashville | 6,030 | 10,242 | 41,988 | 57,235 |
| New York City | 35,979 | 30,259 | 29,298 | 29,316 |
| San Francisco | 7,528 | 7,332 | 13,762 | 13,842 |

An active listing has `availability_365 > 0`, `number_of_reviews_ltm > 0`, and `accommodates > 0`. The large Las Vegas listing-count jump reflects a change in Inside Airbnb collection coverage, not demonstrated market growth. Do not compare raw listing totals across snapshots as a market trend.

### Calendar horizon

Calendars look forward from their snapshot date. In five of twelve snapshots, nights at least 270 days ahead were marked unavailable more often than nights 30–89 days ahead. Distant blocked dates may reflect host choices rather than reservations. BR9 therefore prefers snapshots where a night is within 90 days of collection; a fallback within 120 days is flagged as lower confidence.

March snapshots cover nights in April–June 2026, overlapping BTS. June snapshots provide fresher calendar status for later June nights. For each destination and night, the ClickHouse view chooses one suitable snapshot rather than adding snapshots together.

### Reviews

| Destination | Reviews | Distinct reviewers | Review range | Reviewers with at least two trips to the same destination |
|---|---:|---:|---|---:|
| Las Vegas | 726,693 | 619,565 | 2011-06 to 2026-07 | 62,515 |
| Hawaii | 1,584,339 | 1,221,435 | 2010-02 to 2026-07 | 190,046 |
| New Orleans | 511,822 | 459,218 | 2010-04 to 2026-06 | 32,969 |
| Nashville | 838,603 | 750,529 | 2009-04 to 2026-07 | 55,824 |
| New York City | 990,170 | 870,793 | 2009-05 to 2026-06 | 74,635 |
| San Francisco | 438,299 | 381,247 | 2009-05 to 2026-06 | 32,960 |
| **Total** | **5,089,926** | | | |

Reviewer IDs recur across destinations. Among 4,100,637 distinct reviewers, 3,911,407 appear in one selected destination and 189,230 appear in at least two. This supports the BR10 onward-destination analysis.

## 4. Interpretation limits

| Limitation | Pipeline response | Required interpretation |
|---|---|---|
| BTS has no passenger count or trip purpose | Count arriving flights against an airport-specific historical baseline. | Flight load is not tourist headcount. |
| BTS times are local and `FlightDate` is the departure date | Derive UTC time from origin local departure and elapsed duration. | The conversion was checked against reported arrival times. |
| `available = f` includes host-blocked nights | Filter likely active listings and prefer calendars within 90 nights. | Unavailable share is an upper bound on booked share. |
| Airbnb omits hotels | Report Airbnb occupancy only. | A high value does not imply an entire city has no rooms. |
| Airbnb listing coverage changes between snapshots | Choose one snapshot per night. | Do not infer capacity growth from snapshot totals. |
| Reviews cover only reviewers and currently observable listings | Start cohorts in 2022 and exclude the final two months. | Return rates apply only to observed reviewers. |
| BTS is published with a delay | Replay the latest available month through Kafka. | The stream is a simulation, not a live flight feed. |

Data attribution: US BTS, Inside Airbnb (CC BY 4.0), and OpenFlights (ODbL).
