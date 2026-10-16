# BR5 Spec: Low-Season Promotion Finder

Business context: see [intent.md](../../intent/br5/intent.md). This is the entry point of the spec. Details are split into:

| File | Covers |
|---|---|
| [spec-data-import.md](spec-data-import.md) | Loading the raw data into the data store; the two start-up modes |
| [spec-pipeline-config.md](spec-pipeline-config.md) | The advanced settings screen and the restart guide |
| [spec-monitoring.md](spec-monitoring.md) | Grafana dashboards and run history |

Priorities: **Must** = needed for the demo and the report, **Should** = wanted, **Could** = only if time allows.

## 1. Key value: tell the hotel what promotion to run

> The hotel partner picks **who they want to reach** (hotel, guest country, room type) and the app answers three questions:
> 1. **When** is the low season for this choice? (the *giai đoạn thấp điểm*)
> 2. **What promotion** should we offer, and why?
> 3. **Who and where**: which room types and guest countries are worth targeting, and through which booking channel?

Everything else in this spec (the pipeline, settings, monitoring) exists to produce that answer correctly and to let the team measure how well the pipeline produces it.

Users:
- **Hotel partner / marketing user:** chooses the target, reads the recommendation.
- **Project team (us):** changes the technical settings, re-runs, and compares run times for the report.

## 2. The app at a glance

A small web app (as in the main technology solution) with three screens. Grafana is used only for monitoring and is linked from the app.

1. **Promotion Finder:** choose the target, press **Find promotions**, read the recommendation.
2. **Advanced Settings:** processing and analysis settings for the team (see the config spec).
3. **Data:** whether data is loaded, and start an import (see the import spec).

## 3. Promotion Finder

### 3.1 What the business user chooses

Only three choices, because these are the ones a hotel marketer decides on. Each is a single-choice drop-down with an **All** option.

| ID | Choice | Options | Default | Priority |
|---|---|---|---|---|
| Q1 | **Hotel** | City Hotel, Resort Hotel, All | All | Must |
| Q2 | **Guest country** (target market) | countries with enough bookings, All | All | Must |
| Q3 | **Room type** | room types with enough bookings, All | All | Must |

- Choosing **All** for country or room type means "rank them for me" (see 3.2).
- Options with too little data are not offered (see 3.5), so the user never gets a recommendation based on a handful of bookings.
- Year, month, week and similar choices are **not** asked: the analysis always looks at the whole period in the data. Analysis parameters (such as how low "low" is) are team settings, not business choices (see the config spec).
- The button is disabled while a run is in progress; only one run at a time, with a clear "busy" message.

### 3.2 What the user gets back

| ID | Result | Priority |
|---|---|---|
| R1 | **Low-season periods:** which months are low season for this choice, shown on a month-by-month demand chart against the peak, in plain words (for example "Nov – Feb: demand is 40% of peak") | Must |
| R2 | **Promotion recommendations:** one card per opportunity (see below), ranked by unused potential | Must |
| R3 | **Supporting figures:** the demand and price table behind R1 and R2 | Should |
| R4 | **Run summary:** run time, settings used, bookings read and excluded with reasons (collapsed by default, mostly for the team) | Must |
| R5 | Export of the recommendations as CSV | Could |

**What a recommendation card contains**

| Part | Content |
|---|---|
| Target | Hotel, room type, guest country |
| When | The low-season months to run the promotion |
| Why | Demand in low season vs. its own peak, and the potential: room-nights per month that could be filled if demand reached the peak |
| What | The suggested promotion: offer type(s) and discount level, from the rules in 3.4 |
| Where | The booking channel(s) most used by this target in low season (for example Online travel agent, Direct), where the offer should be shown |
| Evidence | One line per suggestion saying which figure triggered it (for example "guests book 5 days ahead on average") |

**How the choices shape the list**

| User picks | The app shows |
|---|---|
| Country, room type and hotel all chosen | One card for that exact target |
| Country = All | Cards for the best-potential countries (default top 10) for the chosen hotel and room type |
| Room type = All | Cards for the best-potential room types for the chosen country and hotel |
| Everything = All | Cards for the best-potential room type × country combinations, top 10 |

If there is not enough data, show "not enough data to recommend" with the reason, never an empty chart or a guess.

### 3.3 Definitions

- **Demand** = room-nights of bookings that were **not canceled** (stay nights = weekend nights + week nights). **Price** = average ADR (average daily rate) of those bookings.
- **Period** = calendar month. Low-season periods are shown as ranges where months are adjacent (for example "Nov – Feb").
- **Peak** = the month with the highest demand for the chosen target.
- **Low season** = months whose demand is below the low-season threshold (default **60%**) of the peak.
- **Potential** = peak demand − low-season demand, in room-nights per month. This is the ranking measure.
- The data does not cover every month of every year (2015 starts in July, 2017 ends in August), so a month's demand is the **average per year** over the years where that month exists, not a plain sum.
- Guest country is the guest's home country (the data has no hotel location). Countries are shown by name where known, otherwise by code.

### 3.4 How a promotion is suggested (rule-based starting point)

Each rule compares the target's **low-season** bookings with its **peak-season** bookings. All thresholds are team-adjustable defaults (see the config spec).

| Signal in the data | Suggested offer | Default trigger |
|---|---|---|
| Guests book **far ahead** in low season | **Early-bird offer**: book at least N days ahead (N from the typical lead time) | Typical lead time of 30 days or more |
| Guests book **at the last minute** | **Last-minute deal** shown close to arrival | Typical lead time of 14 days or less |
| Low-season stays are **much shorter** than peak stays | **Stay-longer offer** (for example third night free) | At least 25% shorter than peak |
| **Families** are much rarer in low season | **Family package** (kids stay free) | Share of bookings with children less than half of peak |
| Low-season price is **already far below** peak price | **Add value, don't cut the price further**: free breakfast, room upgrade | Already 30% or more below peak price |
| Low-season price is **close to** peak price | **Discount**, with depth from the demand level below | Otherwise |

Discount depth (when a discount is suggested), by how far demand falls below peak:

| Demand vs. peak | Suggested discount |
|---|---|
| Below 40% | Strong: 20–30% |
| 40–50% | Moderate: 10–20% |
| 50–60% | Light: 5–10% |

Notes:
- Several offers can apply to one target. Each card lists all that triggered, each with its evidence line. If none triggers, the card says "no specific offer pattern; consider a general discount at the level above".
- These are **suggestions from booking patterns**, not predictions: the data has no information on how guests react to prices, so the app does not claim revenue results.

### 3.5 Data cleaning and minimum data (applied at every run, counts in R4)

| Rule | Treatment |
|---|---|
| Canceled bookings | Excluded |
| Bookings with zero nights | Excluded |
| Bookings with zero guests | Excluded |
| Missing country | Kept, shown as "Unknown" |
| ADR of 0 or below (e.g. complimentary stays) | Counted in demand, left out of average price |
| Exact duplicate rows (the data has no booking ID) | Kept by default; the team can choose to remove them |
| **Minimum data** | A country or room type is offered only with at least **100** valid bookings in total; a target with less shows "not enough data". On this data, about 33 of 165 countries qualify |

## 4. Non-functional requirements

- **Correctness:** counts in R4 add up (read = used + excluded); the same choices and settings always give the same recommendation.
- **Speed (targets, to be confirmed on the demo laptop):** a run with default settings finishes in about a minute or less, with visible progress.
- **Runs locally** with the existing Docker stack; no login or user accounts.
- **Plain language** for the business user; technical terms only on the Advanced Settings screen, each with a one-line explanation.

## 5. Out of scope

Payment method, province/city, restaurants (the data only covers hotels), demand forecasting, predicting promotion revenue, real-time streaming input, user accounts.

## 6. Acceptance checklist

- [ ] Pick hotel, country and room type, press the button, and get low-season months (R1) and a recommendation card with when / why / what / where (R2).
- [ ] Picking a different country changes the low-season months and the suggested offers.
- [ ] **All** for country or room type returns a ranked list; a target with too little data is handled.
- [ ] Every suggested offer shows the figure that triggered it.
- [ ] Both start-up modes work (see import spec); data survives a stack restart.
- [ ] Changing a processing setting changes the next run, and the run history shows which setting produced which time.
- [ ] Grafana shows run times, resource use and data/result figures (see monitoring spec).
- [ ] The restart guide covers every setting that cannot change live.

## 7. Open decisions (please confirm or change)

1. **Promotion rules and numbers in 3.4** (triggers and discount tiers) are my starting proposal from a look at the data; please adjust to what a hotel marketer would find credible.
2. **Low-season threshold default 60%:** on this data it flags roughly Nov–Feb for both hotels.
3. **Duplicates default:** keep exact duplicates? They are more often canceled bookings, so excluding cancellations already removes many.
4. **Bigger data for benchmarking:** the dataset has only 119k rows, which is small for optimization comparisons. Add the optional "data multiplier" in the import spec?
5. **One Spark run per click vs. a long-running Spark session:** left to the plan; the spec assumes each run starts fresh, so settings apply cleanly.
