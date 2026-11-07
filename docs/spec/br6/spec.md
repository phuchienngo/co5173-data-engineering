# BR6 Spec: Route Stop Finder

Business context: see [intent.md](../../intent/br6/intent.md). This is the entry point of the spec. Details are split into:

| File | Covers |
|---|---|
| [spec-data-import.md](spec-data-import.md) | Loading the raw data into the data store; the two start-up modes |
| [spec-pipeline-config.md](spec-pipeline-config.md) | The advanced settings screen and the restart guide |
| [spec-monitoring.md](spec-monitoring.md) | Grafana dashboards and run history |

Priorities: **Must** = needed for the demo and the report, **Should** = wanted, **Could** = only if time allows.

## 1. Key value: good stops along the way

> The visitor picks a **city**, **where they start and end** (optionally stops in between) and the **kinds of places** they like. The app answers three questions:
> 1. **Which places** lie along this route?
> 2. **Which of them are worth a stop**, and why? (popular with other visitors, and fitting this visitor's own habits)
> 3. **How far off the route** is each one, and where along the route does it come?

Every result is computed from what the user chose. Nothing about the city, the route or the visitor is fixed in advance. The rest of this spec (pipeline, settings, monitoring) exists to produce this answer correctly and to let the team measure how well the pipeline does it.

Users:
- **Visitor (customer):** chooses the route, reads the suggestions.
- **Project team (us):** changes the technical settings, re-runs, compares run times for the report.

## 2. The app at a glance

BR6 has **no web app of its own**. It reuses the BR5 app (left menu: Promotion Finder, Advanced Settings, Data, Run History) and changes it as little as possible, because the BR5 benchmark screenshots were taken from it and must stay valid. Grafana is used only for monitoring and is linked from the app. **There is no map**: results are lists and figures.

| Left menu item | After the change |
|---|---|
| Promotion Finder | Unchanged (BR5) |
| **Route Finder** | **New:** choose the route, press **Find stops**, read the suggestions (section 3) |
| Advanced Settings | Opens a **selection page**: choose *Promotion Finder* (BR5) or *Route Finder* (BR6), then the matching settings (config spec) |
| Data | Same selection page; then the matching data status and import (import spec) |
| Run History | Same selection page; then the matching past runs and two-run comparison (monitoring spec) |

| ID | Requirement | Priority |
|---|---|---|
| G1 | The only new left menu item is **Route Finder**. The other menu items keep their names and order. Screenshots of BR5 taken after this change will show the extra menu item; the user accepts this | Must |
| G2 | The selection page of Advanced Settings, Data and Run History offers two panels, **Promotion Finder** and **Route Finder**. Choosing *Promotion Finder* shows exactly what the menu item shows today, so it looks like the BR5 screenshots in the BR5 result pages (the BR5 monitoring links in the sidebar included) | Must |
| G3 | Choosing *Route Finder* shows the BR6 content of that function, built in the same style as its BR5 counterpart (same layout and wording style, so the two are easy to compare) | Must |
| G4 | Every link that exists today keeps working and opens the **BR5 content directly**, with no selection step: the Grafana tables linking to Run History, the `run` and `a` parameters of the page addresses, and the addresses used by the BR5 benchmark script. BR6 adds its own direct links the same way (for example from its Grafana dashboards) | Must |
| G5 | Each BR keeps its own settings, saved presets, data status and run history; nothing is shared except the app, the Spark cluster and the monitoring stack. A two-run comparison only offers runs of the same BR | Must |
| G6 | The sidebar **Monitoring** block (Grafana dashboards and Spark history) works like the menu: BR5 content shows what it shows today; Route Finder and BR6 content show BR6's three Grafana dashboards and the Spark history link; the selection pages show both, grouped under *Promotion Finder* and *Route Finder*, so the user can pick. Grafana and the Spark history page themselves are also organized per BR (monitoring spec, M9 and M10). If this turns out to be unreasonable, ask the user before changing it | Must |
| G7 | The Spark cluster is shared, so the Route Finder shows "busy" while a BR5 import or run is going on. BR5 screens are not changed for this; the restart guide notes that BR5 and BR6 must not run at the same time while measuring | Should |

## 3. Route Finder

### 3.1 What the user chooses

| ID | Choice | Options | Default | Priority |
|---|---|---|---|---|
| Q1 | **City** | the cities loaded in the data store (up to 15) | none, must pick | Must |
| Q2 | **Start point** | a place of the chosen city, found by typing part of its name | none, must pick | Must |
| Q3 | **End point** | same as Q2; must differ from the start | none, must pick | Must |
| Q4 | **Stops in between** | 0 to 3 places, same search as Q2; the route goes start → stops in the given order → end | none | Should |
| Q5 | **Place types** | one or more of the place groups in 3.4 | all | Must |
| Q6 | **Visitor** | an existing visitor of the chosen city, or **General visitor** | General visitor | Must |

- The place search shows name, category and address (when known), because many places share a name. Only places with a known location within the city limit (3.5) can be picked, of any type (a hotel or a station is a fair start); private homes (*Home (private)*) are not listed.
- The start, the end and the stops are never suggested.
- Choosing a **visitor** makes the ranking personal: the app uses that visitor's own past check-ins (see 3.3). Visitors are anonymous numbers; the list shows how many check-ins each has and their favourite place types. Only visitors with enough history are offered (see 3.5). **General visitor** ranks by popularity and closeness only.
- How far off the route a place may be, how many suggestions to show and how the ranking is weighed are **team settings**, not user choices (see the config spec).
- The button is disabled while a run is in progress; only one run at a time, with a clear "busy" message. A run started from the app can be stopped.

### 3.2 What the user gets back

| ID | Result | Priority |
|---|---|---|
| R1 | **Route summary:** start → stops → end, route length in km, number of places found near the route (any type) and how many remain after the filters (chosen place types, minimum visitors) | Must |
| R2 | **Suggestion list:** one card per place (below), ranked, top 10 by default | Must |
| R3 | **Supporting figures:** the full list of places near the route with their scores, and the number of suggestions per place type | Should |
| R4 | **Run summary:** run time, settings used, check-ins read, used and excluded with reasons (collapsed by default, mostly for the team) | Must |
| R5 | Export of the suggestion list as CSV | Could |

**What a suggestion card contains**

| Part | Content |
|---|---|
| Place | Name, address when known, category and place type |
| Detour | How far the place is from the route, in km |
| Position | How far from the start along the route, and between which two points (for example "3.2 km from the start, between the start and stop 1") |
| Popular because | Number of different visitors and total check-ins at this place in this city |
| Fits you because | Only with a chosen visitor: how often this visitor went there, and the share of the visitor's check-ins in this place type |
| Score | The overall score, with its three parts (popularity, fit, closeness) |

If nothing is found, show "no places within the allowed detour for these choices" and suggest widening the place types; never an empty page. Small cities can have few qualifying places (see 3.5); the city list shows how many.

### 3.3 Definitions

- **Route:** straight lines from the start through the stops to the end. The data has no road network, so the app does not know roads, travel time or traffic; all distances are straight-line distances in km, measured on a flat map of the city around its centre (about 1% off at 100 km from the centre). The app states this on the result page.
- **Detour** of a place = its straight-line distance to the nearest point of the route. A place is a **candidate** if its detour is within the maximum detour (default **1 km**).
- **Position** = distance along the route from the start to the point nearest to the place.
- **Visit** = one check-in. The whole period in the data is used (2012–2013 and 2017–2018); no year or month choice is asked.
- **Popularity** of a place = number of **different visitors** who checked in there (check-ins as tie-breaker). **Frequency** of a visitor at a place = that visitor's check-ins there.
- **Score** has three parts, each from 0 to 1, combined with team weights (default **50% popularity, 30% fit, 20% closeness**):
  - *Popularity:* different visitors, compared with the most popular place on this route that passes the filters.
  - *Fit* (chosen visitor only): the visitor's share of check-ins in the place's type, divided by their share in their favourite type, so the favourite type scores 1 (for example 50% Food & drink, 30% Shopping, 20% Sights & culture give 1.0, 0.6 and 0.4). Only check-ins at offered place types count. Earlier visits to this exact place are shown on the card but don't change the score.
  - *Closeness:* 1 at the route, falling to 0 at the maximum detour.
  - For **General visitor** there is no fit part; it has its own weights (default **70% popularity, 30% closeness**).
- **Order:** score, then different visitors, then check-ins, then place id.
- **City** means the data set of that city. The "venue city" field inside the data (often a suburb) is not used for this.
- Places and visitors are identified together with their city, because the data re-uses the same numbers in different cities.
- Ties are broken by place id (see Order above), so the same choices always give the same list.

### 3.4 Place types

The data has about 760 categories (Shopping Mall, Coffee Shop, Museum, Road, ...). They are grouped into a few **place types**; the grouping is a reference table the team can edit.

| Place type | Examples |
|---|---|
| Sights & culture | museums, monuments, temples, historic sites, plazas, art venues |
| Nature & outdoors | parks, beaches, viewpoints, lakes, trails |
| Food & drink | restaurants, cafés, bakeries, street food |
| Shopping | malls, markets, shops |
| Entertainment & nightlife | cinemas, theatres, bars, clubs, stadiums |
| **Not offered** | homes, offices, schools and universities, roads, bridges, stations, hotels, medical, generic "neighborhood" or "building", and any category not yet grouped |

"Not offered" places are never suggested and do not count towards a visitor's preferences.

### 3.5 Data cleaning and minimum data (applied at every run, counts in R4)

| Rule | Treatment |
|---|---|
| Check-ins without location (13.3% of the data; the place has no coordinates or name) | Excluded, counted |
| Check-ins very far from the city (more than 100 km from the city centre; 53 on this data) | Excluded, counted |
| Check-ins in "Not offered" categories | Not used for suggestions or preferences, counted |
| Duplicates | Not needed: the data has no exact duplicate rows |
| **Minimum data (place)** | A place is suggested only with at least **3 different visitors** |
| **Minimum data (visitor)** | A visitor is offered only with at least **10 used check-ins** in the city (after the rules above, so only offered place types count). On this data: about 10,200 visitors in Istanbul, 19 in Beijing |

A check-in is excluded for the first rule that applies to its place, in the order of the table (no location, far from the city, Not offered), so read = used + excluded holds exactly. A place's type is the type of its main category.

On this data, places with at least 3 visitors range from 37 of 553 in Beijing and about 420 in Palembang to over 10,000 in Jakarta and Petaling Jaya, and 12,700 in New York. With the data multiplier, counts are divided by the multiplier first, so synthetic copies never change the answer.

## 4. Non-functional requirements

- **Correctness:** counts in R4 add up (read = used + excluded); the same choices and settings always give the same list.
- **Speed (targets, to be confirmed on the demo laptop):** a run on the largest city with default settings finishes in about a minute or less, with visible progress.
- **Runs locally** with the existing Docker stack; no login or user accounts.
- **Plain language** for the visitor; technical terms only on the Advanced Settings screen, each with a one-line explanation.

## 5. Out of scope

Real maps or any external map service, road routing and travel time, opening hours, ratings and prices, real-time or live-location input, multi-day trip planning, user accounts, cities outside the 15 in the data (there is no Vietnamese city).

## 6. Acceptance checklist

- [ ] Pick city, start and end (and optionally stops, place types, a visitor), press the button, and get a route summary (R1) and a ranked list with detour, position and reasons (R2).
- [ ] Changing the city, the start or end, or the visitor changes the list. Nothing is fixed in the code.
- [ ] A chosen visitor with a different history gets a different ranking than **General visitor**.
- [ ] A route with no places nearby, and a city with few places, are handled with a clear message.
- [ ] Only one menu item is added (Route Finder); Advanced Settings, Data and Run History open a selection page; *Promotion Finder* shows the same screens as the BR5 screenshots; all existing BR5 links still work.
- [ ] Both start-up modes work (see import spec); data survives a stack restart.
- [ ] Changing a processing setting changes the next run, and the run history shows which setting produced which time.
- [ ] Grafana shows run times, resource use and data/result figures (see monitoring spec).
- [ ] The restart guide covers every setting that cannot change live.

## 7. Open decisions (please confirm or change)

1. **Scoring and defaults in 3.3 and 3.5** (weights 50/30/20, maximum detour 1 km, top 10, at least 3 visitors per place, at least 10 check-ins per visitor) are my starting proposal; please adjust. *Decided so far:* the fit formula (no bonus for an earlier visit), General visitor weights 70/30, and visitor check-ins counted at offered place types only.
2. **Place types in 3.4:** the five groups and what goes into "Not offered" (hotels and stations are left out as stops). The first version of the category table is drafted for review in `data/br6/category_place_type.csv` (summary and choices to check: [plan-data-model.md](../../plan/br6/plan-data-model.md), section 5).
3. **Straight-line routes:** accepted as the price of having no map and no road data.
4. **Visitor choice:** is picking an anonymous visitor from the data an acceptable stand-in for "the customer's history"?
5. ~~Map on the result page~~ **Decided:** no map (list and figures only).
6. ~~Own app for BR6~~ **Decided:** reuse the BR5 app with one new menu item and selection pages (section 2).
7. ~~One Spark run per click~~ **Decided:** each run starts its own Spark application, as in BR5 (15–30 s start-up); see [plan-pipeline.md](../../plan/br6/plan-pipeline.md), F1.

Items 1 and 2 are used as the defaults above and can be changed in Advanced Settings.
