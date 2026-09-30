# 03 · Purpose — inputs and outputs by business requirement

The pipeline combines two main sources: BTS domestic flight records and Inside Airbnb accommodation data. OpenFlights provides airport time zones and coordinates as reference data.

## BR9 · Destination pressure alerts

**Question:** Which destination has unusually many arriving flights while Airbnb accommodation is close to full?

| Layer | Input or output | Purpose |
|---|---|---|
| BTS input | Flight date, origin and destination airports, scheduled and actual times, duration, delays, cancellations, diversions | Derive events and count arrivals per airport and 15-minute window. |
| Airbnb `listings` input | Listing ID, neighbourhood, coordinates, `accommodates`, availability, recent reviews | Estimate guest capacity and identify active listings. |
| Airbnb `calendar` input | Listing ID, night date, `available` status | Estimate unavailable capacity by neighbourhood and night. |
| OpenFlights reference | IATA code, time zone, coordinates | Convert BTS local times to UTC and locate airports. |
| Intermediate output | `data/lake/flights/`, `data/lake/flight_events/` | Normalized flights and events at the 12 monitored airports. |
| ClickHouse output | `baseline_airport_slot`, `fact_airport_flow_15min`, `fact_occupancy_night` | Historical p95, 15-minute flight flow, and nightly accommodation occupancy. |
| Serving output | `v_destination_15min`, `v_destination_live`, `v_destination_alerts`; Grafana and Streamlit | Combined risk level and alert history. |

```text
BTS → br9_02 → lake/flight_events → br9_03 (history) ───┐
                                   → Kafka → stream ──────┼→ ClickHouse views → BR9 dashboard
Airbnb listings + calendar → br9_04 ─────────────────────┘
OpenFlights → airport time zones and coordinates
```

Flight flow compares arrivals with historical p95 for the same airport, local weekday, and 15-minute slot. Occupancy is unavailable guest capacity divided by active guest capacity. BR9 is a **risk indicator**, not a confirmed tourist count or a measure of all accommodation. BTS has no passenger count or trip purpose. Airbnb `available = f` can mean a booking or a host-blocked night. Streaming replays historical BTS data rather than receiving live flights.

## BR10 · Returning guests

**Question:** What share of guests return to the same destination, which cohorts retain guests, and where do guests go next?

| Layer | Input or output | Purpose |
|---|---|---|
| Airbnb `reviews` input | `review_id`, `reviewer_id`, `listing_id`, review date from each destination's latest snapshot | Use one review as a proxy for one stay; link stays by guest. Names and review text are excluded. |
| Airbnb `listings` input | Listing ID, neighbourhood, room type, capacity | Add listing attributes to stays. |
| Intermediate output | `data/lake/stays/` | Stays assigned to trips for each guest. |
| Core ClickHouse output | `fact_stay`, `dim_guest` | Stays and guest-level repeat and RFM measures. |
| Mart output | `mart_br10_city_overview`, `mart_br10_cohort`, `mart_br10_city_flow`, `mart_br10_rfm_segment`, `mart_br10_yearly` | Destination KPIs, cohorts, onward travel, segments, and annual trends. |
| Serving output | Grafana and Streamlit dashboards; guest CSV export | Return rates, cohorts, destination flow, and guest segments. |

```text
Airbnb reviews + listings → br10_01 (stays and trips)
→ br10_02 (guest dimension and RFM) → br10_03 (five marts) → BR10 dashboard
```

Stays by the same guest no more than three days apart form one trip. A guest returns to the same destination if they make at least two trips there. BR10 does not use BTS or OpenFlights. Its rates describe **guests who wrote reviews in the six selected destinations**; guests without reviews and some reviews from removed listings are absent.

## Further reading

- [Implementation plan](00-implementation-plan.md)
- [Dataset assessment](01-dataset-assessment.md)
- [Runbook](02-runbook.md)
