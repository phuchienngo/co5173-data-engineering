# BR3 Spec: Customer Sentiment Analysis

Business context: see [intent.md](../../intent/br3/intent.md).

Priorities: **Must** = needed for the demo and the report, **Should** = wanted, **Could** = only if time allows.

## 1. Key value: show what guests complain about, and where

> A property manager sees, in near real time, how guests feel about each listing and area, and which words come up most in complaints.

Users:
- **Property manager / marketing user:** reads the Grafana dashboard.
- **Project team (us):** compares the main pipeline with the alternative and reports the figures.

## 2. What the user sees (Grafana dashboard `br3-data-result`)

| # | Item | Priority |
|---|---|---|
| R1 | Reviews processed and share of positive reviews | Must |
| R2 | Sentiment distribution (positive, neutral, negative, non_english) | Must |
| R3 | Reviews by room type, and reviews over time | Should |
| R4 | Neighbourhoods ranked by sentiment | Must |
| R5 | Most frequent complaint keywords (from negative reviews) | Must |

## 3. Data

| Source | Content | Size |
|---|---|---|
| `data/br3/reviews.csv.gz` | Inside Airbnb Bangkok reviews (snapshot 2026-06-29): `listing_id`, `id`, `comments` | 693,647 reviews |
| `data/br3/listings.csv` | Inside Airbnb Bangkok listings (dimension) | 31,069 listings |

Reviews arrive as a stream (Kafka topic `reviews`). Listings are static.

## 4. Definitions

- **Sentiment label:** VADER compound score ≥ 0.05 is positive, ≤ −0.05 is negative, otherwise neutral.
- **non_english:** VADER only scores English. A review whose share of English stop words is below 20% gets `non_english` and is not scored.
- **Keywords:** lowercase words of 3+ letters (`[a-z]{3,}`) minus Spark's English stop words.
- **Valid review:** its `listing_id` exists in `dim_airbnb_listing`; others are dropped by the inner join.

## 5. Non-functional requirements

- **Correctness:** the main and the alternative pipeline give the same keywords and the same English/non-English decision for the same review.
- **Speed (measured in the benchmark):** backlog drain time, throughput, latency under a steady stream, memory and CPU.
- **Runs locally** on the existing Docker stack.

## 6. Out of scope

Aspect-based sentiment, translating non-English reviews, topic models, a trained model, word-cloud rendering.

## 7. Acceptance checklist

- [ ] `make demo-br BR=3` fills `fact_reviews`, and the dashboard shows R1–R5.
- [ ] A restart does not duplicate rows (`fact_reviews` is truncated before each run).
- [ ] The benchmark compares the main solution (Spark) with the alternative (ClickHouse Materialized View) on correctness, backlog and steady-stream performance ([plan-benchmark.md](../../plan/br3/plan-benchmark.md)).

## 8. Open decisions

1. The alternative solution has no sentiment label: ClickHouse cannot run VADER in SQL. The comparison covers keywords and language only for that part.
