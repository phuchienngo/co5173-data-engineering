# Documentation index

One folder per business requirement. Each BR doc explains the requirement, the pipeline, and how to run it.

| BR | Owner | Doc | Dashboard | Run command |
|---|---|---|---|---|
| BR1 Tour route optimization | Tiến | [br1/tour-route.md](br1/tour-route.md) (diagram: [br1/architecture.svg](br1/architecture.svg)); [result/br1/result-benchmark.md](result/br1/result-benchmark.md); [report (HTML)](result/br1-br2-benchmark-report.html); [report notes (VI)](result/br1-br2-report-notes.md) | Grafana uid `br1-tour` at http://localhost:3000/d/br1-tour; business queries: uid `br1-finder` at http://localhost:3000/d/br1-finder | `make demo-br BR=1` |
| BR2 Dynamic pricing | Tiến | [br2/dynamic-pricing.md](br2/dynamic-pricing.md); [result/br2/result-benchmark.md](result/br2/result-benchmark.md); [report (HTML)](result/br1-br2-benchmark-report.html); [report notes (VI)](result/br1-br2-report-notes.md); [result/br2/result-stream-benchmark.md](result/br2/result-stream-benchmark.md) (stream: Spark vs ClickHouse MV) | Grafana uid `br2-pricing` at http://localhost:3000/d/br2-pricing; business queries: uid `br2-advisor` at http://localhost:3000/d/br2-advisor | `bash scripts/download_tlc.sh yellow 2025-07 2026-07`, then `make demo-br BR=2` |
| BR3 Sentiment analysis | Chiến | [spec/br3/spec.md](spec/br3/spec.md), [plan/br3/plan-pipeline.md](plan/br3/plan-pipeline.md), [result/br3/result-benchmark.md](result/br3/result-benchmark.md) (code: `jobs/br3_sentiment_analysis/`) | Grafana uid `br3-data-result` at http://localhost:3000/d/br3-data-result | `make demo-br BR=3` |
| BR4 Revenue monitoring | Chiến | [spec/br4/spec.md](spec/br4/spec.md), [plan/br4/plan-pipeline.md](plan/br4/plan-pipeline.md), [result/br4/result-benchmark.md](result/br4/result-benchmark.md) (code: `jobs/br4_revenue_monitoring/`) | Grafana uid `br4-data-result` at http://localhost:3000/d/br4-data-result | `make demo-br BR=4` |
