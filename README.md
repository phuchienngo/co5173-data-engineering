# Data Pipeline · BTS + Inside Airbnb

This project studies tourism pressure in six US destinations: Las Vegas, Hawaii, New Orleans, Nashville, New York City, and San Francisco. It implements BR9 and BR10 with Kafka, Spark batch and Structured Streaming, ClickHouse, Grafana, and Streamlit.

| Requirement | Business question | Main sources |
|---|---|---|
| BR9 | Which destination has unusual flight arrivals while Airbnb accommodation is close to full? | BTS flights, Inside Airbnb listings and calendars |
| BR10 | What share of guests return to the same destination, and where do they go next? | Inside Airbnb reviews and listings |

## Documentation

- [Implementation plan](docs/00-implementation-plan.md): architecture, logic, data model, and design choices.
- [Dataset assessment](docs/01-dataset-assessment.md): measured coverage, data quality, and limitations.
- [Runbook](docs/02-runbook.md): commands and expected results.
- [Purpose](docs/03-purpose.md): inputs and outputs for each business requirement.

## Project layout

```text
project-root/
├── docker-compose.yml     Nine services
├── docker/                Spark and Streamlit images
├── conf/                  ClickHouse, Prometheus, and Grafana configuration
├── sql/clickhouse/        Tables, views, and quality checks
├── jobs/                  Ingest, profiling, batch, streaming, producer, and shared code
├── app/                   Streamlit application
├── scripts/               Startup, shutdown, job, and quality-check scripts
├── docs/                  Project documentation
└── data/                  Raw files, Parquet lake, checkpoints, and reports
```

`data/` is ignored by Git because it contains downloaded datasets and generated output. After cloning, follow the [runbook](docs/02-runbook.md) to download sources and build the lake and reports locally. Run all commands from the directory containing `docker-compose.yml`.

## Local services

| Service | Address |
|---|---|
| Streamlit | http://localhost:8501 |
| Grafana | http://localhost:3000 |
| Kafka UI | http://localhost:8080 |
| Spark Master | http://localhost:8082 |
| Spark application UI | http://localhost:4040 while a job runs |
| ClickHouse HTTP | http://localhost:8123 |
| Prometheus | http://localhost:9090 |

This stack shares host ports with the original `phuong-an-a` stack, so run one at a time.

Data sources: US Bureau of Transportation Statistics (public-domain data), Inside Airbnb (CC BY 4.0), and OpenFlights (ODbL).
