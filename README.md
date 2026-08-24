# TfL Live Data Capture → Google Cloud Storage

Automated pipeline that captures live TfL API data every 5 minutes
and writes JSON files to a GCS bucket. Runs entirely on GitHub Actions — free.

## What gets captured

| Endpoint | GCS path | Every | ~Files/day | ~Size/month |
|---|---|---|---|---|
| Line status | `tfl/line_status/YYYY/MM/DD/` | 5 min | 288 | ~5 MB |
| BikePoints (800+ stations) | `tfl/bikepoints/YYYY/MM/DD/` | 5 min | 288 | ~200 MB |
| Road disruptions | `tfl/road_disruptions/YYYY/MM/DD/` | 5 min | 288 | ~10 MB |
| Air quality | `tfl/air_quality/YYYY/MM/DD/` | 5 min | 288 | ~2 MB |

All rows include `captured_at` UTC timestamp (ISO 8601).

## Tech stack
- **Capture**: Python + `requests` + `google-cloud-storage`
- **Scheduling**: GitHub Actions cron (free, public repo)
- **Storage**: Google Cloud Storage (5GB free forever)
- **Future**: Snowflake + dbt + Power BI (see `snowflake_load.sql`)
