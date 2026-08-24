"""
TfL Data Capture → Google Cloud Storage
=========================================
Captures live data from 4 TfL API endpoints and writes each
snapshot as a JSON file to a GCS bucket.

Why JSON files instead of CSV appending?
  - Each run writes ONE small file per endpoint (e.g. line_status_2025-03-15T14:32:00Z.json)
  - Files never grow large — no 100MB GitHub limit problem
  - GCS is designed for this pattern (object storage = files, not databases)
  - When ready to load into Snowflake, one COPY INTO command reads all files at once
  - If one run fails, you lose only that run's file — not a corrupted CSV

File naming pattern:
  gs://your-bucket/tfl/line_status/2025/03/15/line_status_2025-03-15T14:32:00Z.json
  gs://your-bucket/tfl/bikepoints/2025/03/15/bikepoints_2025-03-15T14:32:00Z.json
  gs://your-bucket/tfl/road_disruptions/2025/03/15/road_disruptions_2025-03-15T14:32:00Z.json
  gs://your-bucket/tfl/air_quality/2025/03/15/air_quality_2025-03-15T14:32:00Z.json

The date-partitioned folder structure (year/month/day) means when you load
into Snowflake later, you can load one day or one month at a time cleanly.

Environment variables required (set as GitHub Secrets):
  TFL_APP_KEY        — your TfL API key
  GCS_BUCKET_NAME    — your GCS bucket name (without gs:// prefix)
  GCP_CREDENTIALS    — your GCP service account JSON key (full JSON string)
"""

import os
import json
import requests
import tempfile
from datetime import datetime, timezone
from pathlib import Path


# ─── CONFIG ──────────────────────────────────────────────────────────────────

TFL_BASE_URL   = "https://api.tfl.gov.uk"
TFL_APP_KEY    = os.environ.get("TFL_APP_KEY", "")
GCS_BUCKET     = os.environ.get("GCS_BUCKET_NAME", "")

# ─── GCS SETUP ───────────────────────────────────────────────────────────────

def get_gcs_client():
    """
    Authenticates to Google Cloud Storage using a service account key.

    The service account JSON is stored as a GitHub Secret (GCP_CREDENTIALS).
    We write it to a temp file because the GCS library expects a file path,
    not a raw string.

    Returns a GCS client and the bucket object, or (None, None) if it fails.
    """
    try:
        from google.cloud import storage
        from google.oauth2 import service_account

        creds_json = os.environ.get("GCP_CREDENTIALS", "")
        if not creds_json:
            print("  ERROR: GCP_CREDENTIALS environment variable not set")
            return None, None

        # Parse the JSON credentials
        creds_dict = json.loads(creds_json)

        # Build credentials object directly from dict (no temp file needed)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        client = storage.Client(
            project=creds_dict.get("project_id"),
            credentials=credentials
        )
        bucket = client.bucket(GCS_BUCKET)
        return client, bucket

    except json.JSONDecodeError:
        print("  ERROR: GCP_CREDENTIALS is not valid JSON")
        return None, None
    except Exception as e:
        print(f"  ERROR setting up GCS client: {e}")
        return None, None


def upload_to_gcs(bucket, data: list | dict, endpoint_name: str, captured_at: str):
    """
    Uploads a JSON file to GCS.

    File path structure:
      tfl/{endpoint_name}/{year}/{month}/{day}/{endpoint_name}_{captured_at}.json

    Example:
      tfl/line_status/2025/03/15/line_status_2025-03-15T14:32:00Z.json

    Each file contains a JSON array of rows, all sharing the same captured_at.
    This structure means:
      - You can list all files for a given day easily
      - Snowflake can load by date partition (COPY INTO for just March data etc.)
      - Individual files are small (a few KB to a few MB each)
      - Failed runs = missing files, not corrupted data
    """
    if not bucket:
        return False

    try:
        # Parse the timestamp to build folder path
        # captured_at format: 2025-03-15T14:32:00Z
        dt = datetime.strptime(captured_at, "%Y-%m-%dT%H:%M:%SZ")
        year  = dt.strftime("%Y")
        month = dt.strftime("%m")
        day   = dt.strftime("%d")

        # Build the GCS object path (this becomes the "file path" inside the bucket)
        gcs_path = f"tfl/{endpoint_name}/{year}/{month}/{day}/{endpoint_name}_{captured_at}.json"

        # Wrap the data in an envelope with metadata
        # This makes the file self-describing — you know what it is just by reading it
        payload = {
            "endpoint":    endpoint_name,
            "captured_at": captured_at,
            "row_count":   len(data) if isinstance(data, list) else 1,
            "rows":        data if isinstance(data, list) else [data]
        }

        # Upload as JSON string
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(
            data=json.dumps(payload, indent=2, ensure_ascii=False),
            content_type="application/json"
        )

        row_count = payload["row_count"]
        print(f"  ✓ Uploaded {row_count} rows → gs://{GCS_BUCKET}/{gcs_path}")
        return True

    except Exception as e:
        print(f"  ERROR uploading to GCS: {e}")
        return False


# ─── TfL API HELPERS ─────────────────────────────────────────────────────────

def get_timestamp() -> str:
    """
    Returns current UTC time as ISO 8601 string.
    e.g. 2025-03-15T14:32:07Z
    Used as captured_at on every single row — this is how you reconstruct
    when each snapshot was taken when loading into Snowflake later.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def call_tfl(endpoint: str, params: dict = {}) -> list | dict | None:
    """
    Makes a GET request to the TfL Unified API.
    Returns parsed JSON or None if the request fails.
    Handles timeouts, HTTP errors, and JSON decode errors gracefully
    so one bad API response doesn't crash the whole run.
    """
    url = f"{TFL_BASE_URL}{endpoint}"
    if TFL_APP_KEY:
        params = {**params, "app_key": TFL_APP_KEY}

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.Timeout:
        print(f"  TIMEOUT calling {endpoint}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"  HTTP {e.response.status_code} from {endpoint}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  REQUEST ERROR {endpoint}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"  JSON DECODE ERROR from {endpoint}")
        return None


# ─── CAPTURE FUNCTIONS ───────────────────────────────────────────────────────

def capture_line_status(bucket, captured_at: str):
    """
    Captures current status of all Tube, DLR, Overground, and Elizabeth line services.

    Each line can have multiple concurrent status entries — for example the
    Central line might show 'Good Service' on most of the line but 'Part
    Closure' between two stations. We flatten these so each status entry
    becomes its own row, linked by captured_at and line_id.

    Key field — status_severity (integer):
      10  = Good Service  (normal operation)
      6   = Minor Delays
      5   = Severe Delays
      4   = Part Closure
      3   = Part Suspended
      0   = Suspended     (full line closure)
      20  = Special Service (e.g. planned engineering weekend)
    Lower number = worse. 10 = perfect. This is the core KPI for line performance.

    Frequency: every 5 minutes
    Typical rows per call: 15–25 (one per status entry per line)
    """
    print("Capturing line status...")

    modes = "tube,dlr,overground,elizabeth-line"
    data  = call_tfl(f"/Line/Mode/{modes}/Status")
    if not data:
        return

    rows = []
    for line in data:
        line_id   = line.get("id", "")
        line_name = line.get("name", "")
        mode_name = line.get("modeName", "")

        for status in line.get("lineStatuses", [{}]):
            disruption = status.get("disruption") or {}

            # validityPeriods tells us when this status started and when it
            # is expected to end — useful for calculating disruption duration
            validity   = (status.get("validityPeriods") or [{}])[0]

            rows.append({
                "captured_at":          captured_at,
                "line_id":              line_id,
                "line_name":            line_name,
                "mode_name":            mode_name,
                "status_severity":      status.get("statusSeverity"),
                "status_description":   status.get("statusSeverityDescription", ""),
                "reason":               status.get("reason", ""),
                "disruption_category":  disruption.get("categoryDescription", ""),
                "disruption_type":      disruption.get("category", ""),
                "affected_routes":      ", ".join(
                                            r.get("name", "")
                                            for r in disruption.get("affectedRoutes", [])
                                        ),
                "affected_stops":       ", ".join(
                                            s.get("name", "")
                                            for s in disruption.get("affectedStops", [])
                                        ),
                "valid_from":           validity.get("fromDate", ""),
                "valid_to":             validity.get("toDate", ""),
            })

    upload_to_gcs(bucket, rows, "line_status", captured_at)


def capture_bikepoints(bucket, captured_at: str):
    """
    Captures real-time availability at all 800+ Santander Cycles docking stations.

    This is the highest-volume endpoint — ~800 rows every 5 minutes.
    Each row = one station snapshot at one moment in time.

    The additionalProperties array in the TfL response contains the actual
    availability numbers. It's a list of {key, value} dicts — we convert it
    to a lookup dict for clean access.

    Key analytical fields:
      bikes_available  — how many bikes can be hired right now
      docks_available  — how many docks are free to return a bike
      total_docks      — full capacity of this station
      occupancy_pct    — bikes_available / total_docks * 100 (we calculate this)

    Frequency: every 5 minutes
    Typical rows per call: ~800 stations
    """
    print("Capturing BikePoints...")

    data = call_tfl("/BikePoint")
    if not data:
        return

    rows = []
    for station in data:
        props = {
            p["key"]: p["value"]
            for p in station.get("additionalProperties", [])
        }

        # Calculate occupancy percentage — useful derived metric
        # (what fraction of the station is occupied by bikes)
        try:
            bikes = int(props.get("NbBikes", 0))
            total = int(props.get("NbDocks", 0))
            occupancy_pct = round((bikes / total * 100), 1) if total > 0 else None
        except (ValueError, TypeError, ZeroDivisionError):
            occupancy_pct = None

        rows.append({
            "captured_at":       captured_at,
            "station_id":        station.get("id", ""),
            "station_name":      station.get("commonName", ""),
            "lat":               station.get("lat"),
            "lng":               station.get("lon"),
            "bikes_available":   props.get("NbBikes"),
            "e_bikes_available": props.get("NbEBikes"),
            "docks_available":   props.get("NbEmptyDocks"),
            "total_docks":       props.get("NbDocks"),
            "occupancy_pct":     occupancy_pct,
            "is_installed":      props.get("Installed"),
            "is_locked":         props.get("Locked"),
            "is_temporary":      props.get("Temporary"),
        })

    upload_to_gcs(bucket, rows, "bikepoints", captured_at)


def capture_road_disruptions(bucket, captured_at: str):
    """
    Captures active road disruptions across the TfL road network.

    Unlike line status (which is always a full snapshot of current state),
    disruptions have their own lifecycle — they start, persist across multiple
    captures, and eventually end. A disruption lasting 3 days will appear in
    ~864 captures (every 5 min for 3 days).

    This lets you do two types of analysis:
      1. Point-in-time: how many disruptions were active at 8am on a Monday?
      2. Duration: how long did this specific disruption last?
         (max(captured_at) - min(captured_at) where disruption_id = X)

    The disruption_id field is the link between captures — same disruption
    across multiple runs will have the same ID.

    Frequency: every 5 minutes
    Typical rows per call: 10–500 (varies enormously by time of day/week)
    """
    print("Capturing road disruptions...")

    data = call_tfl("/Road/all/Disruption")

    # No disruptions is a valid and common state (especially overnight)
    # We still write a file so we know the capture ran — just with a
    # sentinel row so the absence of disruptions is recorded
    if not data:
        rows = [{
            "captured_at":       captured_at,
            "disruption_id":     "NO_ACTIVE_DISRUPTIONS",
            "category":          "",
            "sub_category":      "",
            "description":       "",
            "location":          "",
            "lat":               None,
            "lng":               None,
            "severity":          "",
            "is_blocking":       None,
            "streets_affected":  "",
            "start_date":        "",
            "end_date":          "",
            "last_modified":     "",
        }]
        upload_to_gcs(bucket, rows, "road_disruptions", captured_at)
        return

    rows = []
    for d in data:
        # Extract coordinates if available
        lat, lng = None, None
        coords   = d.get("geography", {}) or {}
        if coords.get("type") == "Point":
            coords_list = coords.get("coordinates", [])
            if len(coords_list) >= 2:
                lng, lat = coords_list[0], coords_list[1]  # GeoJSON is [lng, lat]

        streets = d.get("streets", []) or []
        street_names = ", ".join(
            s.get("name", "") for s in streets if s.get("name")
        )

        rows.append({
            "captured_at":      captured_at,
            "disruption_id":    d.get("id", ""),
            "category":         d.get("category", ""),
            "sub_category":     d.get("subCategory", ""),
            "description":      (d.get("description", "") or "").replace("\n", " "),
            "location":         d.get("location", ""),
            "lat":              lat,
            "lng":              lng,
            "severity":         d.get("severity", ""),
            "is_blocking":      d.get("isBlocking"),
            "streets_affected": street_names,
            "start_date":       d.get("startDate", ""),
            "end_date":         d.get("endDate", ""),
            "last_modified":    d.get("lastModified", ""),
        })

    upload_to_gcs(bucket, rows, "road_disruptions", captured_at)


def capture_air_quality(bucket, captured_at: str):
    """
    Captures London air quality index from TfL.

    TfL publishes a daily air quality forecast — today's and tomorrow's
    expected air quality across London. The forecast is updated daily so
    capturing it every 5 minutes is redundant — but because the overall
    script runs every 5 minutes, this runs with it. The files will be
    largely identical within the same day, which is fine — deduplicate
    in dbt later using DATE(captured_at).

    The band values are: Low / Moderate / High / Very High
    These map to the UK Daily Air Quality Index (DAQI) standard.

    Key analytical use: correlate air quality with BikePoint hire rates.
    Hypothesis: bad air quality (High/Very High) reduces bike hire demand.

    Frequency: every 5 minutes (but data only changes daily)
    Typical rows per call: 2 (today + tomorrow)
    """
    print("Capturing air quality...")

    data = call_tfl("/AirQuality")
    if not data:
        return

    rows = []
    for forecast in data.get("currentForecast", []):
        rows.append({
            "captured_at":      captured_at,
            "forecast_type":    forecast.get("forecastBand", ""),    # 'today' or 'tomorrow'
            "forecast_summary": (forecast.get("forecastSummary", "") or "").replace("\n", " "),
            "forecast_text":    (forecast.get("forecastText", "") or "").replace("\n", " "),
            "no2_band":         forecast.get("nO2Band", ""),
            "o3_band":          forecast.get("o3Band", ""),
            "pm10_band":        forecast.get("pM10Band", ""),
            "pm25_band":        forecast.get("pM25Band", ""),
            "so2_band":         forecast.get("sO2Band", ""),
        })

    upload_to_gcs(bucket, rows, "air_quality", captured_at)


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    # Single timestamp for the entire run
    # Every row written in this run shares this exact captured_at value
    # This is the glue that lets you join line_status + bikepoints + disruptions
    # from the same 5-minute window when analysing in Snowflake later
    captured_at = get_timestamp()

    print(f"\n{'='*55}")
    print(f"TfL Capture Run  →  GCS")
    print(f"Timestamp : {captured_at}")
    print(f"Bucket    : gs://{GCS_BUCKET}")
    print(f"{'='*55}")

    # Validate environment before doing anything
    missing = []
    if not TFL_APP_KEY:   missing.append("TFL_APP_KEY")
    if not GCS_BUCKET:    missing.append("GCS_BUCKET_NAME")
    if not os.environ.get("GCP_CREDENTIALS"): missing.append("GCP_CREDENTIALS")

    if missing:
        print(f"\n  ERROR: Missing environment variables: {', '.join(missing)}")
        print("  These must be set as GitHub Secrets.")
        raise SystemExit(1)

    # Authenticate to GCS once — reuse the same client for all four uploads
    _, bucket = get_gcs_client()
    if not bucket:
        print("\n  ERROR: Could not connect to GCS. Check GCP_CREDENTIALS.")
        raise SystemExit(1)

    # Run all four capture functions
    capture_line_status(bucket, captured_at)
    capture_bikepoints(bucket, captured_at)
    capture_road_disruptions(bucket, captured_at)
    capture_air_quality(bucket, captured_at)

    print(f"\n✓ All done — {captured_at}\n")


if __name__ == "__main__":
    main()
