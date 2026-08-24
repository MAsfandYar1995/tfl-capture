"""
TfL Data Capture → Google Cloud Storage  (v2 — 7 endpoints)
=============================================================
Captures live data from 7 TfL API endpoints every time it runs.
Designed to run every 5 minutes via GitHub Actions cron.

Endpoints captured:
  ALREADY IN V1:
  1.  Line Status          — Tube/DLR/Overground/Elizabeth disruptions
  2.  BikePoint            — Santander Cycles dock availability (800+ stations)
  3.  Road Disruptions     — Active road closures and roadworks
  4.  Air Quality          — London borough air quality index

  NEW IN V2:
  5.  Tube Arrivals        — Live next-train predictions per line
  6.  Bus Arrivals         — Live bus arrival predictions at 50 busiest stops
  7.  Road Traffic Speeds  — Live congestion speeds on TfL Red Routes

Storage pattern:
  Each run writes one JSON file per endpoint to GCS.
  Path: gs://BUCKET/tfl/{endpoint}/{YYYY}/{MM}/{DD}/{endpoint}_{captured_at}.json
  Files are small (KB to a few MB each) and never grow large.
  All rows share the same captured_at timestamp for the entire run.

Environment variables (GitHub Secrets):
  TFL_APP_KEY       — TfL API primary key
  GCS_BUCKET_NAME   — GCS bucket name (no gs:// prefix)
  GCP_CREDENTIALS   — Full GCP service account JSON string
"""

import os
import json
import requests
from datetime import datetime, timezone


# ─── CONFIG ──────────────────────────────────────────────────────────────────

TFL_BASE = "https://api.tfl.gov.uk"

TFL_APP_KEY  = os.environ.get("TFL_APP_KEY", "")
GCS_BUCKET   = os.environ.get("GCS_BUCKET_NAME", "")

# ── Tube lines to capture arrivals for ───────────────────────────────────────
# All currently operating lines. Each call returns next trains on that line.
TUBE_LINES = [
    "bakerloo", "central", "circle", "district",
    "hammersmith-city", "jubilee", "metropolitan",
    "northern", "piccadilly", "victoria",
    "waterloo-city", "elizabeth", "dlr",
    "london-overground"
]

# ── Busiest London bus stops (NaPTAN IDs) ────────────────────────────────────
# Curated list of the 50 busiest bus stops across London — major interchanges,
# transport hubs, and high-footfall locations. Covers all zones and areas.
# Source: TfL passenger count data + known interchange hubs.
# These IDs work directly with /StopPoint/{id}/Arrivals
BUS_STOPS = {
    # Zone 1 — Central London
    "490000173RF": "Oxford Circus (Margaret St)",
    "490000173RA": "Oxford Circus (Regent St)",
    "490000173RG": "Oxford Circus (Argyll St)",
    "490004733S":  "Victoria Station (Terminus Pl)",
    "490004733N":  "Victoria Station (Buckingham Palace Rd)",
    "490000077A":  "Liverpool Street Station",
    "490000077B":  "Liverpool Street (Bishopsgate)",
    "490000152A":  "Paddington Station (Praed St)",
    "490000152B":  "Paddington (London St)",
    "490000083C":  "London Bridge Station (Tooley St)",
    "490000083E":  "London Bridge (Borough High St)",
    "490000072A":  "King's Cross Station (Euston Rd)",
    "490000072B":  "King's Cross (York Way)",
    "490000230VB": "Waterloo Station (York Rd)",
    "490000230VA": "Waterloo (Waterloo Rd)",
    "490000235A":  "Westminster Station (Bridge St)",
    "490000235B":  "Westminster (Parliament Sq)",
    "490000091D":  "Marble Arch",
    "490000070A":  "Holborn Station",
    "490000125A":  "Oxford Street / New Bond St",

    # Zone 1/2 — Inner London hubs
    "490000117A":  "Elephant & Castle Station",
    "490000117B":  "Elephant & Castle (New Kent Rd)",
    "490000060B":  "Hammersmith Bus Station",
    "490000060A":  "Hammersmith (King St)",
    "490000222A":  "Vauxhall Bus Station",
    "490000222B":  "Vauxhall (South Lambeth Rd)",
    "490000121A":  "Old Street Station",
    "490000130A":  "Aldgate Bus Station",
    "490000007A":  "Angel Station (Upper St)",
    "490000009A":  "Bank Station (Queen Victoria St)",

    # Zone 2 — Major hubs
    "490001080S":  "Stratford Bus Station",
    "490001080N":  "Stratford (Great Eastern Rd)",
    "490000096B":  "Mile End Station",
    "490000102A":  "New Cross Gate Station",
    "490000158A":  "Peckham Bus Station",
    "490000174A":  "Brixton Station (Brixton Rd)",
    "490000174B":  "Brixton (Effra Rd)",
    "490000200A":  "Tooting Broadway Station",
    "490000052A":  "Clapham Junction Station",
    "490000052B":  "Clapham Junction (St John's Hill)",

    # Zone 2/3 — Outer hubs
    "490000135A":  "Lewisham Bus Station",
    "490000019A":  "Barking Station",
    "490000041A":  "Camden Town Station (Camden High St)",
    "490000041B":  "Camden Town (Buck St)",
    "490000103A":  "North Greenwich Bus Station",
    "490000160A":  "Putney Bridge Station",
    "490000168B":  "Richmond Bus Station",
    "490000197A":  "Shepherd's Bush (Uxbridge Rd)",
    "490000197B":  "Shepherd's Bush (Goldhawk Rd)",

    # Zone 3+ — Outer London
    "490000218A":  "Uxbridge Bus Station",
    "490000016A":  "Croydon (George St)",
}

# ── TfL-managed roads (Red Routes) to capture speed data for ─────────────────
# Red Routes are the 5% of London roads that carry 30% of traffic.
# TfL manages these directly and publishes speed data for them.
ROAD_IDS = [
    "A1", "A2", "A3", "A4", "A5",
    "A10", "A11", "A12", "A13", "A20",
    "A21", "A23", "A24", "A30", "A40",
    "A41", "A316", "A406",  # North Circular
    "A205",                  # South Circular
]


# ─── GCS SETUP ───────────────────────────────────────────────────────────────

def get_gcs_bucket():
    """
    Authenticates to GCS using the service account JSON stored in
    GCP_CREDENTIALS environment variable (GitHub Secret).
    Returns the bucket object or None if authentication fails.
    """
    try:
        from google.cloud import storage
        from google.oauth2 import service_account

        creds_json = os.environ.get("GCP_CREDENTIALS", "")
        if not creds_json:
            print("  ERROR: GCP_CREDENTIALS not set")
            return None

        creds_dict  = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = storage.Client(
            project=creds_dict.get("project_id"),
            credentials=credentials
        )
        return client.bucket(GCS_BUCKET)

    except json.JSONDecodeError:
        print("  ERROR: GCP_CREDENTIALS is not valid JSON")
        return None
    except Exception as e:
        print(f"  ERROR connecting to GCS: {e}")
        return None


def upload(bucket, rows: list, endpoint_name: str, captured_at: str):
    """
    Uploads a list of rows as a single JSON file to GCS.

    File path: tfl/{endpoint}/{year}/{month}/{day}/{endpoint}_{captured_at}.json
    File contents: { endpoint, captured_at, row_count, rows: [...] }

    One file per endpoint per run. Small, self-describing, date-partitioned.
    Never grows large. Easy to COPY INTO Snowflake later by date range.
    """
    if not bucket:
        return

    try:
        dt    = datetime.strptime(captured_at, "%Y-%m-%dT%H:%M:%SZ")
        path  = (
            f"tfl/{endpoint_name}/"
            f"{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/"
            f"{endpoint_name}_{captured_at}.json"
        )
        payload = {
            "endpoint":    endpoint_name,
            "captured_at": captured_at,
            "row_count":   len(rows),
            "rows":        rows,
        }
        bucket.blob(path).upload_from_string(
            json.dumps(payload, indent=2, ensure_ascii=False),
            content_type="application/json"
        )
        print(f"  ✓ {len(rows):>6,} rows  →  gs://{GCS_BUCKET}/{path}")

    except Exception as e:
        print(f"  ERROR uploading {endpoint_name}: {e}")


# ─── TfL API HELPER ──────────────────────────────────────────────────────────

def tfl_get(endpoint: str, params: dict = {}) -> list | dict | None:
    """
    Makes a GET request to the TfL Unified API.
    Always appends the API key. Returns parsed JSON or None on failure.
    Handles timeouts and HTTP errors gracefully — one bad call won't
    crash the whole run.
    """
    url    = f"{TFL_BASE}{endpoint}"
    params = {**params, **({"app_key": TFL_APP_KEY} if TFL_APP_KEY else {})}

    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.Timeout:
        print(f"    TIMEOUT: {endpoint}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"    HTTP {e.response.status_code}: {endpoint}")
        return None
    except Exception as e:
        print(f"    ERROR: {endpoint} — {e}")
        return None


# ─── CAPTURE FUNCTIONS ───────────────────────────────────────────────────────

def capture_line_status(bucket, captured_at: str):
    """
    STATUS of all Tube, DLR, Overground, and Elizabeth line services.

    severity scale: 10 = Good Service · 6 = Minor Delays · 5 = Severe Delays
                    4 = Part Closure · 0 = Suspended · 20 = Special Service
    Lower = worse (except 20). 10 = perfect normal operation.

    Each line can have multiple simultaneous statuses (e.g. good service
    on most of the line + part closure on one branch). We flatten them
    so each status entry is its own row.

    ~15–25 rows per call.
    """
    print("  Line status...")
    modes = "tube,dlr,overground,elizabeth-line"
    data  = tfl_get(f"/Line/Mode/{modes}/Status")
    if not data:
        return

    rows = []
    for line in data:
        line_id, line_name, mode = (
            line.get("id", ""),
            line.get("name", ""),
            line.get("modeName", ""),
        )
        for status in line.get("lineStatuses", [{}]):
            dis      = status.get("disruption") or {}
            validity = (status.get("validityPeriods") or [{}])[0]
            rows.append({
                "captured_at":          captured_at,
                "line_id":              line_id,
                "line_name":            line_name,
                "mode_name":            mode,
                "status_severity":      status.get("statusSeverity"),
                "status_description":   status.get("statusSeverityDescription", ""),
                "reason":               status.get("reason", ""),
                "disruption_category":  dis.get("categoryDescription", ""),
                "disruption_type":      dis.get("category", ""),
                "affected_routes":      ", ".join(r.get("name","") for r in dis.get("affectedRoutes",[])),
                "affected_stops":       ", ".join(s.get("name","") for s in dis.get("affectedStops",[])),
                "valid_from":           validity.get("fromDate", ""),
                "valid_to":             validity.get("toDate", ""),
            })
    upload(bucket, rows, "line_status", captured_at)


def capture_bikepoints(bucket, captured_at: str):
    """
    REAL-TIME AVAILABILITY at all 800+ Santander Cycles docking stations.

    Key fields:
      bikes_available  — bikes ready to hire right now
      docks_available  — empty docks to return a bike
      occupancy_pct    — bikes / total_docks * 100 (derived)
      e_bikes_available — electric bikes specifically

    ~800 rows per call. Highest volume endpoint (~200MB/month in GCS).
    """
    print("  BikePoints...")
    data = tfl_get("/BikePoint")
    if not data:
        return

    rows = []
    for station in data:
        props = {p["key"]: p["value"] for p in station.get("additionalProperties", [])}
        try:
            bikes = int(props.get("NbBikes", 0))
            total = int(props.get("NbDocks", 0))
            occ   = round(bikes / total * 100, 1) if total > 0 else None
        except (ValueError, TypeError, ZeroDivisionError):
            occ = None

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
            "occupancy_pct":     occ,
            "is_installed":      props.get("Installed"),
            "is_locked":         props.get("Locked"),
            "is_temporary":      props.get("Temporary"),
        })
    upload(bucket, rows, "bikepoints", captured_at)


def capture_road_disruptions(bucket, captured_at: str):
    """
    ACTIVE ROAD DISRUPTIONS on the TfL road network.

    Disruptions have their own lifecycle — they persist across multiple
    captures. The disruption_id links the same disruption across runs,
    letting you calculate true duration:
      MAX(captured_at) - MIN(captured_at) WHERE disruption_id = X

    When there are no disruptions (common overnight), we write a sentinel
    row so we know the capture ran successfully with zero active disruptions.

    Typically 10–500 rows per call depending on time of day.
    """
    print("  Road disruptions...")
    data = tfl_get("/Road/all/Disruption")

    if not data:
        rows = [{"captured_at": captured_at, "disruption_id": "NO_ACTIVE_DISRUPTIONS",
                 "category": "", "sub_category": "", "description": "",
                 "location": "", "lat": None, "lng": None,
                 "severity": "", "is_blocking": None,
                 "streets_affected": "", "start_date": "", "end_date": ""}]
        upload(bucket, rows, "road_disruptions", captured_at)
        return

    rows = []
    for d in data:
        # GeoJSON coordinates are [lng, lat] — note the reversal
        coords = (d.get("geography") or {}).get("coordinates", [])
        lat = coords[1] if len(coords) >= 2 else None
        lng = coords[0] if len(coords) >= 2 else None

        rows.append({
            "captured_at":      captured_at,
            "disruption_id":    d.get("id", ""),
            "category":         d.get("category", ""),
            "sub_category":     d.get("subCategory", ""),
            "description":      (d.get("description") or "").replace("\n", " "),
            "location":         d.get("location", ""),
            "lat":              lat,
            "lng":              lng,
            "severity":         d.get("severity", ""),
            "is_blocking":      d.get("isBlocking"),
            "streets_affected": ", ".join(s.get("name","") for s in (d.get("streets") or []) if s.get("name")),
            "start_date":       d.get("startDate", ""),
            "end_date":         d.get("endDate", ""),
        })
    upload(bucket, rows, "road_disruptions", captured_at)


def capture_air_quality(bucket, captured_at: str):
    """
    LONDON AIR QUALITY FORECAST from TfL.

    Returns today's and tomorrow's forecast. Updated daily by TfL.
    Bands: Low / Moderate / High / Very High (UK DAQI standard).

    Analytical use: correlate air quality bands with BikePoint hire rates.
    Hypothesis: High/Very High air quality suppresses cycling demand.

    Only 2 rows per call (today + tomorrow).
    """
    print("  Air quality...")
    data = tfl_get("/AirQuality")
    if not data:
        return

    rows = []
    for f in data.get("currentForecast", []):
        rows.append({
            "captured_at":      captured_at,
            "forecast_type":    f.get("forecastBand", ""),
            "forecast_summary": (f.get("forecastSummary") or "").replace("\n", " "),
            "forecast_text":    (f.get("forecastText") or "").replace("\n", " "),
            "no2_band":         f.get("nO2Band", ""),
            "o3_band":          f.get("o3Band", ""),
            "pm10_band":        f.get("pM10Band", ""),
            "pm25_band":        f.get("pM25Band", ""),
            "so2_band":         f.get("sO2Band", ""),
        })
    upload(bucket, rows, "air_quality", captured_at)


def capture_tube_arrivals(bucket, captured_at: str):
    """
    ── NEW IN V2 ──
    LIVE NEXT-TRAIN PREDICTIONS for every Tube, DLR, Overground,
    and Elizabeth line — one call per line.

    This is more granular than line_status:
      - line_status tells you WHETHER there's a problem
      - tube_arrivals tells you actual train frequency and wait times

    Key fields:
      line_id           — e.g. 'central', 'jubilee'
      vehicle_id        — unique train ID (tracks individual trains)
      station_name      — where the train is predicted to arrive
      destination_name  — where the train is heading
      expected_arrival  — predicted arrival time (ISO8601)
      time_to_station   — seconds until arrival (TfL's live estimate)
      current_location  — plain text location of train right now
      direction         — 'inbound' or 'outbound'

    Analytical uses:
      - Calculate actual headway (gap between trains) per line per hour
      - Compare scheduled vs actual frequency
      - Detect knock-on effects of disruptions on train spacing
      - Peak vs off-peak frequency analysis

    ~100–500 rows per line per call. Total: ~3,000–7,000 rows per run.
    We loop through all 14 lines — each is one separate API call.
    If one line fails, we continue with the others.
    """
    print("  Tube arrivals (all lines)...")
    all_rows = []

    for line_id in TUBE_LINES:
        data = tfl_get(f"/Line/{line_id}/Arrivals")
        if not data:
            continue  # skip this line if API call failed, try next line

        for train in data:
            all_rows.append({
                "captured_at":      captured_at,
                "line_id":          line_id,
                "vehicle_id":       train.get("vehicleId", ""),
                "naptan_id":        train.get("naptanId", ""),
                "station_name":     train.get("stationName", ""),
                "platform_name":    train.get("platformName", ""),
                "destination_id":   train.get("destinationNaptanId", ""),
                "destination_name": train.get("destinationName", ""),
                "direction":        train.get("direction", ""),
                "current_location": train.get("currentLocation", ""),
                "expected_arrival": train.get("expectedArrival", ""),
                "time_to_station":  train.get("timeToStation"),    # seconds
                "timing_source":    train.get("timing", {}).get("source", "") if train.get("timing") else "",
            })

    print(f"    → {len(all_rows):,} arrival predictions across {len(TUBE_LINES)} lines")
    upload(bucket, all_rows, "tube_arrivals", captured_at)


def capture_bus_arrivals(bucket, captured_at: str):
    """
    ── NEW IN V2 ──
    LIVE BUS ARRIVAL PREDICTIONS at the 50 busiest London bus stops.

    Why 50 stops and not all 19,600?
      Capturing all stops every 5 min = 19,600 API calls per run.
      At 500 requests/min (TfL free tier limit) that takes 39 minutes per run.
      Completely impossible at 5-minute intervals.

      50 stops = 50 API calls per run = ~6 seconds total. Perfectly feasible.
      These 50 stops cover major interchanges, transport hubs, and high-volume
      locations across all zones — a representative sample of London bus demand.

    Key fields:
      stop_id           — NaPTAN ID of the bus stop
      stop_name         — human-readable stop name (from our lookup dict)
      line_id           — bus route number e.g. '25', '73', '38'
      vehicle_id        — unique bus ID (tracks individual buses)
      destination_name  — where the bus is going
      expected_arrival  — predicted arrival time (ISO8601)
      time_to_station   — seconds until bus arrives at this stop
      towards           — direction description e.g. 'Oxford Circus'

    Analytical uses:
      - Bus punctuality: is the bus on time vs scheduled?
      - Frequency analysis: how many buses per hour per route?
      - Peak crowding proxy: many buses waiting = high demand stop
      - Compare bus reliability across different areas of London

    ~5–30 rows per stop per call. Total: ~500–1,500 rows per run.
    Each stop is one API call. We continue if any individual stop fails.
    """
    print(f"  Bus arrivals ({len(BUS_STOPS)} stops)...")
    all_rows      = []
    failed_stops  = 0

    for stop_id, stop_name in BUS_STOPS.items():
        data = tfl_get(f"/StopPoint/{stop_id}/Arrivals")
        if data is None:
            failed_stops += 1
            continue  # skip this stop if call failed

        for bus in data:
            all_rows.append({
                "captured_at":      captured_at,
                "stop_id":          stop_id,
                "stop_name":        stop_name,
                "line_id":          bus.get("lineName", ""),        # route number e.g. '73'
                "line_name":        bus.get("lineId", ""),          # e.g. '73' (same as lineName for buses)
                "vehicle_id":       bus.get("vehicleId", ""),
                "destination_name": bus.get("destinationName", ""),
                "towards":          bus.get("towards", ""),
                "direction":        bus.get("direction", ""),
                "expected_arrival": bus.get("expectedArrival", ""),
                "time_to_station":  bus.get("timeToStation"),       # seconds
                "timing_source":    bus.get("timing", {}).get("source", "") if bus.get("timing") else "",
                "operator":         bus.get("operatorName", ""),
            })

    if failed_stops > 0:
        print(f"    → {failed_stops} stops failed (API errors) — skipped")
    print(f"    → {len(all_rows):,} bus arrival predictions")
    upload(bucket, all_rows, "bus_arrivals", captured_at)


def capture_road_speeds(bucket, captured_at: str):
    """
    ── NEW IN V2 ──
    LIVE TRAFFIC SPEEDS on TfL-managed roads (Red Routes).

    Red Routes are the ~5% of London roads that carry ~30% of all traffic.
    TfL manages them directly and publishes live speed and flow data.

    Why this matters:
      - Congestion severity: is the road flowing freely or blocked?
      - Correlate with road disruptions: does a nearby incident slow traffic?
      - Time-of-day patterns: rush hour vs off-peak speed profiles
      - Weather impact: does rain slow traffic on specific corridors?

    Key fields:
      road_id           — TfL road ID e.g. 'A1', 'A40', 'A406'
      link_id           — specific road segment ID
      point_id          — measurement point on the road
      speed             — current speed in mph
      flow              — vehicles per hour (traffic volume)
      travelTime        — journey time in minutes for this segment

    Note: Not all road IDs return speed data — some only have disruption data.
    We capture whatever the API returns and skip silently if nothing comes back.

    ~10–50 rows per road per call. Total: ~200–1,000 rows per run.
    """
    print(f"  Road traffic speeds ({len(ROAD_IDS)} roads)...")
    all_rows = []

    for road_id in ROAD_IDS:
        # Try the speed endpoint — not all roads have this data
        data = tfl_get(f"/Road/{road_id}/Speed")
        if not data:
            continue

        # Speed data can come back as a dict or a list depending on the road
        if isinstance(data, dict):
            data = [data]

        for segment in data:
            all_rows.append({
                "captured_at":   captured_at,
                "road_id":       road_id,
                "link_id":       segment.get("linkId", ""),
                "point_id":      segment.get("pointId", ""),
                "speed":         segment.get("speed"),          # mph
                "flow":          segment.get("flow"),           # vehicles/hour
                "travel_time":   segment.get("travelTime"),     # minutes
                "road_name":     segment.get("roadName", ""),
                "direction":     segment.get("direction", ""),
            })

    print(f"    → {len(all_rows):,} speed measurements")
    if all_rows:
        upload(bucket, all_rows, "road_speeds", captured_at)
    else:
        # Road speed endpoint can be unreliable — don't upload empty file
        print("    → No speed data returned (endpoint may be intermittent)")


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    # ── Single timestamp for the ENTIRE run ──────────────────────────────────
    # This is the most important line in the script.
    # Every row from every endpoint in this run shares this exact captured_at.
    # This is what lets you join line_status + bikepoints + tube_arrivals
    # + bus_arrivals + road_speeds from the same 5-minute window in Snowflake.
    # Without this, you can't know which observations are contemporaneous.
    captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"\n{'='*60}")
    print(f"TfL Capture v2  →  GCS")
    print(f"Timestamp : {captured_at}")
    print(f"Bucket    : gs://{GCS_BUCKET}")
    print(f"Endpoints : 7  (4 original + 3 new)")
    print(f"{'='*60}\n")

    # ── Validate environment variables ───────────────────────────────────────
    missing = [
        v for v in ["TFL_APP_KEY", "GCS_BUCKET_NAME", "GCP_CREDENTIALS"]
        if not os.environ.get(v)
    ]
    if missing:
        print(f"ERROR: Missing GitHub Secrets: {', '.join(missing)}")
        raise SystemExit(1)

    # ── Authenticate to GCS once, reuse for all uploads ─────────────────────
    bucket = get_gcs_bucket()
    if not bucket:
        print("ERROR: Could not connect to GCS. Check GCP_CREDENTIALS secret.")
        raise SystemExit(1)

    # ── Run all 7 capture functions ──────────────────────────────────────────
    # Original 4
    capture_line_status(bucket, captured_at)
    capture_bikepoints(bucket, captured_at)
    capture_road_disruptions(bucket, captured_at)
    capture_air_quality(bucket, captured_at)

    # New 3
    capture_tube_arrivals(bucket, captured_at)
    capture_bus_arrivals(bucket, captured_at)
    capture_road_speeds(bucket, captured_at)

    print(f"\n{'='*60}")
    print(f"✓ Complete — {captured_at}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
