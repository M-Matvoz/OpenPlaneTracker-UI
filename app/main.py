import asyncio
import pandas as pd
import requests
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from datetime import datetime, timedelta
import os
import uvicorn
import uuid
from contextlib import asynccontextmanager
from FlightRadar24 import FlightRadar24API
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import math
import numpy as np

INFLUX_URL = "http://influxdb:8086"
try:
    with open("/config/influx_token.txt", "r") as f:
        INFLUX_TOKEN = f.read().strip()
except FileNotFoundError:
    INFLUX_TOKEN = "XOiL-HBxFhIUARmvKdkt_PGF2yImUU31SSn-Zd74q4k"
INFLUX_ORG = "planetracker"
INFLUX_BUCKET = "flights"

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

SENTINEL_URL = os.getenv("OPT_SENTINEL_URL", "http://sentinel:8001/api/sdrs")
# New collated endpoint (served by OpenPlaneTracker-server)
# Default collated URL should point to the server container on the Docker network
# Use OPT_COLLATED_URL to override if needed
COLLATED_URL = os.getenv("OPT_COLLATED_URL", "http://openplanetracker-server:8080/api/collated")
DB_URI = "postgresql://postgres:postgrespw@db:5432/planetracker"
engine = create_engine(DB_URI)

db_ready = False
live_history = pd.DataFrame()
flight_routes_cache = {}
flight_uuids = {}
fr_api = FlightRadar24API()


def _ensure_live_history_schema():
    """Guarantee the cache has the columns we later query against."""
    global live_history
    required_columns = ["timestamp", "flight", "hex", "lat", "lon"]
    for col in required_columns:
        if col not in live_history.columns:
            live_history[col] = pd.Series(dtype="object")


def _normalize_aircraft_payload(payload: dict) -> pd.DataFrame:
    """Convert an ADSB/collated payload into a safe dataframe.

    The ADSB container may omit `flight` for many aircraft and provides a top-level
    `now` field rather than per-row timestamps. This helper stamps every row with a
    timestamp derived from `now` and falls back to `hex` when `flight` is missing.
    """
    aircraft = payload.get("aircraft", [])
    if not isinstance(aircraft, list) or not aircraft:
        return pd.DataFrame()

    df = pd.DataFrame(aircraft)
    if df.empty:
        return df

    payload_now = payload.get("now", None)
    ts = pd.to_datetime(payload_now, unit="s", errors="coerce") if payload_now is not None else pd.NaT
    if pd.isna(ts):
        ts = pd.Timestamp.now()

    df["timestamp"] = ts
    if "flight" not in df.columns:
        df["flight"] = df.get("hex", "")
    df["flight"] = df["flight"].fillna("").astype(str).str.strip()
    df.loc[df["flight"].isin(["", "None", "nan"]), "flight"] = df.get("hex", "").fillna("").astype(str).str.strip()
    return df


def init_db():
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS flights (
                    id SERIAL PRIMARY KEY,
                    flight_no VARCHAR(50),
                    started_tracking TIMESTAMP,
                    ended_tracking TIMESTAMP,
                    flightpath_uuid UUID,
                    from_airport VARCHAR(10),
                    to_airport VARCHAR(10)
                )
            """))
        return True
    except OperationalError:
        return False


async def fetch_live_data():
    global db_ready, live_history
    while True:
        try:
            if not db_ready:
                db_ready = init_db()
            _ensure_live_history_schema()

            # Try fetching a single collated JSON from the server
            try:
                response = requests.get(COLLATED_URL, timeout=5)
                if response.status_code == 200 and response.text.strip():
                    data = response.json()
                    if "aircraft" in data:
                        df = _normalize_aircraft_payload(data)
                        if not df.empty and "lat" in df.columns:
                            df = df.dropna(subset=["lat", "lon"])
                            now = pd.Timestamp.now()
                            if db_ready:
                                db_cols = ["flight", "alt_baro", "gs", "track", "lat", "lon", "hex"]
                                for col in db_cols:
                                    if col not in df.columns:
                                        df[col] = None
                                db_df = df[db_cols].copy()
                                db_df["flight"] = db_df["flight"].astype(str).str.strip()
                                db_df["hex"] = db_df["hex"].astype(str).str.strip()
                                for col in ["alt_baro", "gs", "track", "lat", "lon"]:
                                    db_df[col] = pd.to_numeric(db_df[col], errors="coerce")
                                db_df["timestamp"] = now

                                live_history = pd.concat([live_history, db_df])

                                points = []
                                for _, row in db_df.iterrows():
                                    flight_no = str(row.get("flight", "")).strip()
                                    hex_code = str(row.get("hex", "")).strip()

                                    # Use hex + flight combo or just hex as the session identifier
                                    session_key = f"{hex_code}_{flight_no}"

                                    if session_key not in flight_uuids:
                                        new_uuid = str(uuid.uuid4())
                                        flight_uuids[session_key] = new_uuid

                                        # Register new flight in postgres
                                        try:
                                            with engine.begin() as conn:
                                                conn.execute(
                                                    text(
                                                        "INSERT INTO flights (flight_no, started_tracking, ended_tracking, flightpath_uuid, from_airport, to_airport) VALUES (:f, :s, :e, :u, :fr, :to)"
                                                    ),
                                                    {
                                                        "f": flight_no,
                                                        "s": now,
                                                        "e": now,
                                                        "u": new_uuid,
                                                        "fr": "Unknown",
                                                        "to": "Unknown",
                                                    },
                                                )
                                        except Exception as e:
                                            print(f"Napaka pri ustvarjanju leta v DB: {e}")
                                    else:
                                        # Update ended tracking
                                        try:
                                            with engine.begin() as conn:
                                                conn.execute(
                                                    text(
                                                        "UPDATE flights SET ended_tracking = :e WHERE flightpath_uuid = :u"
                                                    ),
                                                    {"e": now, "u": flight_uuids[session_key]},
                                                )
                                        except Exception as e:
                                            pass

                                    f_uuid = flight_uuids[session_key]
                                    p = (
                                        Point("flight_telemetry")
                                        .tag("flight_uuid", f_uuid)
                                        .tag("flight_no", flight_no)
                                        .tag("hex", hex_code)
                                        .field("lat", float(row["lat"]) if pd.notnull(row["lat"]) else 0.0)
                                        .field("lon", float(row["lon"]) if pd.notnull(row["lon"]) else 0.0)
                                        .field(
                                            "alt_baro", float(row["alt_baro"]) if pd.notnull(row["alt_baro"]) else 0.0
                                        )
                                        .field("gs", float(row["gs"]) if pd.notnull(row["gs"]) else 0.0)
                                        .field("track", float(row["track"]) if pd.notnull(row["track"]) else 0.0)
                                    )
                                    points.append(p)

                                try:
                                    if points:
                                        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
                                except Exception as e:
                                    print(f"Napaka InfluxDB: {e}")
                            else:
                                fallback_df = df.copy()
                                fallback_df["timestamp"] = (
                                    pd.Timestamp.now()
                                    if "timestamp" not in fallback_df.columns
                                    else fallback_df["timestamp"]
                                )
                                if "flight" not in fallback_df.columns:
                                    fallback_df["flight"] = fallback_df.get("hex", "")
                                live_history = pd.concat([live_history, fallback_df], ignore_index=True)
            except Exception as e:
                print(f"Napaka fetching collated data: {e}")

            # Da preprečimo preveliko rabo pomnilnika, ohranimo zadnjih 24 ur (namesto le 5 minut)
            cutoff = pd.Timestamp.now() - pd.Timedelta(hours=24)
            if not live_history.empty and "timestamp" in live_history.columns:
                live_history = live_history[live_history["timestamp"] >= cutoff]
        except Exception as e:
            print(f"Napaka ReadSB: {e}")

        await asyncio.sleep(2)


async def update_flight_routes():
    global flight_routes_cache
    while True:
        try:
            if not live_history.empty and "timestamp" in live_history.columns and "flight" in live_history.columns:
                # Get unique flight calls from the last hour
                recent_cutoff = pd.Timestamp.now() - pd.Timedelta(hours=1)
                recent_flights = live_history[live_history["timestamp"] >= recent_cutoff]["flight"].dropna().unique()

                # Filter planes that we haven't checked or that previously returned N/A
                # (to avoid getting stuck forever, maybe only retry ones not in cache)
                flights_to_check = [f for f in recent_flights if f.strip() and f not in flight_routes_cache]

                # Fetch global flight dataset and compare
                if flights_to_check:
                    all_fr_flights = await asyncio.to_thread(fr_api.get_flights)
                    for fr_f in all_fr_flights:
                        if fr_f.callsign in flights_to_check:
                            orig = getattr(fr_f, "origin_airport_iata", "N/A")
                            dest = getattr(fr_f, "destination_airport_iata", "N/A")
                            flight_routes_cache[fr_f.callsign] = {"origin": orig, "destination": dest}

                    # Mark any remaining missing planes as N/A so we don't query every time
                    for f in flights_to_check:
                        if f not in flight_routes_cache:
                            flight_routes_cache[f] = {"origin": "N/A", "destination": "N/A"}

                    # Update PostgreSQL mapping
                    if db_ready:
                        try:
                            with engine.begin() as conn:
                                for f_call, route in flight_routes_cache.items():
                                    if route["origin"] != "N/A" and route["origin"] != "?":
                                        conn.execute(
                                            text(
                                                "UPDATE flights SET from_airport = :fro, to_airport = :to WHERE flight_no = :f"
                                            ),
                                            {"fro": route["origin"], "to": route["destination"], "f": f_call},
                                        )
                        except Exception as e:
                            pass

        except Exception as e:
            print(f"Napaka FR24: {e}")

        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(fetch_live_data())
    fr_task = asyncio.create_task(update_flight_routes())
    yield
    task.cancel()
    fr_task.cancel()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="/app/static"), name="static")


@app.get("/")
async def get_index():
    with open("/app/static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/collated")
async def get_collated_data():
    if live_history.empty:
        return {"planes": [], "paths": {}}

    _ensure_live_history_schema()
    latest_source = live_history.copy()
    latest_source["timestamp"] = pd.to_datetime(latest_source["timestamp"], errors="coerce")
    latest_source["flight"] = latest_source["flight"].fillna("").astype(str).str.strip()
    latest_source.loc[latest_source["flight"].isin(["", "None", "nan"]), "flight"] = (
        latest_source.get("hex", "").fillna("").astype(str).str.strip()
    )
    latest_source = latest_source.dropna(subset=["timestamp"])
    if latest_source.empty:
        return {"planes": [], "paths": {}}

    # Pridobi nazadnje videno lokacijo vseh letal
    latest_df = latest_source.sort_values("timestamp").groupby("flight").tail(1).copy()

    # Kot aktivna obravnavamo samo tista letala, ki smo jih obravnavali v zadnjih 5 minutah
    active_cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=5)
    active_latest_df = latest_df[latest_df["timestamp"] >= active_cutoff].copy()
    active_flights = active_latest_df["flight"].unique()

    if active_flights.size == 0:
        return {"planes": [], "paths": {}}

    # Prikažemo CELOTNO zgodovino (do 24ur iz predpomnilnika) samo za TRENUTNO AKTIVNA letala
    active_history = latest_source[latest_source["flight"].isin(active_flights)]
    paths = (
        active_history.sort_values("timestamp")
        .dropna(subset=["lat", "lon"])
        .groupby("flight")
        .apply(lambda x: x[["lat", "lon"]].values.tolist(), include_groups=False)
        .to_dict()
    )

    # Sanitize paths: convert numpy types to Python floats and drop invalid points
    sanitized_paths = {}
    for flt, pts in (paths or {}).items():
        good_pts = []
        for p in pts:
            # p may be a list/ndarray like [lat, lon]
            try:
                lat = float(p[0])
                lon = float(p[1])
            except Exception:
                continue
            if not (math.isfinite(lat) and math.isfinite(lon)):
                continue
            good_pts.append([lat, lon])
        sanitized_paths[flt] = good_pts

    active_latest_df = active_latest_df.replace({pd.NA: None})
    planes = active_latest_df.to_dict(orient="records")

    # Sanitize plane records: replace NaN/inf with None and coerce numpy types
    def _sanitize_obj(obj):
        if isinstance(obj, dict):
            return {k: _sanitize_obj(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitize_obj(v) for v in obj]
        # pandas/ numpy missing
        try:
            if pd.isna(obj):
                return None
        except Exception:
            pass
        # numeric types
        if isinstance(obj, (int, float, np.number)):
            try:
                f = float(obj)
                if not math.isfinite(f):
                    return None
                return f
            except Exception:
                return None
        return obj

    for p in planes:
        flight_call = str(p.get("flight", "")).strip()
        route = flight_routes_cache.get(flight_call, {"origin": "?", "destination": "?"})
        p["route_origin"] = route["origin"]
        p["route_dest"] = route["destination"]
        # sanitize each record in-place
    planes = [_sanitize_obj(p) for p in planes]

    return {"planes": planes, "paths": sanitized_paths}


@app.get("/api/history/dates")
async def get_history_dates():
    if not db_ready:
        return {"min": None, "max": None}
    try:
        dates_df = pd.read_sql("SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts FROM flights", engine)
        if dates_df.empty or pd.isnull(dates_df["min_ts"].iloc[0]):
            return {"min": None, "max": None}
        return {"min": dates_df["min_ts"].iloc[0].isoformat(), "max": dates_df["max_ts"].iloc[0].isoformat()}
    except Exception:
        return {"min": None, "max": None}


@app.get("/api/history/data")
async def get_history_data(target_time: str):
    if not db_ready:
        return {"planes": [], "paths": {}}
    try:
        target_dt = datetime.fromisoformat(target_time)
        window_start = target_dt - timedelta(minutes=5)

        query = text("""
            SELECT * FROM flights 
            WHERE timestamp >= :start AND timestamp <= :end 
            ORDER BY timestamp
        """)
        hist_df = pd.read_sql(query, engine, params={"start": window_start, "end": target_dt})

        if hist_df.empty:
            return {"planes": [], "paths": {}}

        paths = (
            hist_df.dropna(subset=["lat", "lon"])
            .groupby("flight")
            .apply(lambda x: x[["lat", "lon"]].values.tolist(), include_groups=False)
            .to_dict()
        )

        latest_df = hist_df.groupby("flight").tail(1).copy()
        latest_df = latest_df.replace({pd.NA: None})
        for c in ["timestamp"]:
            latest_df[c] = latest_df[c].astype(str)

        planes = latest_df.to_dict(orient="records")
        for p in planes:
            flight_call = str(p.get("flight", "")).strip()
            route = flight_routes_cache.get(flight_call, {"origin": "?", "destination": "?"})
            p["route_origin"] = route["origin"]
            p["route_dest"] = route["destination"]

        return {"planes": planes, "paths": paths}
    except Exception as e:
        print(e)
        return {"planes": [], "paths": {}}
