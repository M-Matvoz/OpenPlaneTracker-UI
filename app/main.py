import asyncio
import pandas as pd
import requests
import httpx
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
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

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
COLLATED_URL = os.getenv("OPT_COLLATED_URL", "http://openplanetracker-server:8080/api/collated")
DB_URI = "postgresql://postgres:postgrespw@db:5432/planetracker"
engine = create_engine(DB_URI)

db_ready = False
live_history = pd.DataFrame()
flight_uuids = {}


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

            # Try fetching a single collated JSON from the server
            async with httpx.AsyncClient() as client:
                response = await client.get(COLLATED_URL)
                response.raise_for_status()
                data = response.json()
                # print(f"Fetched data: {data}")

                if data and data.get("aircraft"):
                    new_data_df = pd.DataFrame(data["aircraft"])
                    if not new_data_df.empty and "lat" in new_data_df.columns:
                        new_data_df = new_data_df.dropna(subset=["lat", "lon"])
                        now = pd.Timestamp.now()
                        if db_ready:
                            db_cols = ["flight", "alt_baro", "gs", "track", "lat", "lon", "hex"]
                            for col in db_cols:
                                if col not in new_data_df.columns:
                                    new_data_df[col] = None
                            db_df = new_data_df[db_cols].copy()
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
                                    .field("alt_baro", float(row["alt_baro"]) if pd.notnull(row["alt_baro"]) else 0.0)
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
                            fallback_df = new_data_df.copy()
                            fallback_df["timestamp"] = now
                            if "flight" not in fallback_df.columns:
                                fallback_df["flight"] = fallback_df.get("hex", "")
                            live_history = pd.concat([live_history, fallback_df])

            # Da preprečimo preveliko rabo pomnilnika, ohranimo zadnjih 24 ur (namesto le 5 minut)
            cutoff = pd.Timestamp.now() - pd.Timedelta(hours=24)
            live_history = live_history[live_history["timestamp"] >= cutoff]
        except Exception as e:
            print(f"Napaka ReadSB: {e}")

        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(fetch_live_data())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="/app/static"), name="static")


@app.get("/")
async def get_index():
    with open("/app/static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/collated")
async def get_collated_data():
    if live_history.empty:
        return {"aircraft": [], "all_aircraft": [], "paths": {}}

    # Pridobi nazadnje videno lokacijo vseh letal v zadnjih 24 urah
    all_latest_df = live_history.sort_values("timestamp").groupby("flight").tail(1).copy()

    # Kot aktivna obravnavamo samo tista letala, ki smo jih obravnavali v zadnjih 5 minutah
    active_cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=5)
    active_latest_df = all_latest_df[all_latest_df["timestamp"] >= active_cutoff].copy()

    active_flights = active_latest_df["flight"].unique()

    # Zgodovina poti samo za aktivna letala
    active_history = live_history[live_history["flight"].isin(active_flights)]

    # Process paths to include segments for gaps
    paths = {}
    for flight, group in active_history.sort_values("timestamp").groupby("flight"):
        points = group[["lat", "lon", "timestamp"]].to_numpy()
        segments = []
        if len(points) > 0:
            current_segment = [[points[0][0], points[0][1]]]
            for i in range(1, len(points)):
                # Ensure timestamps are timezone-aware for subtraction
                ts1 = pd.to_datetime(points[i - 1][2]).tz_localize(None)
                ts2 = pd.to_datetime(points[i][2]).tz_localize(None)
                time_diff = (ts2 - ts1).total_seconds()
                if time_diff > 10:
                    if len(current_segment) > 1:
                        segments.append(current_segment)
                    current_segment = []
                # Only add point if it has coordinates
                if points[i][0] is not None and points[i][1] is not None:
                    current_segment.append([points[i][0], points[i][1]])
            if len(current_segment) > 1:
                segments.append(current_segment)
        paths[flight] = segments

    # Pripravi podatke za JSON response
    active_latest_df = active_latest_df.replace({pd.NA: None})
    all_latest_df = all_latest_df.sort_values("timestamp", ascending=False).replace({pd.NA: None})

    aircraft = active_latest_df.to_dict(orient="records")
    all_aircraft = all_latest_df.to_dict(orient="records")
    print(f"Active aircraft: {aircraft}, All aircraft: {all_aircraft}, Paths: {paths}")

    return {"aircraft": aircraft, "all_aircraft": all_aircraft, "paths": paths}


@app.get("/api/history/dates")
async def get_history_dates():
    if not db_ready:
        return {"min": None, "max": None}
    try:
        query = text("SELECT MIN(started_tracking) as min_ts, MAX(ended_tracking) as max_ts FROM flights")
        with engine.connect() as connection:
            dates_df = pd.read_sql(query, connection)
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

        # Find flights that were active at target_dt
        active_flights_query = text("""
            SELECT flightpath_uuid, flight_no FROM flights
            WHERE started_tracking <= :target AND ended_tracking >= :target
        """)
        with engine.connect() as connection:
            active_flights_df = pd.read_sql(active_flights_query, connection, params={"target": target_dt})

        if active_flights_df.empty:
            return {"planes": [], "paths": {}}

        active_uuids = tuple(active_flights_df["flightpath_uuid"].astype(str).tolist())

        # Get telemetry for these flights up to target_dt
        flux_query = f"""
            from(bucket: "{INFLUX_BUCKET}")
              |> range(start: 0, stop: {target_dt.isoformat()}Z)
              |> filter(fn: (r) => contains(value: r.flight_uuid, set: {str(list(active_uuids))}))
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> group(columns: ["flight_uuid"])
        """

        query_api = influx_client.query_api()
        result = query_api.query(flux_query, org=INFLUX_ORG)

        planes = []
        paths = {}

        for table in result:
            flight_uuid = table.records[0].values.get("flight_uuid")
            flight_no = active_flights_df[active_flights_df["flightpath_uuid"].astype(str) == flight_uuid][
                "flight_no"
            ].iloc[0]

            # Process paths to include segments for gaps
            segments = []
            if table.records:
                # Sort records by time just in case
                sorted_records = sorted(table.records, key=lambda r: r.get_time())

                current_segment = [
                    [sorted_records[0].get_value_by_key("lat"), sorted_records[0].get_value_by_key("lon")]
                ]
                for i in range(1, len(sorted_records)):
                    time_diff = (sorted_records[i].get_time() - sorted_records[i - 1].get_time()).total_seconds()
                    if time_diff > 10:
                        segments.append(current_segment)
                        current_segment = []
                    current_segment.append(
                        [sorted_records[i].get_value_by_key("lat"), sorted_records[i].get_value_by_key("lon")]
                    )
                segments.append(current_segment)

            paths[flight_no] = segments

            # Get the latest record for the plane's current position
            if table.records:
                latest_record = max(table.records, key=lambda r: r.get_time())
                planes.append(
                    {
                        "hex": latest_record.values.get("hex"),
                        "flight": flight_no,
                        "lat": latest_record.get_value_by_key("lat"),
                        "lon": latest_record.get_value_by_key("lon"),
                        "alt_baro": latest_record.get_value_by_key("alt_baro"),
                        "gs": latest_record.get_value_by_key("gs"),
                        "track": latest_record.get_value_by_key("track"),
                    }
                )

        return {"planes": planes, "paths": paths}
    except Exception as e:
        print(e)
        return {"planes": [], "paths": {}}
