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
            try:
                response = requests.get(COLLATED_URL, timeout=5)
                if response.status_code == 200 and response.text.strip():
                    data = response.json()
                    if "aircraft" in data:
                        df = pd.DataFrame(data["aircraft"])
                        if not df.empty and "lat" in df.columns:
                            df = df.dropna(subset=["lat", "lon"])
                            now = pd.Timestamp.now()
                            if db_ready:
                                db_cols = ['flight', 'alt_baro', 'gs', 'track', 'lat', 'lon', 'hex']
                                for col in db_cols:
                                    if col not in df.columns:
                                        df[col] = None
                                db_df = df[db_cols].copy()
                                db_df['flight'] = db_df['flight'].astype(str).str.strip()
                                db_df['hex'] = db_df['hex'].astype(str).str.strip()
                                for col in ['alt_baro', 'gs', 'track', 'lat', 'lon']:
                                    db_df[col] = pd.to_numeric(db_df[col], errors='coerce')
                                db_df['timestamp'] = now
                                
                                live_history = pd.concat([live_history, db_df])
                                
                                points = []
                                for _, row in db_df.iterrows():
                                    flight_no = str(row.get('flight', '')).strip()
                                    hex_code = str(row.get('hex', '')).strip()
                                    
                                    # Use hex + flight combo or just hex as the session identifier
                                    session_key = f"{hex_code}_{flight_no}"
                                    
                                    if session_key not in flight_uuids:
                                        new_uuid = str(uuid.uuid4())
                                        flight_uuids[session_key] = new_uuid
                                        
                                        # Register new flight in postgres
                                        try:
                                            with engine.begin() as conn:
                                                conn.execute(
                                                    text("INSERT INTO flights (flight_no, started_tracking, ended_tracking, flightpath_uuid, from_airport, to_airport) VALUES (:f, :s, :e, :u, :fr, :to)"),
                                                    {"f": flight_no, "s": now, "e": now, "u": new_uuid, "fr": "Unknown", "to": "Unknown"}
                                                )
                                        except Exception as e:
                                            print(f"Napaka pri ustvarjanju leta v DB: {e}")
                                    else:
                                        # Update ended tracking
                                        try:
                                            with engine.begin() as conn:
                                                conn.execute(
                                                    text("UPDATE flights SET ended_tracking = :e WHERE flightpath_uuid = :u"),
                                                    {"e": now, "u": flight_uuids[session_key]}
                                                )
                                        except Exception as e:
                                            pass

                                    f_uuid = flight_uuids[session_key]
                                    p = (Point("flight_telemetry")
                                        .tag("flight_uuid", f_uuid)
                                        .tag("flight_no", flight_no)
                                        .tag("hex", hex_code)
                                        .field("lat", float(row['lat']) if pd.notnull(row['lat']) else 0.0)
                                        .field("lon", float(row['lon']) if pd.notnull(row['lon']) else 0.0)
                                        .field("alt_baro", float(row['alt_baro']) if pd.notnull(row['alt_baro']) else 0.0)
                                        .field("gs", float(row['gs']) if pd.notnull(row['gs']) else 0.0)
                                        .field("track", float(row['track']) if pd.notnull(row['track']) else 0.0))
                                    points.append(p)
                                    
                                try:
                                    if points:
                                        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
                                except Exception as e:
                                    print(f"Napaka InfluxDB: {e}")
                            else:
                                fallback_df = df.copy()
                                fallback_df['timestamp'] = now
                                if 'flight' not in fallback_df.columns:
                                    fallback_df['flight'] = fallback_df.get('hex', '')
                                live_history = pd.concat([live_history, fallback_df])
            except Exception as e:
                print(f"Napaka fetching collated data: {e}")

            # Da preprečimo preveliko rabo pomnilnika, ohranimo zadnjih 24 ur (namesto le 5 minut)
            cutoff = pd.Timestamp.now() - pd.Timedelta(hours=24)
            live_history = live_history[live_history['timestamp'] >= cutoff]
        except Exception as e:
            print(f"Napaka ReadSB: {e}")
        
        await asyncio.sleep(2)

async def update_flight_routes():
    global flight_routes_cache
    while True:
        try:
            if not live_history.empty:
                # Get unique flight calls from the last hour
                recent_cutoff = pd.Timestamp.now() - pd.Timedelta(hours=1)
                recent_flights = live_history[live_history['timestamp'] >= recent_cutoff]['flight'].dropna().unique()
                
                # Filter planes that we haven't checked or that previously returned N/A
                # (to avoid getting stuck forever, maybe only retry ones not in cache)
                flights_to_check = [f for f in recent_flights if f.strip() and f not in flight_routes_cache]
                
                # Fetch global flight dataset and compare
                if flights_to_check:
                    all_fr_flights = await asyncio.to_thread(fr_api.get_flights)
                    for fr_f in all_fr_flights:
                        if fr_f.callsign in flights_to_check:
                            orig = getattr(fr_f, 'origin_airport_iata', 'N/A')
                            dest = getattr(fr_f, 'destination_airport_iata', 'N/A')
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
                                    if route['origin'] != "N/A" and route['origin'] != "?":
                                        conn.execute(
                                            text("UPDATE flights SET from_airport = :fro, to_airport = :to WHERE flight_no = :f"),
                                            {"fro": route['origin'], "to": route['destination'], "f": f_call}
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

@app.get("/api/live")
async def get_live():
    if live_history.empty:
        return {"planes": [], "paths": {}}
    
    # Pridobi nazadnje videno lokacijo vseh letal
    latest_df = live_history.sort_values('timestamp').groupby('flight').tail(1).copy()
    
    # Kot aktivna obravnavamo samo tista letala, ki smo jih obravnavali v zadnjih 5 minutah
    active_cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=5)
    active_latest_df = latest_df[latest_df['timestamp'] >= active_cutoff].copy()
    active_flights = active_latest_df['flight'].unique()
    
    if active_flights.size == 0:
        return {"planes": [], "paths": {}}

    # Prikažemo CELOTNO zgodovino (do 24ur iz predpomnilnika) samo za TRENUTNO AKTIVNA letala
    active_history = live_history[live_history['flight'].isin(active_flights)]
    paths = active_history.sort_values("timestamp").dropna(subset=["lat", "lon"]).groupby("flight").apply(
        lambda x: x[["lat", "lon"]].values.tolist(), include_groups=False
    ).to_dict()

    active_latest_df = active_latest_df.replace({pd.NA: None})
    planes = active_latest_df.to_dict(orient="records")
    for p in planes:
        flight_call = str(p.get('flight', '')).strip()
        route = flight_routes_cache.get(flight_call, {"origin": "?", "destination": "?"})
        p['route_origin'] = route['origin']
        p['route_dest'] = route['destination']
        
    return {"planes": planes, "paths": paths}

@app.get("/api/history/dates")
async def get_history_dates():
    if not db_ready:
        return {"min": None, "max": None}
    try:
        dates_df = pd.read_sql("SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts FROM flights", engine)
        if dates_df.empty or pd.isnull(dates_df['min_ts'].iloc[0]):
            return {"min": None, "max": None}
        return {
            "min": dates_df['min_ts'].iloc[0].isoformat(),
            "max": dates_df['max_ts'].iloc[0].isoformat()
        }
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
            
        paths = hist_df.dropna(subset=["lat", "lon"]).groupby("flight").apply(
            lambda x: x[["lat", "lon"]].values.tolist(), include_groups=False
        ).to_dict()

        latest_df = hist_df.groupby('flight').tail(1).copy()
        latest_df = latest_df.replace({pd.NA: None})
        for c in ['timestamp']:
            latest_df[c] = latest_df[c].astype(str)
        
        planes = latest_df.to_dict(orient="records")
        for p in planes:
            flight_call = str(p.get('flight', '')).strip()
            route = flight_routes_cache.get(flight_call, {"origin": "?", "destination": "?"})
            p['route_origin'] = route['origin']
            p['route_dest'] = route['destination']
            
        return {"planes": planes, "paths": paths}
    except Exception as e:
        print(e)
        return {"planes": [], "paths": {}}

