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
import numpy as np
import uvicorn
import uuid
import math
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


@app.get("/live/collated")
async def get_collated_data():
    if live_history.empty:
        return {"aircraft": [], "all_aircraft": [], "paths": {}, "dashed_paths": {}}

    # 1. Pridobi nazadnje videno lokacijo vseh letal v zadnjih 24 urah
    all_latest_df = live_history.sort_values("timestamp").groupby("hex").tail(1).copy()

    # 2. Kot aktivna obravnavamo samo tista letala, ki smo jih opazili v zadnjih 5 minutah
    active_cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=5)
    active_latest_df = all_latest_df[all_latest_df["timestamp"] >= active_cutoff].copy()
    active_hexes = active_latest_df["hex"].unique()

    # 3. Zgodovina poti samo za aktivna letala (grupirano in indeksirano preko HEX-a)
    active_history = live_history[live_history["hex"].isin(active_hexes)]

    paths = {}
    dashed_paths = {}

    for hex_code, group in active_history.sort_values("timestamp").groupby("hex"):
        points = group[["lat", "lon", "timestamp"]].to_numpy()
        segments = []
        d_segments = []

        if len(points) > 0:
            current_segment = [[points[0][0], points[0][1]]]
            for i in range(1, len(points)):
                ts1 = pd.to_datetime(points[i - 1][2]).tz_localize(None)
                ts2 = pd.to_datetime(points[i][2]).tz_localize(None)
                time_diff = (ts2 - ts1).total_seconds()

                if time_diff > 10:
                    if len(current_segment) > 1:
                        segments.append(current_segment)

                    # Črtkana povezava
                    d_segments.append([[points[i - 1][0], points[i - 1][1]], [points[i][0], points[i][1]]])
                    current_segment = []

                if pd.notnull(points[i][0]) and pd.notnull(points[i][1]):
                    current_segment.append([points[i][0], points[i][1]])

            if len(current_segment) > 1:
                segments.append(current_segment)

        paths[hex_code] = segments
        dashed_paths[hex_code] = d_segments

    # 4. REŠITEV ZA MASK & SIZE ERROR:
    # Namesto zapletenega maskiranja nad sortiranimi podatki uporabimo enostaven .replace()
    # Pred pretvorbo v slovar poskrbimo, da se Timestamp objekti spremenijo v ISO nize
    for df in [active_latest_df, all_latest_df]:
        if "timestamp" in df.columns:
            df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    active_latest_df = active_latest_df.replace({np.nan: None})
    all_latest_df = all_latest_df.replace({np.nan: None})

    # Poenotimo izhod: če je flight prazen ali enak hex-u, naj bo None za frontend logiko
    aircraft_list = active_latest_df.to_dict(orient="records")
    all_aircraft_list = all_latest_df.to_dict(orient="records")

    for ac in aircraft_list + all_aircraft_list:
        if ac.get("flight"):
            ac["flight"] = str(ac["flight"]).strip()
            if ac["flight"] == "" or ac["flight"] == ac["hex"]:
                ac["flight"] = None

    return {
        "aircraft": aircraft_list,
        "all_aircraft": all_aircraft_list,
        "paths": paths,
        "dashed_paths": dashed_paths,
    }


@app.get("/live/history/dates")
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


@app.get("/live/history/data")
async def get_history_data(target_time: str):
    try:
        # Rešitev za napačne ISO formate, če se podvoji 'T'
        if target_time.count("T") > 1:
            parts = target_time.split("T")
            target_time = f"{parts[0]}T{parts[1]}"

        # 1. Parsiramo prejeti lokalni čas z offsetom (+02:00)
        target_dt = datetime.fromisoformat(target_time)

        # 2. Pretvorba v čisti UTC za InfluxDB poizvedbo
        from datetime import timezone

        target_utc = target_dt.astimezone(timezone.utc)
        start_utc = target_utc - timedelta(minutes=30)

        # 3. Formatiramo v standardni UTC niz z 'Z' na koncu, kar Influx pričakuje
        target_utc_str = target_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        start_utc_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        flux_query = f"""
            from(bucket: "{INFLUX_BUCKET}")
            |> range(start: {start_utc_str}, stop: {target_utc_str})
            |> filter(fn: (r) => r["_measurement"] == "flight_telemetry")
            |> pivot(rowKey:["_time", "flight_uuid", "hex", "flight_no"], columnKey: ["_field"], valueColumn: "_value")
        """

        tables = influx_client.query_api().query(flux_query, org=INFLUX_ORG)

        paths_by_hex = {}
        seen_hexes = {}

        for table in tables:
            for record in table.records:
                # Pravilno branje vseh vrednosti iz pivotiranega Flux zapisa preko .values.get()
                h = record.values.get("hex")
                fl = record.values.get("flight_no")
                lat = record.values.get("lat")
                lon = record.values.get("lon")
                alt = record.values.get("alt_baro")
                gs = record.values.get("gs")
                tr = record.values.get("track")
                t = record.get_time()  # .get_time() je pravilna metoda za časovno oznako

                if not h or lat is None or lon is None or math.isnan(lat) or math.isnan(lon):
                    continue

                h = str(h).strip()
                fl = str(fl).strip() if fl else None
                if fl == "" or fl == h:
                    fl = None

                if h not in paths_by_hex:
                    paths_by_hex[h] = []
                paths_by_hex[h].append({"lat": lat, "lon": lon, "time": t})

                # Za primerjavo najnovejše točke uporabimo UTC čas 't'
                if h not in seen_hexes or t > seen_hexes[h]["_raw_time"]:
                    seen_hexes[h] = {
                        "hex": h,
                        "flight": fl,
                        "lat": lat,
                        "lon": lon,
                        "alt_baro": None if alt is None or math.isnan(alt) else int(alt),
                        "gs": None if gs is None or math.isnan(gs) else int(gs),
                        "track": None if tr is None or math.isnan(tr) else tr,
                        "_raw_time": t,
                    }

        # Formatiramo poti v segmente
        # Formatiramo poti v segmente in črtkane segmente
        formatted_paths = {}
        dashed_paths = {}

        for h, pts in paths_by_hex.items():
            pts.sort(key=lambda x: x["time"])
            segments = []
            d_segments = []

            if pts:
                curr_seg = [[pts[0]["lat"], pts[0]["lon"]]]
                for i in range(1, len(pts)):
                    diff = (pts[i]["time"] - pts[i - 1]["time"]).total_seconds()

                    if diff > 10:
                        # Zaključimo trenutni polni segment
                        if len(curr_seg) > 1:
                            segments.append(curr_seg)

                        # Ustvarimo črtkani segment med zadnjo točko starega in prvo točko novega segmenta
                        d_segments.append([[pts[i - 1]["lat"], pts[i - 1]["lon"]], [pts[i]["lat"], pts[i]["lon"]]])

                        curr_seg = []

                    curr_seg.append([pts[i]["lat"], pts[i]["lon"]])

                if len(curr_seg) > 1:
                    segments.append(curr_seg)

            formatted_paths[h] = segments
            dashed_paths[h] = d_segments
        # Očistimo začasni '_raw_time' pred pošiljanjem JSON-a
        aircraft_list = []

        for h, ac in seen_hexes.items():
            # ac["_raw_time"] je čas zadnjega prejetega signala za to letalo
            # Preverimo, če je letalo oddalo signal v zadnjih 3 minutah (180 sekundah) pred target_utc
            time_since_last_signal = (target_utc - ac["_raw_time"]).total_seconds()

            if time_since_last_signal <= 180:  # 3 minute cutoff (lahko spremeniš na 120 ali 300)
                # Očistimo začasni '_raw_time' pred pošiljanjem
                ac.pop("_raw_time", None)
                aircraft_list.append(ac)
            else:
                # Letalo je že odletelo iz dosega oz. je zastarelo, ne dodamo ga med aktivne markerje
                pass

        return {
            "aircraft": aircraft_list,
            "all_aircraft": aircraft_list,
            "paths": formatted_paths,
            "dashed_paths": dashed_paths,  # DOLOČENO: dodamo črtkane poti
        }

    except Exception as e:
        print(f"Napaka v zgodovini: {e}")
        return {"aircraft": [], "all_aircraft": [], "paths": {}, "dashed_paths": {}}
