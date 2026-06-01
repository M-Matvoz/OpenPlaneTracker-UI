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
flight_uuids = {}
