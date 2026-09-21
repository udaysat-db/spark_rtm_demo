"""SignalNow console — FastAPI backend.

Serves the single-pipeline monitoring UI plus a small JSON API. The API talks only
to `DataSource` (mock today, Kafka-tailing in production). Run locally:
    uvicorn app:app --port 8000     (from the app/ directory)
On Databricks Apps the command in app.yaml starts uvicorn on the app port.
"""
import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from backend import get_data_source

app = FastAPI(title="SignalNow Console")
data = get_data_source()

_STATIC = os.path.join(os.path.dirname(__file__), "static")


@app.get("/api/snapshot")
def snapshot():
    """Everything the UI needs for one refresh."""
    return JSONResponse(data.snapshot())


@app.get("/api/health")
def health():
    return {"ok": True, "mock": data.is_mock}


@app.post("/api/controls/burst")
def set_burst(on: bool = True):
    """Demo control (mock backend only) — flips the simulated scenario."""
    data.set_burst(on)
    return {"burst": on, "mock": data.is_mock}


@app.post("/api/controls/mode")
def set_mode(mode: str = "rtm"):
    """Preview the UI under an rtm/mb latency profile (mock backend only)."""
    data.set_mode(mode)
    return {"mode": mode, "mock": data.is_mock}


# Serve the frontend at / (mounted last so /api routes win).
app.mount("/", StaticFiles(directory=_STATIC, html=True), name="static")
