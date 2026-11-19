"""
FastAPI Server for Urban Traffic Intelligence Web App.
Provides RESTful APIs for cities, live incidents, KPIs, and status.
Includes Google Maps-like traffic severity color tagging and Auto-refresh support.
"""

import os
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from web.database import (
    get_monitored_cities,
    get_live_city_incidents,
    get_live_city_kpi,
    get_incident_type_distribution
)

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(
    title="Urban Traffic Intelligence API",
    description="Backend API serving real-time urban traffic congestion and road incident metrics for Hanoi, HCMC, and Da Nang.",
    version="1.0.0"
)

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files (Frontend HTML/CSS/JS)
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/")
def serve_index():
    """Serve Frontend Single Page Application."""
    index_file = BASE_DIR / "templates" / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {
        "message": "Urban Traffic Intelligence API is running.",
        "documentation": "/docs",
        "api_endpoints": {
            "cities": "/api/cities",
            "live_incidents": "/api/incidents?city_id=HAN",
            "kpi_metrics": "/api/kpi?city_id=HAN",
            "type_distribution": "/api/distribution?city_id=HAN"
        }
    }

@app.get("/api/cities")
def api_get_cities():
    """Retrieve list of monitored cities with geographical centers."""
    try:
        cities = get_monitored_cities()
        return {"status": "success", "data": cities}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/incidents")
def api_get_incidents(
    city_id: str = Query("HAN", description="City identifier: HAN, SGN, or DAD")
):
    """
    Retrieve live traffic congestion points and road incidents for the selected city.
    Includes Google Maps-like severity categorization (severe: red, moderate: yellow, light: green).
    """
    city_id = city_id.upper()
    try:
        incidents = get_live_city_incidents(city_id)
        return {
            "status": "success",
            "city_id": city_id,
            "count": len(incidents),
            "data": incidents
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/kpi")
def api_get_kpi(
    city_id: str = Query("HAN", description="City identifier: HAN, SGN, or DAD")
):
    """Retrieve live aggregated KPI metrics (total spots, queue length, delay)."""
    city_id = city_id.upper()
    try:
        kpi = get_live_city_kpi(city_id)
        return {
            "status": "success",
            "city_id": city_id,
            "data": kpi
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/distribution")
def api_get_distribution(
    city_id: str = Query("HAN", description="City identifier: HAN, SGN, or DAD")
):
    """Retrieve distribution breakdown of incident categories for charting."""
    city_id = city_id.upper()
    try:
        distribution = get_incident_type_distribution(city_id)
        return {
            "status": "success",
            "city_id": city_id,
            "data": distribution
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
def api_health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "traffic-web-backend"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web.server:app", host="0.0.0.0", port=8000, reload=True)

