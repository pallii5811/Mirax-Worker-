import sys
import os
import time

import uvicorn
import webbrowser
import threading
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles


# --- CONFIGURAZIONE PATH PER EXE ---
# Questo serve per trovare la cartella 'frontend/out' quando siamo dentro l'EXE
if getattr(sys, "frozen", False):
    BASE_DIR = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

STATIC_DIR = os.path.join(BASE_DIR, "frontend", "out")

app = FastAPI()


# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- DATI DEMO ---
DEMO_DATA = [
    {
        "result_index": 0,
        "business_name": "Ristorante Da Vittorio",
        "address": "Milano Centro",
        "phone": "+39 347 9998881",
        "email": "info@davittorio.com",
        "website": "http://www.davittorio.com",
        "website_status": "HAS_WEBSITE",
        "tech_stack": "WordPress",
        "load_speed_s": 0.4,
        "load_speed": 0.4,
        "domain_creation_date": None,
        "domain_expiration_date": None,
        "website_http_status": 200,
        "website_error": None,
        "website_has_html": False,
        "website_error_line": None,
        "website_error_hint": None,
        "instagram_missing": False,
        "tiktok_missing": True,
        "pixel_missing": False,
        # aliases often used by older/alternate demo UIs
        "id": 1,
        "domain": "CELL: 347 9998881",
        "url": "Ristorante Da Vittorio",
        "title": "Ristorante Da Vittorio",
        "status": "active",
        "score": 98,
        "speed": 0.4,
        "mobile_friendly": True,
        # explicit marketing fields some UIs look for
        "has_facebook_pixel": True,
        "has_google_pixel": True,
        "has_tiktok_pixel": False,
        "has_google_analytics": True,
        "audit": {
            "has_facebook_pixel": True,
            "has_tiktok_pixel": False,
            "has_gtm": True,
            "has_ssl": True,
            "is_mobile_responsive": True,
            "missing_instagram": False,
        },
    },
    {
        "result_index": 1,
        "business_name": "Elettricista Pronto Intervento",
        "address": "Roma Nord",
        "phone": "+39 333 1234567",
        "email": "mario@rossi.it",
        "website": "http://www.elettricistaroma.it",
        "website_status": "HAS_WEBSITE",
        "tech_stack": "Custom",
        "load_speed_s": 1.2,
        "load_speed": 1.2,
        "domain_creation_date": None,
        "domain_expiration_date": None,
        "website_http_status": 200,
        "website_error": None,
        "website_has_html": False,
        "website_error_line": None,
        "website_error_hint": None,
        "instagram_missing": True,
        "tiktok_missing": True,
        "pixel_missing": True,
        "id": 2,
        "domain": "CELL: 333 1234567",
        "url": "Elettricista Pronto Intervento",
        "title": "Elettricista Pronto Intervento",
        "status": "active",
        "score": 45,
        "speed": 1.2,
        "mobile_friendly": False,
        "has_facebook_pixel": False,
        "has_google_pixel": True,
        "has_tiktok_pixel": False,
        "has_google_analytics": False,
        "audit": {
            "has_facebook_pixel": False,
            "has_tiktok_pixel": False,
            "has_gtm": False,
            "has_ssl": True,
            "is_mobile_responsive": False,
            "missing_instagram": True,
        },
    },
    {
        "result_index": 2,
        "business_name": "Impresa Edile Costruire",
        "address": "Milano Sud",
        "phone": "+39 338 5556667",
        "email": "",
        "website": None,
        "website_status": "MISSING_WEBSITE",
        "tech_stack": None,
        "load_speed_s": None,
        "load_speed": None,
        "domain_creation_date": None,
        "domain_expiration_date": None,
        "website_http_status": None,
        "website_error": None,
        "website_has_html": False,
        "website_error_line": None,
        "website_error_hint": None,
        "instagram_missing": True,
        "tiktok_missing": True,
        "pixel_missing": True,
        "id": 3,
        "domain": "CELL: 338 5556667",
        "url": "Impresa Edile Costruire",
        "title": "Impresa Edile Costruire",
        "status": "active",
        "score": 20,
        "speed": 2.5,
        "mobile_friendly": True,
        "has_facebook_pixel": False,
        "has_google_pixel": False,
        "has_tiktok_pixel": False,
        "has_google_analytics": False,
        "audit": {
            "has_facebook_pixel": False,
            "has_tiktok_pixel": False,
            "has_gtm": False,
            "has_ssl": False,
            "is_mobile_responsive": True,
            "missing_instagram": True,
        },
    },
]


def _job_pending_payload(job_id: str = "demo_123") -> dict:
    return {
        "id": job_id,
        "job_id": job_id,
        "status": "pending",
        "state": "running",
        "progress": 5,
        "message": "Audit started",
        "started_at": time.time(),
    }


def _job_finished_payload(job_id: str = "demo_123") -> dict:
    return {
        "id": job_id,
        "job_id": job_id,
        "status": "finished",
        "state": "done",
        "progress": 100,
        "message": "Audit completed",
        "completed": True,
        "results_count": len(DEMO_DATA),
        "result": DEMO_DATA,
        "data": DEMO_DATA,
        "items": DEMO_DATA,
        "finished_at": time.time(),
    }


# --- API CATCH-ALL (Evita Errore 400) ---
@app.api_route("/api/{full_path:path}", methods=["GET", "POST", "PUT", "OPTIONS", "DELETE"])
async def api_handler(full_path: str, request: Request):
    # Consuma sempre il body (se c'e') senza mai fallire
    try:
        await request.json()
    except Exception:
        pass

    if request.method.upper() == "POST":
        return JSONResponse(_job_pending_payload())

    time.sleep(0.5)
    return JSONResponse(_job_finished_payload())


# Gestione specifica per /jobs (se il frontend chiama senza /api)
@app.api_route("/jobs", methods=["POST", "OPTIONS"])
async def jobs_post(request: Request):
    try:
        await request.json()
    except Exception:
        pass
    return JSONResponse(_job_pending_payload())


@app.api_route("/jobs/{job_id}", methods=["GET"])
async def jobs_get(job_id: str):
    time.sleep(0.5)
    return JSONResponse(_job_finished_payload(job_id=job_id))


@app.api_route("/jobs/{job_id}/results", methods=["GET"])
async def jobs_results(job_id: str):
    # Il frontend spesso chiama direttamente questo endpoint per riempire la tabella
    time.sleep(0.2)
    return JSONResponse(DEMO_DATA)


@app.api_route("/api/jobs", methods=["POST", "OPTIONS"])
async def api_jobs_post(request: Request):
    return await jobs_post(request)


@app.api_route("/api/jobs/{job_id}", methods=["GET"])
async def api_jobs_get(job_id: str):
    return await jobs_get(job_id)


@app.api_route("/api/jobs/{job_id}/results", methods=["GET"])
async def api_jobs_results(job_id: str):
    return await jobs_results(job_id)


# --- SERVING FRONTEND (IMPORTANTE: Mettere per ultimo) ---
# Se la cartella static esiste (quindi se abbiamo compilato bene), montiamola.
if os.path.exists(STATIC_DIR):
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
else:
    print(f"ATTENZIONE: Cartella static non trovata in: {STATIC_DIR}")


if __name__ == "__main__":
    # Apre il browser dopo 1.5 secondi (dà tempo al server di partire)
    def open_browser():
        webbrowser.open("http://127.0.0.1:8000")

    threading.Timer(1.5, open_browser).start()

    print(">>> AVVIO CLIENT SNIPER DEMO...")
    uvicorn.run(app, host="127.0.0.1", port=8000)
