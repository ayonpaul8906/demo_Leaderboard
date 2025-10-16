import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import List
from threading import Thread, Lock
from bs4 import BeautifulSoup
from flask import Flask, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP
import uuid
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit
import requests

# Playwright imports
from playwright.async_api import async_playwright, Browser, Page

# ---------- CONFIG ----------
BASE_DIR = os.path.dirname(__file__)
PARTICIPANTS_FILE = os.path.join(BASE_DIR, "participants.json")
LABS_FILE = os.path.join(BASE_DIR, "labs.json")

# For Render: Use environment variable for Firebase credentials
FIREBASE_KEY_PATH = os.path.join(BASE_DIR, "firebase-admin-key.json")
FIREBASE_CREDENTIALS = os.environ.get("FIREBASE_CREDENTIALS")  # JSON string

# Performance settings - Optimized for Render
CONCURRENCY = int(os.environ.get("CONCURRENCY", 3))  # Lower for Render's limited resources
FETCH_TIMEOUT = 20000
POLITE_DELAY = float(os.environ.get("POLITE_DELAY", 1.0))

# Scheduler settings
UPDATE_INTERVAL_MINUTES = int(os.environ.get("UPDATE_INTERVAL", 30))
AUTO_START = os.environ.get("AUTO_START", "true").lower() == "true"

# Render-specific: Keep-alive URL (your deployed URL)
RENDER_URL = os.environ.get("RENDER_URL", "")  # e.g., "https://your-app.onrender.com"
KEEP_ALIVE = os.environ.get("KEEP_ALIVE", "true").lower() == "true"

# ---------- LOGGING SETUP ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ---------- INIT FIREBASE ----------
try:
    # Try environment variable first (better for Render)
    if FIREBASE_CREDENTIALS:
        logger.info("Loading Firebase credentials from environment variable")
        import json
        cred_dict = json.loads(FIREBASE_CREDENTIALS)
        cred = credentials.Certificate(cred_dict)
    elif os.path.exists(FIREBASE_KEY_PATH):
        logger.info("Loading Firebase credentials from file")
        cred = credentials.Certificate(FIREBASE_KEY_PATH)
    else:
        raise FileNotFoundError("No Firebase credentials found!")
    
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    logger.info("✅ Firebase initialized successfully")
except Exception as e:
    logger.error(f"❌ Firebase initialization failed: {str(e)}")
    raise

# ---------- LOAD FILES ----------
try:
    with open(PARTICIPANTS_FILE, "r", encoding="utf-8") as f:
        PARTICIPANTS = json.load(f)
    logger.info(f"✅ Loaded {len(PARTICIPANTS)} participants")

    with open(LABS_FILE, "r", encoding="utf-8") as f:
        TARGET_LABS = json.load(f)
    logger.info(f"✅ Loaded {len(TARGET_LABS)} target labs")

    TARGET_LABS_LOWER = [t.strip().lower() for t in TARGET_LABS]
except Exception as e:
    logger.error(f"❌ Failed to load configuration files: {str(e)}")
    PARTICIPANTS = []
    TARGET_LABS = []
    TARGET_LABS_LOWER = []

# ---------- FLASK ----------
app = Flask(__name__)
CORS(app)

# ---------- GLOBAL STATE ----------
update_lock = Lock()
update_status = {
    "running": False,
    "progress": 0,
    "total": len(PARTICIPANTS),
    "last_run_start": None,
    "last_run_end": None,
    "next_run": None,
    "errors": [],
    "success_count": 0
}

# ---------- KEEP-ALIVE SYSTEM ----------
def keep_alive_ping():
    """Ping our own server to prevent Render from sleeping."""
    if RENDER_URL and KEEP_ALIVE:
        try:
            logger.info(f"🏓 Keep-alive ping to {RENDER_URL}")
            requests.get(f"{RENDER_URL}/health", timeout=10)
        except Exception as e:
            logger.warning(f"Keep-alive ping failed: {str(e)}")

# ---------- UTILITIES ----------
def parse_completed_labs(html_text: str) -> List[str]:
    """Parse the labs completed by scanning text."""
    soup = BeautifulSoup(html_text, "html.parser")
    page_text = soup.get_text(separator="\n").lower()
    
    completed = []
    for i, lab in enumerate(TARGET_LABS_LOWER):
        if lab in page_text:
            completed.append(TARGET_LABS[i])
    
    return completed

async def fetch_profile_playwright(page: Page, url: str) -> str:
    """Fetch participant profile HTML using Playwright."""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=FETCH_TIMEOUT)
        await page.wait_for_timeout(2000)
        return await page.content()
    except Exception as e:
        logger.error(f"Error fetching {url}: {str(e)}")
        raise

async def fetch_and_update(user: dict, browser: Browser, sem: asyncio.Semaphore):
    """Fetch profile, parse labs, update Firestore."""
    name = user.get("name", "Unknown User").strip()
    profile = user.get("profile")
    user_id = str(user.get("id") or name or uuid.uuid4()).replace(" ", "_")

    page = None
    
    async with sem:
        try:
            page = await browser.new_page()
            
            logger.info(f"📥 Fetching: {name}")
            html = await fetch_profile_playwright(page, profile)
            
            completed = parse_completed_labs(html)
            logger.info(f"✅ {name}: {len(completed)} labs")
            
            doc_data = {
                "userId": user_id,
                "name": name,
                "displayName": name,
                "email": user.get("email", ""),
                "profilePic": user.get("profilePic", ""),
                "profile": profile,
                "completed_labs": completed,
                "completed_count": len(completed),
                "last_updated": SERVER_TIMESTAMP,
                "error": None
            }
            
            db.collection("leaderboard").document(user_id).set(doc_data, merge=True)
            
            with update_lock:
                update_status["progress"] += 1
                update_status["success_count"] += 1
            
        except Exception as e:
            logger.error(f"❌ Error with {name}: {str(e)}")
            
            with update_lock:
                update_status["errors"].append({"name": name, "error": str(e)})
                update_status["progress"] += 1
            
            try:
                db.collection("leaderboard").document(user_id).set({
                    "userId": user_id,
                    "name": name,
                    "displayName": name,
                    "email": user.get("email", ""),
                    "profile": profile,
                    "error": str(e),
                    "last_updated": SERVER_TIMESTAMP,
                    "completed_count": 0
                }, merge=True)
            except:
                pass
        finally:
            if page:
                await page.close()
            await asyncio.sleep(POLITE_DELAY)

async def run_full_update():
    """Run all participant updates concurrently."""
    with update_lock:
        if update_status["running"]:
            logger.warning("⚠️ Update already running, skipping...")
            return
        
        update_status["running"] = True
        update_status["progress"] = 0
        update_status["errors"] = []
        update_status["success_count"] = 0
        update_status["last_run_start"] = datetime.utcnow().isoformat()
    
    logger.info(f"🚀 Starting automated update for {len(PARTICIPANTS)} participants")
    
    try:
        async with async_playwright() as p:
            # Launch with minimal resources for Render
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',  # Important for low-memory environments
                    '--disable-gpu',
                    '--no-first-run',
                    '--no-zygote',
                    '--single-process',  # Use single process to save memory
                ]
            )
            
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = [fetch_and_update(u, browser, sem) for u in PARTICIPANTS]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            await browser.close()
        
        logger.info(f"✅ Update completed. Success: {update_status['success_count']}/{len(PARTICIPANTS)}")
        
        # Update metadata
        db.collection("metadata").document("last_update").set({
            "timestamp": SERVER_TIMESTAMP,
            "success_count": update_status["success_count"],
            "total_count": len(PARTICIPANTS),
            "errors": len(update_status["errors"])
        })
        
    except Exception as e:
        logger.error(f"❌ Fatal error in update: {str(e)}")
    finally:
        with update_lock:
            update_status["running"] = False
            update_status["last_run_end"] = datetime.utcnow().isoformat()

def background_update():
    """Run update in background thread."""
    try:
        asyncio.run(run_full_update())
    except Exception as e:
        logger.error(f"Background update error: {str(e)}")

# ---------- SCHEDULER ----------
scheduler = BackgroundScheduler()

def scheduled_update():
    """Called by scheduler - also pings keep-alive."""
    logger.info("⏰ Scheduled update triggered")
    keep_alive_ping()  # Keep Render awake
    Thread(target=background_update, daemon=True).start()

# Schedule the main update job
scheduler.add_job(
    func=scheduled_update,
    trigger=IntervalTrigger(minutes=UPDATE_INTERVAL_MINUTES),
    id='leaderboard_update',
    name='Update leaderboard data',
    replace_existing=True
)

# Schedule keep-alive pings every 10 minutes (more frequent than updates)
if RENDER_URL and KEEP_ALIVE:
    scheduler.add_job(
        func=keep_alive_ping,
        trigger=IntervalTrigger(minutes=10),
        id='keep_alive',
        name='Keep Render awake',
        replace_existing=True
    )
    logger.info("✅ Keep-alive job scheduled every 10 minutes")

def get_next_run_time():
    if scheduler.running:
        job = scheduler.get_job('leaderboard_update')
        if job and job.next_run_time:
            return job.next_run_time.isoformat()
    return None

# ---------- ROUTES ----------
@app.route("/", methods=["GET"])
def home():
    """Home endpoint for health checks."""
    return jsonify({
        "status": "running",
        "message": "Google Cloud Study Jams Leaderboard API",
        "endpoints": {
            "health": "/health",
            "leaderboard": "/leaderboard-data",
            "status": "/update-status",
            "trigger": "/trigger-update (POST)"
        }
    })

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "participants_count": len(PARTICIPANTS),
        "labs_count": len(TARGET_LABS),
        "scheduler_running": scheduler.running,
        "update_interval_minutes": UPDATE_INTERVAL_MINUTES,
        "next_scheduled_update": get_next_run_time(),
        "last_update": update_status.get("last_run_end"),
        "keep_alive_enabled": KEEP_ALIVE and bool(RENDER_URL)
    })

@app.route("/update-status", methods=["GET"])
def get_update_status():
    """Get current update status."""
    status = update_status.copy()
    status["next_run"] = get_next_run_time()
    return jsonify(status)

@app.route("/trigger-update", methods=["POST", "GET"])  # Allow GET for easy testing
def trigger_manual_update():
    """Manually trigger an update."""
    if update_status["running"]:
        return jsonify({
            "status": "already_running",
            "progress": f"{update_status['progress']}/{update_status['total']}"
        }), 409
    
    logger.info("🔧 Manual update triggered")
    Thread(target=background_update, daemon=True).start()
    
    return jsonify({
        "status": "update_started",
        "message": "Manual update triggered",
        "timestamp": datetime.utcnow().isoformat()
    })

@app.route("/leaderboard-data", methods=["GET"])
def get_leaderboard_data():
    """Fetch leaderboard data sorted by completed labs."""
    try:
        docs = db.collection("leaderboard").stream()
        leaderboard = []
        
        for doc in docs:
            data = doc.to_dict()
            if data.get("test"):
                continue
                
            leaderboard.append({
                "userId": data.get("userId"),
                "name": data.get("name"),
                "displayName": data.get("displayName", data.get("name")),
                "email": data.get("email", ""),
                "profilePic": data.get("profilePic", ""),
                "completed_count": data.get("completed_count", 0),
                "completed_labs": data.get("completed_labs", []),
                "profile": data.get("profile"),
                "last_updated": data.get("last_updated"),
                "error": data.get("error")
            })

        leaderboard.sort(key=lambda x: x["completed_count"], reverse=True)
        
        return jsonify({
            "data": leaderboard,
            "total": len(leaderboard),
            "timestamp": datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"❌ Error fetching leaderboard: {str(e)}")
        return jsonify({"error": str(e)}), 500

# ---------- STARTUP ----------
@app.before_request
def before_first_request():
    """Initialize scheduler on first request."""
    if not scheduler.running:
        scheduler.start()
        logger.info(f"✅ Scheduler started - Updates every {UPDATE_INTERVAL_MINUTES} minutes")
        
        if AUTO_START:
            logger.info("🚀 Running initial update...")
            Thread(target=background_update, daemon=True).start()

atexit.register(lambda: scheduler.shutdown() if scheduler.running else None)

# ---------- MAIN ----------
if __name__ == "__main__":
    logger.info("🚀 Starting Render-optimized leaderboard server...")
    logger.info(f"📊 Participants: {len(PARTICIPANTS)}")
    logger.info(f"📚 Labs: {len(TARGET_LABS)}")
    logger.info(f"⏰ Auto-update interval: {UPDATE_INTERVAL_MINUTES} minutes")
    logger.info(f"🚦 Auto-start: {AUTO_START}")
    logger.info(f"🏓 Keep-alive: {KEEP_ALIVE}")
    
    scheduler.start()
    logger.info("✅ Scheduler started")
    
    if AUTO_START:
        logger.info("🚀 Running initial update on startup...")
        Thread(target=background_update, daemon=True).start()
    
    # Render uses PORT environment variable
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)