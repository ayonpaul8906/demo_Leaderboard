# app.py (Optimized: resource-blocking, light waits, concurrency=2, shorter batch delay)
import asyncio
import json
import logging
import os
import sys
import gc
import uuid
from datetime import datetime
from typing import List
from threading import Thread, Lock
from bs4 import BeautifulSoup
from flask import Flask, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit
import requests
from playwright.async_api import async_playwright, Page, BrowserContext

# ---------- CONFIG ----------
BASE_DIR = os.path.dirname(__file__)
PARTICIPANTS_FILE = os.path.join(BASE_DIR, "participants.json")
LABS_FILE = os.path.join(BASE_DIR, "labs.json")

FIREBASE_KEY_PATH = os.path.join(BASE_DIR, "firebase-admin-key.json")
FIREBASE_CREDENTIALS = os.environ.get("FIREBASE_CREDENTIALS")

# Tunables (adjust via env if needed)
CONCURRENCY = int(os.environ.get("CONCURRENCY", "2"))   # try 1 or 2 on Render
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "25"))    # number per batch
BATCH_DELAY = float(os.environ.get("BATCH_DELAY", "10"))  # seconds between batches (reduced)
POLITE_DELAY = float(os.environ.get("POLITE_DELAY", "0.5"))  # seconds between users
PAGE_RETRY_ATTEMPTS = int(os.environ.get("PAGE_RETRY_ATTEMPTS", "2"))
FETCH_TIMEOUT = int(os.environ.get("FETCH_TIMEOUT", "45000"))  # ms
UPDATE_INTERVAL_MINUTES = int(os.environ.get("UPDATE_INTERVAL", "60"))
AUTO_START = os.environ.get("AUTO_START", "true").lower() == "true"

RENDER_URL = os.environ.get("RENDER_URL", "")
KEEP_ALIVE = os.environ.get("KEEP_ALIVE", "true").lower() == "true"

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("playwright").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ---------- FIREBASE INIT ----------
try:
    if FIREBASE_CREDENTIALS:
        cred_dict = json.loads(FIREBASE_CREDENTIALS)
        if "private_key" in cred_dict and isinstance(cred_dict["private_key"], str):
            cred_dict["private_key"] = cred_dict["private_key"].replace("\\n", "\n")
        cred = credentials.Certificate(cred_dict)
    elif os.path.exists(FIREBASE_KEY_PATH):
        cred = credentials.Certificate(FIREBASE_KEY_PATH)
    else:
        raise FileNotFoundError("No Firebase credentials found!")
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    logger.info("✅ Firebase initialized")
except Exception as e:
    logger.error(f"❌ Firebase init failed: {e}")
    raise

# ---------- LOAD CONFIG FILES ----------
try:
    with open(PARTICIPANTS_FILE, "r", encoding="utf-8") as f:
        PARTICIPANTS = json.load(f)
    logger.info(f"✅ Loaded {len(PARTICIPANTS)} participants")
except Exception as e:
    logger.error(f"Failed to load participants.json: {e}")
    PARTICIPANTS = []

try:
    with open(LABS_FILE, "r", encoding="utf-8") as f:
        TARGET_LABS = json.load(f)
    TARGET_LABS_LOWER = [t.strip().lower() for t in TARGET_LABS]
    logger.info(f"✅ Loaded {len(TARGET_LABS)} target labs")
except Exception as e:
    logger.error(f"Failed to load labs.json: {e}")
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
    "errors": [],
    "success_count": 0
}

# ---------- HELPERS ----------
def keep_alive_ping():
    if RENDER_URL and KEEP_ALIVE:
        try:
            logger.info("🏓 Keep-alive ping")
            requests.get(f"{RENDER_URL}/health", timeout=10)
        except Exception:
            pass

def parse_completed_labs(html_text: str) -> List[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    page_text = soup.get_text(separator="\n").lower()
    completed = []
    for i, lab in enumerate(TARGET_LABS_LOWER):
        if lab in page_text:
            completed.append(TARGET_LABS[i])
    return completed

async def create_light_context(browser) -> BrowserContext:
    """
    Create a context that blocks heavy resources (images, fonts, styles, media, trackers)
    to speed up loads and reduce memory/CPU.
    """
    context = await browser.new_context(
        viewport={"width": 1280, "height": 720},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )

    async def handler(route, request):
        # Block resource types that aren't needed for text scraping
        if request.resource_type in ("image", "font", "stylesheet", "media", "other"):
            await route.abort()
        else:
            await route.continue_()

    try:
        await context.route("**/*", handler)
    except Exception:
        # route may fail on some builds; ignore and continue
        pass
    return context

async def fetch_profile_light(page: Page, url: str) -> str:
    """
    Load a profile with retries. Prefer waiting for minimal selector if present,
    otherwise return content as soon as domcontentloaded arrives.
    """
    last_error = None
    for attempt in range(PAGE_RETRY_ATTEMPTS):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=FETCH_TIMEOUT)
            # Try minimal selectors that might indicate labs area.
            # These selectors are conservative — adjust if you know exact structure.
            selectors = [
                "div.profile",                # generic
                "div.completed-labs",         # hypothetical
                "section",                    # generic fallback
            ]
            got = False
            for sel in selectors:
                try:
                    await page.wait_for_selector(sel, timeout=2000)
                    got = True
                    break
                except Exception:
                    continue
            # no heavy fixed wait — fetch content immediately
            content = await page.content()
            return content
        except Exception as e:
            last_error = e
            logger.debug(f"Attempt {attempt+1}/{PAGE_RETRY_ATTEMPTS} for {url} failed: {e}")
            if attempt < PAGE_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(1)
    raise last_error

async def process_user_with_page(user: dict, page: Page):
    name = user.get("name", "Unknown").strip()
    profile = user.get("profile", "")
    user_id = str(user.get("id") or name or uuid.uuid4()).replace(" ", "_")

    try:
        logger.debug(f"Fetching {name} ({profile})")
        html = await fetch_profile_light(page, profile)
        # quick early check: if no target-lab keywords present, treat as 0 quickly
        low = html.lower()
        found_any = any(keyword in low for keyword in TARGET_LABS_LOWER[:5]) if TARGET_LABS_LOWER else False
        # parse fully regardless; parse_completed_labs is cheap
        completed = parse_completed_labs(html)
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
        logger.info(f"✅ {name}: {len(completed)} labs")
    except Exception as e:
        logger.error(f"❌ Error for {name}: {e}")
        with update_lock:
            update_status["errors"].append({"name": name, "error": str(e)})
            update_status["progress"] += 1
        try:
            db.collection("leaderboard").document(user_id).set({
                "userId": user_id,
                "name": name,
                "displayName": name,
                "profile": profile,
                "error": str(e),
                "last_updated": SERVER_TIMESTAMP,
                "completed_count": 0
            }, merge=True)
        except Exception:
            pass
    finally:
        await asyncio.sleep(POLITE_DELAY)

async def run_batch(batch: List[dict]):
    """
    Runs one batch: launches browser, creates N pages (N = CONCURRENCY),
    and processes users round-robin on those pages.
    """
    logger.info(f"    Launching browser for batch of {len(batch)} users (concurrency={CONCURRENCY})")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=[
                "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"
            ])
            # create light context that blocks heavy resources
            context = await create_light_context(browser)
            # create pages up to concurrency (reuse to reduce overhead)
            pages = []
            for _ in range(min(CONCURRENCY, len(batch))):
                pages.append(await context.new_page())

            # Round-robin assign tasks to pages using semaphore pattern
            sem = asyncio.Semaphore(CONCURRENCY)

            async def worker(user, page):
                async with sem:
                    await process_user_with_page(user, page)

            tasks = []
            for idx, user in enumerate(batch):
                page = pages[idx % len(pages)]
                tasks.append(worker(user, page))

            # gather with return_exceptions True to continue on errors
            await asyncio.gather(*tasks, return_exceptions=True)

            # cleanup
            for pg in pages:
                try:
                    await pg.close()
                except Exception:
                    pass
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Batch error: {e}")
        # if browser launch failed, mark progress for these users to avoid infinite loops
        with update_lock:
            update_status["progress"] += len(batch)
            update_status["errors"].append({"batch_error": str(e)})
    finally:
        # free memory
        gc.collect()

async def run_full_update():
    with update_lock:
        if update_status["running"]:
            logger.warning("Update already running — skipping")
            return
        update_status.update({
            "running": True,
            "progress": 0,
            "errors": [],
            "success_count": 0,
            "last_run_start": datetime.utcnow().isoformat()
        })

    total = len(PARTICIPANTS)
    logger.info(f"🚀 Starting update for {total} participants (batch_size={BATCH_SIZE}, concurrency={CONCURRENCY})")

    try:
        for batch_index in range(0, total, BATCH_SIZE):
            batch = PARTICIPANTS[batch_index: batch_index + BATCH_SIZE]
            logger.info(f"➡️ Batch {batch_index // BATCH_SIZE + 1}: {len(batch)} users")
            keep_alive_ping()
            await run_batch(batch)
            logger.info(f"Sleeping {BATCH_DELAY}s between batches (quick memory release)")
            await asyncio.sleep(BATCH_DELAY)

        logger.info(f"✅ Update finished. Success: {update_status['success_count']}/{total}")
        try:
            db.collection("metadata").document("last_update").set({
                "timestamp": SERVER_TIMESTAMP,
                "success_count": update_status["success_count"],
                "total_count": total,
                "errors": len(update_status["errors"])
            })
        except Exception as e:
            logger.warning(f"Failed to write metadata: {e}")

    except Exception as e:
        logger.error(f"Fatal error in run_full_update: {e}")

    finally:
        with update_lock:
            update_status["running"] = False
            update_status["last_run_end"] = datetime.utcnow().isoformat()
        gc.collect()

def background_update():
    try:
        asyncio.run(run_full_update())
    except Exception as e:
        logger.error(f"Background update error: {e}")

# ---------- SCHEDULER ----------
scheduler = BackgroundScheduler()

def scheduled_update():
    logger.info("⏰ Scheduled update triggered")
    keep_alive_ping()
    Thread(target=background_update, daemon=True).start()

scheduler.add_job(func=scheduled_update, trigger=IntervalTrigger(minutes=UPDATE_INTERVAL_MINUTES), id="leaderboard_update", name="Update leaderboard data", replace_existing=True)

if RENDER_URL and KEEP_ALIVE:
    scheduler.add_job(func=keep_alive_ping, trigger=IntervalTrigger(minutes=10), id="keep_alive", name="Keep alive ping", replace_existing=True)

def get_next_run_time():
    job = scheduler.get_job("leaderboard_update")
    return job.next_run_time.isoformat() if job and job.next_run_time else None

# ---------- ROUTES ----------
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "running",
        "message": "Study Jams Leaderboard API",
        "endpoints": {
            "health": "/health",
            "leaderboard": "/leaderboard-data",
            "status": "/update-status",
            "trigger": "/trigger-update"
        }
    })

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "participants_count": len(PARTICIPANTS),
        "scheduler_running": scheduler.running,
        "next_run": get_next_run_time(),
        "last_update": update_status.get("last_run_end"),
        "running": update_status.get("running"),
        "progress": update_status.get("progress")
    })

@app.route("/update-status", methods=["GET"])
def get_update_status():
    s = update_status.copy()
    s["next_run"] = get_next_run_time()
    return jsonify(s)

@app.route("/trigger-update", methods=["GET", "POST"])
def trigger_manual_update():
    if update_status["running"]:
        return jsonify({"status": "already_running", "progress": f"{update_status['progress']}/{update_status['total']}" }), 409
    Thread(target=background_update, daemon=True).start()
    return jsonify({"status": "update_started", "timestamp": datetime.utcnow().isoformat()})

@app.route("/leaderboard-data", methods=["GET"])
def get_leaderboard_data():
    try:
        docs = db.collection("leaderboard").stream()
        res = []
        for doc in docs:
            data = doc.to_dict()
            if data.get("test"):
                continue
            res.append({
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
        res.sort(key=lambda x: x["completed_count"], reverse=True)
        return jsonify({"data": res, "total": len(res), "timestamp": datetime.utcnow().isoformat()})
    except Exception as e:
        logger.error(f"Error fetching leaderboard: {e}")
        return jsonify({"error": str(e)}), 500

# ---------- STARTUP ----------
@app.before_request
def ensure_scheduler():
    if not scheduler.running:
        scheduler.start()
        logger.info(f"Scheduler started (interval={UPDATE_INTERVAL_MINUTES} minutes)")
        if AUTO_START:
            logger.info("Auto-starting initial update")
            Thread(target=background_update, daemon=True).start()

atexit.register(lambda: scheduler.shutdown() if scheduler.running else None)

if __name__ == "__main__":
    logger.info("Starting leaderboard server (optimized)")
    logger.info(f"Participants: {len(PARTICIPANTS)}, batch_size={BATCH_SIZE}, concurrency={CONCURRENCY}")
    scheduler.start()
    if AUTO_START:
        Thread(target=background_update, daemon=True).start()
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
