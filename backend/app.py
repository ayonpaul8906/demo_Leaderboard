# app.py
import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import List, Optional
from threading import Thread, Lock
from flask import Flask, jsonify, request
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP
import uuid
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit
import requests
import aiohttp

# ---------- CONFIG ----------
BASE_DIR = os.path.dirname(__file__)
PARTICIPANTS_FILE = os.path.join(BASE_DIR, "participants.json")
LABS_FILE = os.path.join(BASE_DIR, "labs.json")

FIREBASE_KEY_PATH = os.path.join(BASE_DIR, "firebase-admin-key.json")
FIREBASE_CREDENTIALS = os.environ.get("FIREBASE_CREDENTIALS")

# Tunables - keep conservative for Render free tier
CONCURRENCY = int(os.environ.get("CONCURRENCY", "4"))   # safe value; lower if memory issues
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "25"))
BATCH_DELAY = float(os.environ.get("BATCH_DELAY", "8.0"))
POLITE_DELAY = float(os.environ.get("POLITE_DELAY", "0.5"))
PAGE_RETRY_ATTEMPTS = int(os.environ.get("PAGE_RETRY_ATTEMPTS", "2"))
FETCH_TIMEOUT = int(os.environ.get("FETCH_TIMEOUT", "25000"))  # ms for aiohttp
UPDATE_INTERVAL_MINUTES = int(os.environ.get("UPDATE_INTERVAL", "60"))
AUTO_START = os.environ.get("AUTO_START", "true").lower() == "true"

RENDER_URL = os.environ.get("RENDER_URL", "")
KEEP_ALIVE = os.environ.get("KEEP_ALIVE", "true").lower() == "true"

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

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

# ---------- KEEP ALIVE ----------
def keep_alive_ping():
    if RENDER_URL and KEEP_ALIVE:
        try:
            logger.info("🏓 Keep-alive ping")
            requests.get(f"{RENDER_URL}/health", timeout=10)
        except Exception:
            pass

# ---------- UTILITIES: JSON endpoints for Cloud Skills Boost ----------
def extract_profile_id(url: str) -> Optional[str]:
    """
    Extract the profile ID from various forms of the profile field.
    Accepts plain id or full URL.
    """
    if not url:
        return None
    # If already just an id:
    if len(url) == 36 and "-" in url:
        return url
    # Try extract from URL
    import re
    m = re.search(r"public_profiles\/([a-zA-Z0-9\-]+)", url)
    if m:
        return m.group(1)
    # Try last path segment
    parts = url.strip().rstrip("/").split("/")
    if parts:
        candidate = parts[-1]
        if len(candidate) >= 8:
            return candidate
    return None

async def fetch_profile_json(session: aiohttp.ClientSession, profile_id: str) -> List[str]:
    """
    Fetch JSON endpoints for a profile and return a list of completed titles (strings).
    Endpoints tried: /badges and /quests (the site uses both).
    """
    base = f"https://www.cloudskillsboost.google/public_profiles/{profile_id}"
    endpoints = ["/badges", "/quests"]
    collected = []
    headers = {"User-Agent": "StudyJamsLeaderboard/1.0 (+your-email@example.com)"}
    for ep in endpoints:
        url = base + ep
        try:
            async with session.get(url, headers=headers, timeout=FETCH_TIMEOUT/1000) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # Some endpoints return JSON list; others may wrap in object. Try parse.
                    try:
                        data = json.loads(text)
                        if isinstance(data, list):
                            for item in data:
                                # item might be dict with 'title' or 'name' fields
                                if isinstance(item, dict):
                                    title = item.get("title") or item.get("name") or item.get("label")
                                    if title:
                                        collected.append(title.strip())
                        elif isinstance(data, dict):
                            # If object, try to find arrays inside
                            for v in data.values():
                                if isinstance(v, list):
                                    for item in v:
                                        if isinstance(item, dict):
                                            title = item.get("title") or item.get("name") or item.get("label")
                                            if title:
                                                collected.append(title.strip())
                    except Exception:
                        # not JSON? try simple heuristics: look for "title" using substrings
                        pass
                else:
                    # non-200 - ignore
                    pass
        except Exception as e:
            logger.debug(f"JSON fetch error for {url}: {e}")
    # dedupe and normalize
    unique = []
    lower_seen = set()
    for t in collected:
        tl = t.strip()
        if not tl:
            continue
        tlower = tl.lower()
        if tlower not in lower_seen:
            unique.append(tl)
            lower_seen.add(tlower)
    return unique

def match_target_labs_from_list(completed_titles: List[str]) -> List[str]:
    # match against TARGET_LABS (canonical case)
    matched = []
    lower_completed = set([c.strip().lower() for c in completed_titles])
    for lab, lab_lower in zip(TARGET_LABS, TARGET_LABS_LOWER):
        if lab_lower in lower_completed:
            matched.append(lab)
    return matched

# ---------- PROFILE CHECK & UPDATE ----------
async def fetch_and_update_profile(session: aiohttp.ClientSession, user: dict):
    """
    For a single user dict {id,name,profile,...}, fetch JSON endpoints and update Firestore.
    """
    name = user.get("name") or ""
    profile_field = user.get("profile") or ""
    profile_id = extract_profile_id(profile_field)
    user_id = str(user.get("id") or name or uuid.uuid4()).replace(" ", "_")

    if not profile_id:
        # write zero and return
        db.collection("leaderboard").document(user_id).set({
            "userId": user_id,
            "name": name,
            "profile": profile_field,
            "completed_labs": [],
            "completed_count": 0,
            "error": "invalid_profile",
            "last_updated": SERVER_TIMESTAMP
        }, merge=True)
        logger.info(f"{name}: invalid profile id -> skipped")
        return

    try:
        completed_titles = await fetch_profile_json(session, profile_id)
        matched = match_target_labs_from_list(completed_titles)

        db.collection("leaderboard").document(user_id).set({
            "userId": user_id,
            "name": name,
            "profile": profile_field,
            "completed_labs": matched,
            "completed_count": len(matched),
            "error": None,
            "last_updated": SERVER_TIMESTAMP
        }, merge=True)

        logger.info(f"Updated {name}: {len(matched)} labs ({len(completed_titles)} raw items)")

        # small polite delay
        await asyncio.sleep(POLITE_DELAY)

    except Exception as e:
        logger.exception(f"Failed updating {name}: {e}")
        db.collection("leaderboard").document(user_id).set({
            "userId": user_id,
            "name": name,
            "profile": profile_field,
            "error": str(e),
            "completed_count": 0,
            "last_updated": SERVER_TIMESTAMP
        }, merge=True)

# ---------- BATCH RUNNER ----------
async def run_full_update_async():
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
    logger.info(f"Starting update for {total} participants (batch_size={BATCH_SIZE}, concurrency={CONCURRENCY})")

    sem = asyncio.Semaphore(CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        async def guarded_update(user):
            async with sem:
                await fetch_and_update_profile(session, user)
                with update_lock:
                    update_status["progress"] += 1

        tasks = [guarded_update(u) for u in PARTICIPANTS]
        # run in batches to lower memory usage: schedule them but gather in chunks
        chunk = 100  # reduce memory by gathering in chunks
        for i in range(0, len(tasks), chunk):
            batch_tasks = tasks[i:i+chunk]
            await asyncio.gather(*batch_tasks, return_exceptions=True)
            logger.info(f"Processed {min(i+chunk, len(tasks))}/{len(tasks)}")
            await asyncio.sleep(BATCH_DELAY)  # small pause between big chunks

    logger.info("All profile updates attempted")
    with update_lock:
        update_status["running"] = False
        update_status["last_run_end"] = datetime.utcnow().isoformat()

def run_full_update():
    try:
        asyncio.run(run_full_update_async())
    except Exception as e:
        logger.error(f"Background update error: {e}")

# ---------- FLASK ROUTES ----------
@app.route("/profile-completed", methods=["GET"])
def api_profile_completed():
    """
    Quick test endpoint: /profile-completed?profileId=<id> OR ?profileUrl=<full-url>
    returns matched target labs and raw list.
    """
    profile_id = request.args.get("profileId") or None
    profile_url = request.args.get("profileUrl") or None
    if not profile_id and profile_url:
        profile_id = extract_profile_id(profile_url)
    if not profile_id:
        return jsonify({"error": "missing_profile_id"}), 400

    async def getit():
        async with aiohttp.ClientSession() as session:
            titles = await fetch_profile_json(session, profile_id)
            matched = match_target_labs_from_list(titles)
            return {"profileId": profile_id, "matched": matched, "raw_count": len(titles), "raw": titles[:100]}

    data = asyncio.run(getit())
    return jsonify(data)

@app.route("/trigger-update", methods=["GET"])
def trigger_manual_update():
    if update_status["running"]:
        return jsonify({"status": "already_running"}), 409
    Thread(target=run_full_update, daemon=True).start()
    return jsonify({"status": "update_started"})

@app.route("/update-status", methods=["GET"])
def get_update_status():
    s = update_status.copy()
    return jsonify(s)

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

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "ts": datetime.utcnow().isoformat()})

# ---------- SCHEDULER ----------
scheduler = BackgroundScheduler()

def scheduled_update():
    logger.info("Scheduled update triggered")
    keep_alive_ping()
    Thread(target=run_full_update, daemon=True).start()

scheduler.add_job(func=scheduled_update, trigger=IntervalTrigger(minutes=UPDATE_INTERVAL_MINUTES), id="leaderboard_update", name="Update leaderboard data", replace_existing=True)

if RENDER_URL and KEEP_ALIVE:
    scheduler.add_job(func=keep_alive_ping, trigger=IntervalTrigger(minutes=10), id="keep_alive", name="Keep alive ping", replace_existing=True)

def get_next_run_time():
    job = scheduler.get_job("leaderboard_update")
    return job.next_run_time.isoformat() if job and job.next_run_time else None

# ---------- STARTUP ----------
@app.before_request
def ensure_scheduler():
    if not scheduler.running:
        scheduler.start()
        if AUTO_START:
            Thread(target=run_full_update, daemon=True).start()

atexit.register(lambda: scheduler.shutdown() if scheduler.running else None)

if __name__ == "__main__":
    logger.info("Starting leaderboard server (json-based)")
    scheduler.start()
    if AUTO_START:
        Thread(target=run_full_update, daemon=True).start()
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
