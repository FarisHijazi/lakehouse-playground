#!/usr/bin/env python3
"""
Generate realistic, messy podcast platform data simulating Thmanyah-like operations.

This generates intentionally imperfect data to practice real-world data engineering:
- Duplicate records
- Null values in unexpected places
- Mixed date formats
- Unicode/encoding issues (Arabic + English)
- Late-arriving data
- Schema changes over time
- Inconsistent casing and formatting
"""

import json
import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

# Seed for reproducibility
random.seed(42)
np.random.seed(42)

BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

# ============================================================
# Reference Data: Podcasts & Episodes (Arabic-inspired content)
# ============================================================

PODCASTS = [
    {"podcast_id": "pod_001", "name": "سوالف بزنس", "name_en": "Swalif Business", "category": "Business", "language": "ar", "host": "عبدالرحمن أبومالح", "created_at": "2019-03-15"},
    {"podcast_id": "pod_002", "name": "فنجان", "name_en": "Finjan", "category": "Society & Culture", "language": "ar", "host": "عبدالرحمن أبومالح", "created_at": "2018-01-10"},
    {"podcast_id": "pod_003", "name": "بودكاست أريكة", "name_en": "Ariika Podcast", "category": "Society & Culture", "language": "ar", "host": "سارة", "created_at": "2020-06-01"},
    {"podcast_id": "pod_004", "name": "حوارات", "name_en": "Hiwarat", "category": "Education", "language": "ar", "host": "محمد", "created_at": "2019-11-20"},
    {"podcast_id": "pod_005", "name": "Tech Talks Arabia", "name_en": "Tech Talks Arabia", "category": "Technology", "language": "mixed", "host": "Ahmed Al-Farsi", "created_at": "2020-02-14"},
    {"podcast_id": "pod_006", "name": "صحتك أولاً", "name_en": "Your Health First", "category": "Health", "language": "ar", "host": "د. نورة", "created_at": "2021-01-05"},
    {"podcast_id": "pod_007", "name": "كتاب واحد", "name_en": "One Book", "category": "Books", "language": "ar", "host": "خالد", "created_at": "2020-09-10"},
    {"podcast_id": "pod_008", "name": "The Saudi Startup Show", "name_en": "The Saudi Startup Show", "category": "Business", "language": "en", "host": "Reem Al-Saud", "created_at": "2021-04-01"},
    {"podcast_id": "pod_009", "name": "رحلة مع القرآن", "name_en": "Journey with Quran", "category": "Religion", "language": "ar", "host": "الشيخ عمر", "created_at": "2019-06-15"},
    {"podcast_id": "pod_010", "name": "مطبخ البودكاست", "name_en": "Podcast Kitchen", "category": "Food", "language": "ar", "host": "ليلى", "created_at": "2022-01-20"},
]

# Generate episodes for each podcast
def generate_episodes():
    episodes = []
    episode_id_counter = 1
    for podcast in PODCASTS:
        start_date = datetime.strptime(podcast["created_at"], "%Y-%m-%d")
        num_episodes = random.randint(20, 120)
        for i in range(num_episodes):
            ep_date = start_date + timedelta(days=i * random.randint(5, 14))
            if ep_date > datetime(2024, 12, 31):
                break
            duration = random.gauss(2700, 900)  # ~45 min avg, 15 min std
            duration = max(300, min(7200, duration))  # 5 min to 2 hours

            episode = {
                "episode_id": f"ep_{episode_id_counter:04d}",
                "podcast_id": podcast["podcast_id"],
                "title": f"Episode {i+1}: {'حلقة ' + str(i+1) if podcast['language'] == 'ar' else f'Topic {i+1}'}",
                "published_at": ep_date.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_seconds": int(duration),
                "season": (i // 12) + 1,
                "episode_number": i + 1,
            }
            episodes.append(episode)
            episode_id_counter += 1
    return episodes


# ============================================================
# User Generation (with realistic messiness)
# ============================================================

SAUDI_CITIES = ["Riyadh", "Jeddah", "Dammam", "Mecca", "Medina", "Khobar", "Tabuk", "Abha", "Taif", "Buraidah"]
COUNTRIES = ["SA", "AE", "KW", "BH", "QA", "OM", "EG", "JO", "LB", "MA", "US", "UK", "DE", "FR"]
PLATFORMS = ["ios", "android", "web", "smart_speaker", "car_play"]
SUBSCRIPTION_TYPES = ["free", "premium", "premium_annual", "trial"]

# Arabic first names
ARABIC_FIRST_NAMES = ["محمد", "أحمد", "عبدالله", "سلطان", "فهد", "نورة", "سارة", "ريم", "لمياء", "هند",
                      "خالد", "عمر", "ياسر", "طارق", "منى", "فاطمة", "عائشة", "مريم", "زينب", "حسن"]
ENGLISH_FIRST_NAMES = ["Mohammed", "Ahmed", "Abdullah", "Sultan", "Fahad", "Noura", "Sarah", "Reem", "Lamia", "Hind",
                       "Khalid", "Omar", "Yasser", "Tariq", "Mona", "Fatima", "Aisha", "Mariam", "Zainab", "Hassan"]


def generate_users(n=5000):
    users = []
    used_emails = set()

    for i in range(n):
        user_id = f"usr_{i+1:06d}"
        signup_date = datetime(2019, 1, 1) + timedelta(days=random.randint(0, 2100))

        # Randomly pick name style
        if random.random() < 0.6:
            name = random.choice(ARABIC_FIRST_NAMES)
        else:
            name = random.choice(ENGLISH_FIRST_NAMES)

        email_base = f"{name.lower().replace(' ', '.')}_{random.randint(1, 9999)}"
        email_domain = random.choice(["gmail.com", "hotmail.com", "yahoo.com", "outlook.sa", "icloud.com"])
        email = f"{email_base}@{email_domain}"

        # === INTRODUCE MESSINESS ===
        user = {
            "user_id": user_id,
            "name": name,
            "email": email,
            "country": random.choice(COUNTRIES),
            "city": random.choice(SAUDI_CITIES) if random.random() < 0.7 else None,
            "platform": random.choice(PLATFORMS),
            "signup_date": None,  # will be set below with format variation
            "subscription_type": random.choice(SUBSCRIPTION_TYPES),
            "age": random.randint(16, 65) if random.random() > 0.1 else None,
            "gender": random.choice(["M", "F", "male", "female", "m", "f", None]),  # inconsistent formats!
        }

        # Mixed date formats (a real problem!)
        if random.random() < 0.4:
            user["signup_date"] = signup_date.strftime("%Y-%m-%d")
        elif random.random() < 0.5:
            user["signup_date"] = signup_date.strftime("%d/%m/%Y")
        elif random.random() < 0.5:
            user["signup_date"] = signup_date.strftime("%m-%d-%Y")
        else:
            user["signup_date"] = signup_date.isoformat()

        # Null emails (5% of users)
        if random.random() < 0.05:
            user["email"] = None

        # Duplicate users (~3%)
        if random.random() < 0.03 and len(users) > 10:
            dup = users[random.randint(0, len(users) - 1)].copy()
            dup["user_id"] = user_id  # different ID, same person
            dup["signup_date"] = (signup_date + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
            user = dup

        users.append(user)

    return users


# ============================================================
# Listening Events (the core analytics data)
# ============================================================

def generate_listening_events(users, episodes, n=200000):
    events = []
    event_types = ["play", "pause", "resume", "complete", "skip", "seek"]

    # Create time-of-day distribution (more listening in evening in Saudi Arabia)
    # Peak hours: 9-11 PM (21-23), secondary peak: 8-10 AM
    hour_weights = [0.5, 0.3, 0.2, 0.1, 0.1, 0.2, 0.5, 1.0, 2.0, 2.5, 2.0, 1.5,
                    1.0, 1.0, 1.2, 1.5, 1.8, 2.0, 2.5, 3.0, 3.5, 4.0, 3.5, 2.0]

    # Power law for episode popularity (few episodes get most plays)
    episode_weights = np.random.pareto(1.5, len(episodes)) + 1
    episode_weights = episode_weights / episode_weights.sum()

    # Power law for user activity
    user_weights = np.random.pareto(2.0, len(users)) + 1
    user_weights = user_weights / user_weights.sum()

    for i in range(n):
        user = users[np.random.choice(len(users), p=user_weights)]
        episode = episodes[np.random.choice(len(episodes), p=episode_weights)]

        # Event timestamp: between episode publish and "now"
        ep_pub = datetime.strptime(episode["published_at"], "%Y-%m-%d %H:%M:%S")
        days_since = (datetime(2025, 1, 15) - ep_pub).days
        if days_since <= 0:
            continue

        event_day = ep_pub + timedelta(days=random.randint(0, min(days_since, 365)))
        hour = random.choices(range(24), weights=hour_weights, k=1)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        event_time = event_day.replace(hour=hour, minute=minute, second=second)

        # Listening duration (how much of the episode they listened to)
        completion_rate = random.betavariate(2, 3)  # Most people don't finish
        listened_seconds = int(episode["duration_seconds"] * completion_rate)

        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user["user_id"],
            "episode_id": episode["episode_id"],
            "event_type": random.choices(
                event_types,
                weights=[40, 15, 10, 20, 10, 5],
                k=1
            )[0],
            "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "listened_seconds": listened_seconds,
            "platform": user["platform"] if random.random() > 0.1 else random.choice(PLATFORMS),
            "country": user["country"],
            "app_version": f"{random.randint(2, 5)}.{random.randint(0, 15)}.{random.randint(0, 30)}",
        }

        # === MESSINESS ===
        # Late-arriving events (timestamp in the past but arriving "now") - mark with arrival lag
        if random.random() < 0.05:
            event["_arrived_late"] = True
            event["_arrival_lag_hours"] = random.randint(1, 72)

        # Missing fields (3%)
        if random.random() < 0.03:
            field = random.choice(["listened_seconds", "platform", "country"])
            event[field] = None

        # Duplicate events (~2%)
        if random.random() < 0.02:
            events.append(event.copy())  # exact duplicate

        # Bot-like behavior (rapid events from same user, unrealistic listening)
        if random.random() < 0.01:
            event["listened_seconds"] = episode["duration_seconds"]  # instant complete
            event["_suspicious"] = True

        events.append(event)

    return events


# ============================================================
# CDN / Streaming Quality Logs
# ============================================================

def generate_cdn_logs(listening_events, n=50000):
    logs = []
    isps = ["STC", "Mobily", "Zain", "du", "Etisalat", "Ooredoo", "Batelco", "Unknown"]
    qualities = ["128kbps", "256kbps", "64kbps", "320kbps"]
    errors = [None, None, None, None, None, "timeout", "403_forbidden", "buffer_underrun",
              "dns_failure", "connection_reset", None, None, None]

    for i in range(min(n, len(listening_events))):
        event = listening_events[i]
        log = {
            "log_id": str(uuid.uuid4()),
            "event_id": event["event_id"],
            "user_id": event["user_id"],
            "timestamp": event["timestamp"],
            "isp": random.choice(isps),
            "bitrate": random.choice(qualities),
            "buffer_events": random.randint(0, 5) if random.random() < 0.3 else 0,
            "rebuffer_ratio": round(random.uniform(0, 0.15), 4) if random.random() < 0.3 else 0.0,
            "startup_time_ms": int(random.gauss(1500, 800)),
            "error_type": random.choice(errors),
            "cdn_node": f"cdn-{random.choice(['ruh', 'jed', 'dxb', 'fra', 'ams'])}-{random.randint(1, 20):02d}",
            "bytes_transferred": random.randint(100000, 50000000),
        }
        # Negative startup time (bad sensor data)
        if random.random() < 0.02:
            log["startup_time_ms"] = -1 * abs(log["startup_time_ms"])

        logs.append(log)
    return logs


# ============================================================
# Ad Events
# ============================================================

def generate_ad_events(listening_events, n=30000):
    ad_types = ["pre_roll", "mid_roll", "post_roll"]
    ad_actions = ["impression", "click", "skip", "complete"]
    advertisers = ["Saudi Tourism", "STC Pay", "Jarir Bookstore", "Noon.com",
                   "Tamara BNPL", "HungerStation", "Careem", "Lucid Motors", "NEOM", "AlRajhi Bank"]
    campaigns = [f"campaign_{i:03d}" for i in range(1, 51)]

    ads = []
    for i in range(min(n, len(listening_events))):
        event = listening_events[i]
        if random.random() < 0.4:  # not all events have ads
            continue

        ad = {
            "ad_event_id": str(uuid.uuid4()),
            "event_id": event["event_id"],
            "user_id": event["user_id"],
            "timestamp": event["timestamp"],
            "ad_type": random.choice(ad_types),
            "action": random.choices(ad_actions, weights=[50, 5, 30, 15], k=1)[0],
            "advertiser": random.choice(advertisers),
            "campaign_id": random.choice(campaigns),
            "revenue_sar": round(random.uniform(0.01, 2.50), 4) if random.random() > 0.3 else 0.0,
            "duration_seconds": random.randint(5, 30),
        }
        ads.append(ad)
    return ads


# ============================================================
# Podcast Metadata Changes (for SCD practice)
# ============================================================

def generate_metadata_changes(podcasts):
    """Generate historical changes to podcast metadata (for SCD Type 2 practice)."""
    changes = []
    for podcast in podcasts:
        # Each podcast might have had name/category changes
        if random.random() < 0.4:
            change_date = datetime.strptime(podcast["created_at"], "%Y-%m-%d") + timedelta(days=random.randint(100, 500))
            changes.append({
                "podcast_id": podcast["podcast_id"],
                "field_changed": "category",
                "old_value": podcast["category"],
                "new_value": random.choice(["Society & Culture", "Education", "Business", "Lifestyle"]),
                "changed_at": change_date.strftime("%Y-%m-%d %H:%M:%S"),
            })
        if random.random() < 0.3:
            change_date = datetime.strptime(podcast["created_at"], "%Y-%m-%d") + timedelta(days=random.randint(200, 800))
            old_name = podcast["name"]
            changes.append({
                "podcast_id": podcast["podcast_id"],
                "field_changed": "name",
                "old_value": old_name,
                "new_value": old_name + " (محدث)",  # "updated"
                "changed_at": change_date.strftime("%Y-%m-%d %H:%M:%S"),
            })
    return changes


# ============================================================
# Write Data Files
# ============================================================

def write_json(data, filepath):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Written {len(data)} records to {filepath}")


def write_jsonl(data, filepath):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"  Written {len(data)} records to {filepath}")


def write_csv(data, filepath):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    if not data:
        return
    keys = data[0].keys()
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    print(f"  Written {len(data)} records to {filepath}")


def main():
    print("=" * 60)
    print("Generating Podcast Platform Data")
    print("=" * 60)

    # 1. Podcasts
    print("\n[1/7] Generating podcast metadata...")
    write_json(PODCASTS, RAW_DIR / "podcasts.json")

    # 2. Episodes
    print("[2/7] Generating episodes...")
    episodes = generate_episodes()
    write_json(episodes, RAW_DIR / "episodes.json")

    # 3. Users (CSV - because that's how you'd get a user export)
    print("[3/7] Generating users (with intentional data quality issues)...")
    users = generate_users(5000)
    write_csv(users, RAW_DIR / "users.csv")

    # 4. Listening events (JSONL - simulating streaming event logs)
    print("[4/7] Generating listening events...")
    events = generate_listening_events(users, episodes, 200000)
    # Split into daily files to simulate real ingestion
    events_by_date = {}
    for event in events:
        date = event["timestamp"][:10]
        events_by_date.setdefault(date, []).append(event)

    events_dir = RAW_DIR / "listening_events"
    events_dir.mkdir(parents=True, exist_ok=True)
    for date, date_events in sorted(events_by_date.items()):
        write_jsonl(date_events, events_dir / f"events_{date}.jsonl")
    print(f"  Total events: {len(events)} across {len(events_by_date)} days")

    # 5. CDN logs (CSV - simulating CDN log exports)
    print("[5/7] Generating CDN/streaming quality logs...")
    cdn_logs = generate_cdn_logs(events, 50000)
    write_csv(cdn_logs, RAW_DIR / "cdn_logs.csv")

    # 6. Ad events (JSON)
    print("[6/7] Generating ad events...")
    ad_events = generate_ad_events(events, 30000)
    write_json(ad_events, RAW_DIR / "ad_events.json")

    # 7. Metadata changes (for SCD practice)
    print("[7/7] Generating podcast metadata changes...")
    metadata_changes = generate_metadata_changes(PODCASTS)
    write_json(metadata_changes, RAW_DIR / "metadata_changes.json")

    print("\n" + "=" * 60)
    print("Data generation complete!")
    print(f"Raw data directory: {RAW_DIR}")
    print("=" * 60)

    # Summary
    print(f"\nData Quality Issues Introduced:")
    print(f"  - Mixed date formats in users.csv (YYYY-MM-DD, DD/MM/YYYY, MM-DD-YYYY, ISO)")
    print(f"  - ~5% null emails in users")
    print(f"  - ~3% duplicate users (same person, different ID)")
    print(f"  - ~10% null cities")
    print(f"  - Inconsistent gender values (M/F/male/female/m/f/null)")
    print(f"  - ~2% duplicate listening events")
    print(f"  - ~5% late-arriving events")
    print(f"  - ~3% null fields in events")
    print(f"  - ~1% bot-like suspicious events")
    print(f"  - ~2% negative startup times in CDN logs")
    print(f"  - Arabic + English mixed text in podcast names")
    print(f"  - SCD scenarios in metadata changes")


if __name__ == "__main__":
    main()
