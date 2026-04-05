from __future__ import annotations

import argparse
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from fraud_pipeline.database import init_db, insert_transactions


CITY_PROFILES = [
    {"city": "New York", "country": "USA", "lat": 40.7128, "lon": -74.0060},
    {"city": "San Francisco", "country": "USA", "lat": 37.7749, "lon": -122.4194},
    {"city": "Chicago", "country": "USA", "lat": 41.8781, "lon": -87.6298},
    {"city": "Miami", "country": "USA", "lat": 25.7617, "lon": -80.1918},
    {"city": "Austin", "country": "USA", "lat": 30.2672, "lon": -97.7431},
    {"city": "London", "country": "UK", "lat": 51.5072, "lon": -0.1276},
    {"city": "Toronto", "country": "Canada", "lat": 43.6532, "lon": -79.3832},
    {"city": "Singapore", "country": "Singapore", "lat": 1.3521, "lon": 103.8198},
]

MERCHANT_PROFILES = [
    {"name": "NovaFuel", "category": "Fuel", "amount_range": (35, 95)},
    {"name": "BlueCart", "category": "Groceries", "amount_range": (18, 220)},
    {"name": "Cloud9 Travel", "category": "Travel", "amount_range": (180, 1400)},
    {"name": "Electra Hub", "category": "Electronics", "amount_range": (120, 2800)},
    {"name": "Urban Stitch", "category": "Retail", "amount_range": (25, 320)},
    {"name": "Night Owl Bar", "category": "Entertainment", "amount_range": (15, 180)},
    {"name": "Pulse Pharmacy", "category": "Healthcare", "amount_range": (12, 160)},
    {"name": "QuickTap", "category": "Digital Wallet", "amount_range": (5, 110)},
    {"name": "AeroStay", "category": "Hospitality", "amount_range": (80, 900)},
    {"name": "MarketSquare", "category": "Marketplace", "amount_range": (10, 650)},
]

CHANNELS = ["chip", "swipe", "tap", "online"]
ENTRY_MODES = ["card_present", "manual_keyed", "wallet_token", "ecommerce"]
CARD_TIERS = ["standard", "gold", "platinum", "business"]

SCENARIO_WEIGHTS = [
    ("normal", 0.68),
    ("amount_spike", 0.08),
    ("impossible_travel", 0.06),
    ("high_velocity", 0.06),
    ("merchant_burst", 0.04),
    ("late_night", 0.02),
    ("cross_border", 0.02),
    ("account_takeover", 0.04),
]


@dataclass
class CardState:
    card_id: str
    customer_id: str
    home_city: str
    home_country: str
    base_amount: float
    preferred_categories: list[str]
    card_tier: str
    last_event: datetime = field(default_factory=lambda: datetime.now(UTC) - timedelta(minutes=30))
    last_city: str = ""
    last_country: str = ""
    last_amount: float = 0.0
    last_device: str = ""


def _build_cards(count: int = 80) -> list[CardState]:
    cards: list[CardState] = []
    for idx in range(count):
        city = random.choice(CITY_PROFILES)
        preferred = random.sample([merchant["category"] for merchant in MERCHANT_PROFILES], k=3)
        cards.append(
            CardState(
                card_id=f"4111-XXXX-XXXX-{1000 + idx}",
                customer_id=f"CUST-{10000 + idx}",
                home_city=city["city"],
                home_country=city["country"],
                base_amount=round(random.uniform(20, 250), 2),
                preferred_categories=preferred,
                card_tier=random.choice(CARD_TIERS),
                last_city=city["city"],
                last_country=city["country"],
                last_device=f"device-{idx:04d}",
            )
        )
    return cards


def _choose_city(home_city: str, scenario: str) -> dict[str, Any]:
    if scenario in {"impossible_travel", "cross_border", "account_takeover"}:
        candidates = [city for city in CITY_PROFILES if city["city"] != home_city]
        if scenario == "cross_border":
            international = [city for city in candidates if city["country"] != "USA"]
            if international:
                return random.choice(international)
        return random.choice(candidates)

    if random.random() < 0.82:
        for city in CITY_PROFILES:
            if city["city"] == home_city:
                return city
    return random.choice(CITY_PROFILES)


def _choose_merchant(preferred_categories: list[str], scenario: str) -> dict[str, Any]:
    if scenario == "merchant_burst":
        for merchant in MERCHANT_PROFILES:
            if merchant["category"] == "Digital Wallet":
                return merchant

    weighted = []
    for merchant in MERCHANT_PROFILES:
        weight = 3 if merchant["category"] in preferred_categories else 1
        weighted.extend([merchant] * weight)
    return random.choice(weighted)


def _amount_for_scenario(base_amount: float, merchant: dict[str, Any], scenario: str) -> float:
    low, high = merchant["amount_range"]
    if scenario == "amount_spike":
        return round(max(random.uniform(700, 4500), base_amount * random.uniform(4.0, 8.5)), 2)
    if scenario == "high_velocity":
        return round(random.uniform(8, 42), 2)
    if scenario == "merchant_burst":
        return round(random.uniform(1, 15), 2)
    return round(min(random.uniform(low, high), base_amount * random.uniform(0.8, 2.4)), 2)


def _scenario_time(card: CardState, scenario: str) -> datetime:
    if scenario == "high_velocity":
        delta = timedelta(seconds=random.randint(20, 180))
    elif scenario == "merchant_burst":
        delta = timedelta(seconds=random.randint(10, 90))
    elif scenario == "impossible_travel":
        delta = timedelta(minutes=random.randint(8, 45))
    elif scenario == "account_takeover":
        delta = timedelta(minutes=random.randint(3, 18))
    elif scenario == "late_night":
        base = datetime.now(UTC).replace(
            hour=random.randint(1, 4),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
            microsecond=0,
        )
        if base <= card.last_event:
            base = card.last_event + timedelta(minutes=random.randint(5, 15))
        return base
    else:
        delta = timedelta(minutes=random.randint(4, 55))
    return card.last_event + delta


def _pick_scenario() -> str:
    scenarios = [name for name, _ in SCENARIO_WEIGHTS]
    weights = [weight for _, weight in SCENARIO_WEIGHTS]
    return random.choices(scenarios, weights=weights, k=1)[0]


def generate_transactions(cards: list[CardState], batch_size: int = 12) -> list[dict[str, Any]]:
    batch: list[dict[str, Any]] = []
    for _ in range(batch_size):
        card = random.choice(cards)
        scenario = _pick_scenario()
        city = _choose_city(card.home_city, scenario)
        merchant = _choose_merchant(card.preferred_categories, scenario)
        amount = _amount_for_scenario(card.base_amount, merchant, scenario)
        event_time = _scenario_time(card, scenario)

        if scenario == "cross_border":
            channel = "online"
            entry_mode = "ecommerce"
        elif scenario in {"merchant_burst", "account_takeover"}:
            channel = "online"
            entry_mode = "wallet_token" if scenario == "merchant_burst" else "ecommerce"
        else:
            channel = random.choice(CHANNELS)
            entry_mode = random.choice(ENTRY_MODES)

        device_id = (
            f"device-{random.randint(6000, 9999)}"
            if scenario == "account_takeover"
            else card.last_device if scenario in {"high_velocity", "merchant_burst"} else f"device-{random.randint(1000, 9999)}"
        )

        record = {
            "transaction_id": str(uuid.uuid4()),
            "event_timestamp": event_time.isoformat(),
            "ingest_timestamp": datetime.now(UTC).isoformat(),
            "card_id": card.card_id,
            "customer_id": card.customer_id,
            "card_tier": card.card_tier,
            "merchant_name": merchant["name"],
            "merchant_category": merchant["category"],
            "merchant_city": city["city"],
            "merchant_country": city["country"],
            "amount": amount,
            "currency": "USD",
            "channel": channel,
            "entry_mode": entry_mode,
            "device_id": device_id,
            "ip_address": f"10.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "latitude": city["lat"] + random.uniform(-0.05, 0.05),
            "longitude": city["lon"] + random.uniform(-0.05, 0.05),
            "home_city": card.home_city,
            "home_country": card.home_country,
            "scenario_label": scenario,
        }
        batch.append(record)

        card.last_event = event_time
        card.last_city = city["city"]
        card.last_country = city["country"]
        card.last_amount = amount
        card.last_device = device_id
    return batch


def run_stream(batch_size: int, sleep_seconds: float, iterations: int | None, seed: int | None) -> None:
    if seed is not None:
        random.seed(seed)

    init_db()
    cards = _build_cards()
    cycle = 0
    while iterations is None or cycle < iterations:
        batch = generate_transactions(cards, batch_size=batch_size)
        inserted = insert_transactions(batch)
        latest = batch[-1]["event_timestamp"] if batch else "n/a"
        print(f"[stream] inserted={inserted} latest_event={latest}")
        cycle += 1
        if iterations is None or cycle < iterations:
            time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate streaming fake credit-card transactions.")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of transactions per micro-batch.")
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Pause between batches.")
    parser.add_argument("--iterations", type=int, default=None, help="Number of batches to emit. Omit to stream forever.")
    parser.add_argument("--seed", type=int, default=7, help="Random seed for reproducible demos.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_stream(
        batch_size=args.batch_size,
        sleep_seconds=args.sleep_seconds,
        iterations=args.iterations,
        seed=args.seed,
    )
