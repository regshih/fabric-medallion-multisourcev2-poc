#!/usr/bin/env python3
"""Generate synthetic Cosmos DB for NoSQL documents for the three containers
that source the Cosmos-DB half of this POC: digitalSessions, devices, fraudAlerts.

Produces JSON Lines files under ``--out-dir`` (default ``./data/cosmos/``), one
file per container per batch (``initial`` and ``incremental``), loadable with
``cosmos/load_initial.py`` / ``cosmos/load_incremental.py``.

Cross-source business-key convention (must match generators/generate_databricks_data.py,
which is written and run independently by another agent — the two sources are never
directly connected, so this convention is what makes them joinable):

    customerId = "CUST-" + 6-digit zero-padded number   e.g. CUST-000042
    deviceId   = "DEV-"  + 6-digit zero-padded number    e.g. DEV-000042
    transactionId (fraudAlerts only, referencing a Databricks transaction)
               = "TXN-" + 9-digit zero-padded number     e.g. TXN-000000001

customerId values are drawn from 1..--customers (default 25000, matching
CUSTOMER_COUNT in .env.example), which is a subset of the Databricks
generator's own customer ID space — so every customerId this script produces
is guaranteed to also be a valid Databricks customerId, giving genuine
cross-source overlap by construction (not verified against the other
generator's actual output, since the two run independently).

Deliberate, bounded schema variation (to genuinely exercise Cosmos DB's
schema flexibility, not just claim it):
  - ~1 in 6 `devices` documents omit `geoHistory` entirely (never seen away
    from home, or a brand-new device with no travel history yet).
  - `fraudAlerts` documents only carry a `resolution` object once `status`
    is "resolved" or "dismissed" — open/investigating alerts have no
    resolution field at all, not a null placeholder.

All values (names are not used; only fictional IDs, IPs, notes, and device
fingerprints) are synthetic. IP addresses are drawn from the IETF
documentation ranges (RFC 5737 / RFC 3849) so they can never resolve to a
real individual or system. No value is sourced from a real person.

Usage:
    python generators/generate_cosmos_data.py --help
    python generators/generate_cosmos_data.py --seed 42 --batch all
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03dZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()
logger = logging.getLogger("generate_cosmos_data")

DEFAULT_SEED = 20260831
DEFAULT_AS_OF = "2026-08-31T12:00:00Z"
DEFAULT_CUSTOMERS = 25_000
DEFAULT_DEVICES = 5_000
DEFAULT_SESSIONS = 20_000
DEFAULT_ALERTS = 1_500
CONTAINERS = ("digitalSessions", "devices", "fraudAlerts")

# RFC 5737 / RFC 3849 documentation-only address blocks — never real hosts.
_DOC_IP_BLOCKS = ("192.0.2.", "198.51.100.", "203.0.113.")

_SYSTEMS = [
    ("iOS 18.3", "Mobile"),
    ("Android 15", "Mobile"),
    ("Windows 11", "Desktop"),
    ("macOS 15", "Desktop"),
    ("iPadOS 18", "Tablet"),
]
_CITIES = [
    ("US", "NY", "New York"),
    ("US", "WA", "Seattle"),
    ("US", "IL", "Chicago"),
    ("US", "TX", "Austin"),
    ("US", "CA", "San Francisco"),
    ("CA", "ON", "Toronto"),
]
_UNUSUAL_CITIES = [("GB", "ENG", "London"), ("NG", "LA", "Lagos"), ("RO", "B", "Bucharest")]
_AUTH_METHODS = ("password", "password+mfa", "biometric", "passkey")
_ACTIVITY_TYPES = ("login", "viewBalance", "transferFunds", "payBill", "viewStatement", "updateProfile", "logout")
_RISK_SIGNALS = (
    "emulator_detected",
    "rooted_device",
    "jailbroken_device",
    "impossible_travel",
    "new_device_high_risk_country",
    "tor_exit_node_ip",
)
_ALERT_TYPES = ("accountTakeover", "transactionAnomaly", "syntheticIdentity", "cardTesting", "moneyMuleActivity")
_SEVERITIES = ("low", "medium", "high", "critical")
_ALERT_SIGNALS = (
    "velocityAnomaly",
    "deviceMismatch",
    "geoImpossibleTravel",
    "knownFraudDevice",
    "unusualTransactionAmount",
    "newPayeeHighRisk",
)
_NOTE_TEMPLATES = (
    "Reviewed transaction history; escalated to Tier 2.",
    "Contacted customer via out-of-band channel; awaiting response.",
    "Device fingerprint matches a prior confirmed-fraud case.",
    "No corroborating signals found; monitoring for 48h.",
    "Customer confirmed the activity was authorized.",
    "Pattern consistent with a known synthetic-identity ring.",
)


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def customer_id(n: int) -> str:
    return f"CUST-{n:06d}"


def device_id(n: int) -> str:
    return f"DEV-{n:06d}"


def transaction_id(n: int) -> str:
    return f"TXN-{n:09d}"


def _fictional_ip(fake: Faker) -> str:
    block = fake.random_element(_DOC_IP_BLOCKS)
    return f"{block}{fake.random_int(1, 254)}"


def _generate_devices(fake: Faker, *, customer_count: int, device_count: int, anchor: datetime) -> list[dict[str, Any]]:
    devices: list[dict[str, Any]] = []
    for i in range(1, device_count + 1):
        # Deterministic (not fake.random_int) so generate_incremental() can compute the
        # exact same customerId for a given device index without re-reading this batch.
        # Cosmos DB only enforces id uniqueness *within* a partition key value, so an
        # update that guesses the wrong customerId for an existing device doesn't
        # replace it — it silently creates a second, ghost document with the same id
        # under a different partition. Getting this wrong produced exactly that bug
        # during development (confirmed live: 401 device docs for 400 expected ids).
        cid = customer_id(((i - 1) % customer_count) + 1)
        os_name, device_type = fake.random_element(_SYSTEMS)
        first_seen = anchor - timedelta(days=fake.random_int(30, 730))
        last_seen = anchor - timedelta(hours=fake.random_int(0, 240))
        trusted = fake.random_int(1, 100) > 15
        doc: dict[str, Any] = {
            "id": device_id(i),
            "deviceId": device_id(i),
            "customerId": cid,
            "firstSeen": _iso(first_seen),
            "lastSeen": _iso(last_seen),
            "trusted": trusted,
            "deviceFingerprint": f"SYNTH-FP-{fake.hexify('^^^^^^^^^^^^^^^^', upper=True)}",
            "operatingSystem": os_name,
            "appVersion": f"{fake.random_int(4, 9)}.{fake.random_int(0, 12)}.{fake.random_int(0, 30)}",
            "riskSignals": [] if trusted else fake.random_elements(_RISK_SIGNALS, length=fake.random_int(1, 3), unique=True),
            "synthetic": True,
        }
        # Deliberate, bounded schema variation: ~1 in 6 devices have no travel history.
        if i % 6 != 0:
            home = fake.random_element(_CITIES)
            doc["geoHistory"] = [
                {"country": home[0], "state": home[1], "city": home[2], "timestamp": _iso(last_seen - timedelta(days=7))},
                {"country": home[0], "state": home[1], "city": home[2], "timestamp": _iso(last_seen)},
            ]
        devices.append(doc)
    return devices


def _generate_sessions(fake: Faker, devices: list[dict[str, Any]], *, session_count: int, anchor: datetime) -> list[dict[str, Any]]:
    sessions: list[dict[str, Any]] = []
    for i in range(1, session_count + 1):
        dev = fake.random_element(devices)
        os_name, device_type = next(s for s in _SYSTEMS if s[0] == dev["operatingSystem"])
        started = anchor - timedelta(minutes=fake.random_int(1, 60 * 24 * 30))
        duration_min = fake.random_int(1, 45)
        unusual_geo = fake.random_int(1, 100) <= 4
        geo = fake.random_element(_UNUSUAL_CITIES) if unusual_geo else fake.random_element(_CITIES)
        failed_attempts = fake.random_int(0, 3) if fake.random_int(1, 100) <= 15 else 0
        activities = [
            {"activityType": fake.random_element(_ACTIVITY_TYPES), "timestamp": _iso(started + timedelta(minutes=offset * fake.random_int(1, 5)))}
            for offset in range(fake.random_int(1, 5))
        ]
        risk = min(100, 5 + failed_attempts * 20 + (30 if unusual_geo else 0) + (20 if not dev["trusted"] else 0))
        session: dict[str, Any] = {
            "id": f"SESSION-{i:09d}",
            "sessionId": f"SESSION-{i:09d}",
            "customerId": dev["customerId"],
            "device": {
                "deviceId": dev["deviceId"],
                "deviceType": device_type,
                "operatingSystem": dev["operatingSystem"],
                "appVersion": dev["appVersion"],
            },
            "loginTimestamp": _iso(started),
            "logoutTimestamp": _iso(started + timedelta(minutes=duration_min)),
            "ipAddress": _fictional_ip(fake),
            "geo": {"country": geo[0], "state": geo[1], "city": geo[2]},
            "authentication": {
                "method": fake.random_element(_AUTH_METHODS),
                "mfaUsed": fake.random_int(1, 100) > 20,
                "failedAttempts": failed_attempts,
            },
            "activities": activities,
            "sessionRiskScore": risk,
            "synthetic": True,
        }
        sessions.append(session)
    return sessions


def _generate_alerts(fake: Faker, *, customer_count: int, alert_count: int, anchor: datetime) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for i in range(1, alert_count + 1):
        created = anchor - timedelta(hours=fake.random_int(1, 24 * 60))
        status = fake.random_element(("open", "open", "investigating", "resolved", "resolved", "dismissed"))
        alert: dict[str, Any] = {
            "id": f"ALERT-{i:08d}",
            "alertId": f"ALERT-{i:08d}",
            # Deterministic for the same reason as _generate_devices' cid above.
            "customerId": customer_id(((i - 1) % customer_count) + 1),
            "transactionId": transaction_id(fake.random_int(1, 999_999_999)),
            "createdTimestamp": _iso(created),
            "alertType": fake.random_element(_ALERT_TYPES),
            "severity": fake.random_element(_SEVERITIES),
            "status": status,
            "signals": fake.random_elements(_ALERT_SIGNALS, length=fake.random_int(1, 3), unique=True),
            "investigatorNotes": (
                [] if status == "open" else fake.random_elements(_NOTE_TEMPLATES, length=fake.random_int(1, 2), unique=True)
            ),
            "synthetic": True,
        }
        # Deliberate schema variation: resolution only exists once an alert is closed.
        if status in ("resolved", "dismissed"):
            outcome = "confirmedFraud" if (status == "resolved" and fake.random_int(1, 100) <= 60) else "falsePositive"
            alert["resolution"] = {
                "outcome": outcome,
                "resolvedTimestamp": _iso(created + timedelta(hours=fake.random_int(1, 72))),
            }
        alerts.append(alert)
    return alerts


def generate_initial(
    *,
    seed: int = DEFAULT_SEED,
    as_of: str = DEFAULT_AS_OF,
    customer_count: int = DEFAULT_CUSTOMERS,
    device_count: int = DEFAULT_DEVICES,
    session_count: int = DEFAULT_SESSIONS,
    alert_count: int = DEFAULT_ALERTS,
) -> dict[str, list[dict[str, Any]]]:
    """Return a repeatable initial snapshot for all three containers."""
    if min(customer_count, device_count, session_count) < 1 or alert_count < 0:
        raise ValueError("customer, device, and session counts must be positive; alerts cannot be negative")
    fake = Faker()
    Faker.seed(seed)
    anchor = _dt(as_of)
    devices = _generate_devices(fake, customer_count=customer_count, device_count=device_count, anchor=anchor)
    sessions = _generate_sessions(fake, devices, session_count=session_count, anchor=anchor)
    alerts = _generate_alerts(fake, customer_count=customer_count, alert_count=alert_count, anchor=anchor)
    return {"digitalSessions": sessions, "devices": devices, "fraudAlerts": alerts}


def generate_incremental(*, as_of: str = DEFAULT_AS_OF) -> dict[str, list[dict[str, Any]]]:
    """Return deterministic upserts proving change propagation:
    one new session, one device flipping trusted True->False, one new fraud
    alert, and one existing alert's status changing (open -> investigating).
    """
    anchor = _dt(as_of) + timedelta(days=1)
    new_session = {
        "id": "SESSION-INC-000001",
        "sessionId": "SESSION-INC-000001",
        "customerId": customer_id(1),
        "device": {"deviceId": device_id(1), "deviceType": "Mobile", "operatingSystem": "iOS 18.3", "appVersion": "9.1.1"},
        "loginTimestamp": _iso(anchor),
        "logoutTimestamp": _iso(anchor + timedelta(minutes=9)),
        "ipAddress": "203.0.113.77",
        "geo": {"country": "GB", "state": "ENG", "city": "London"},
        "authentication": {"method": "password+mfa", "mfaUsed": True, "failedAttempts": 2},
        "activities": [{"activityType": "login", "timestamp": _iso(anchor)}],
        "sessionRiskScore": 88,
        "changeType": "insert",
        "synthetic": True,
    }
    device_now_untrusted = {
        "id": device_id(1),
        "deviceId": device_id(1),
        "customerId": customer_id(1),
        "firstSeen": _iso(anchor - timedelta(days=400)),
        "lastSeen": _iso(anchor),
        "trusted": False,
        "deviceFingerprint": "SYNTH-FP-INCREMENTAL0",
        "operatingSystem": "iOS 18.3",
        "appVersion": "9.1.1",
        "riskSignals": ["impossible_travel"],
        "geoHistory": [{"country": "GB", "state": "ENG", "city": "London", "timestamp": _iso(anchor)}],
        "changeType": "update",
        "synthetic": True,
    }
    new_alert = {
        "id": "ALERT-INC-000001",
        "alertId": "ALERT-INC-000001",
        "customerId": customer_id(1),
        "transactionId": transaction_id(1),
        "createdTimestamp": _iso(anchor),
        "alertType": "accountTakeover",
        "severity": "critical",
        "status": "open",
        "signals": ["geoImpossibleTravel", "deviceMismatch"],
        "investigatorNotes": [],
        "changeType": "insert",
        "synthetic": True,
    }
    status_changed_alert = {
        "id": "ALERT-00000001",
        "alertId": "ALERT-00000001",
        "customerId": customer_id(1),
        "transactionId": transaction_id(2),
        "createdTimestamp": _iso(anchor - timedelta(hours=6)),
        "alertType": "transactionAnomaly",
        "severity": "high",
        "status": "investigating",
        "signals": ["velocityAnomaly"],
        "investigatorNotes": ["Escalated after the new device-mismatch alert on this customer."],
        "changeType": "update",
        "synthetic": True,
    }
    return {
        "digitalSessions": [new_session],
        "devices": [device_now_untrusted],
        "fraudAlerts": [new_alert, status_changed_alert],
    }


def _write_jsonl(path: Path, documents: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    docs = list(documents)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for document in docs:
            handle.write(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n")
    return len(docs)


def write_batch(root: Path, batch: dict[str, list[dict[str, Any]]], name: str) -> dict[str, Any]:
    target = root / name
    counts = {container: _write_jsonl(target / f"{container}.jsonl", batch[container]) for container in CONTAINERS}
    manifest = {"batch": name, "partitionKey": "/customerId", "counts": counts, "synthetic": True}
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    logger.info("wrote batch=%s counts=%s -> %s", name, counts, target)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=Path("data/cosmos"))
    parser.add_argument("--batch", choices=("initial", "incremental", "all"), default="all")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--as-of", default=DEFAULT_AS_OF)
    parser.add_argument("--customers", type=int, default=DEFAULT_CUSTOMERS, help="Size of the customerId population (1..N). Must align with the Databricks generator's CUSTOMER_COUNT to guarantee overlap.")
    parser.add_argument("--devices", type=int, default=DEFAULT_DEVICES)
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--alerts", type=int, default=DEFAULT_ALERTS)
    args = parser.parse_args()

    manifests = []
    if args.batch in ("initial", "all"):
        manifests.append(
            write_batch(
                args.out_dir,
                generate_initial(
                    seed=args.seed,
                    as_of=args.as_of,
                    customer_count=args.customers,
                    device_count=args.devices,
                    session_count=args.sessions,
                    alert_count=args.alerts,
                ),
                "initial",
            )
        )
    if args.batch in ("incremental", "all"):
        manifests.append(write_batch(args.out_dir, generate_incremental(as_of=args.as_of), "incremental"))
    print(json.dumps(manifests, indent=2))


if __name__ == "__main__":
    main()
