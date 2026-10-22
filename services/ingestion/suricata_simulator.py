"""
suricata_simulator.py
---------------------
Simulates live Suricata IDS network sensor logging to eve.json in real time.
Useful for end-to-end testing of the Suricata streaming ingestion pipeline.
"""

import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import os

SERVICE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) > 2 else SERVICE_ROOT
DEFAULT_EVE_PATH = PROJECT_ROOT / "datasets" / "suricata" / "eve.json"
EVE_FILE = Path(os.getenv("SURICATA_EVE_PATH", str(DEFAULT_EVE_PATH)))
LOG_DIR = EVE_FILE.parent
LOG_DIR.mkdir(parents=True, exist_ok=True)

ALERT_SIGNATURES = [
    {"signature": "ET SCAN Potential SSH BruteForce", "category": "Attempted Administrator Access", "severity": 1},
    {"signature": "ET DOS Possible Slowloris Attack", "category": "Denial of Service", "severity": 1},
    {"signature": "ET EXPLOIT Apache Struts RCE", "category": "Web Application Attack", "severity": 1},
    {"signature": "ET SCAN Nmap Scripting Engine Detected", "category": "Network Scan", "severity": 2},
    {"signature": "ET NOVEL Zero-Day Polymorphic C2 Beacon", "category": "Zero-Day Exploit", "severity": 1},
    {"signature": "ET ANOMALY Out-of-Profile Tunneling", "category": "Novel Anomaly", "severity": 1}
]


def generate_flow_event(is_attack=False):
    now = datetime.now(timezone.utc).isoformat()
    flow_id = random.randint(100000000, 999999999)
    src_ip = f"192.168.1.{random.randint(10, 250)}"
    dest_ip = f"10.0.0.{random.randint(1, 50)}"
    dest_port = random.choice([80, 443, 22, 21, 8080, 53, 3306])
    proto = random.choice(["TCP", "UDP"])

    duration = random.uniform(0.01, 5.0) if not is_attack else random.uniform(0.001, 0.5)
    fwd_pkts = random.randint(2, 40) if not is_attack else random.randint(50, 500)
    bwd_pkts = random.randint(1, 30) if not is_attack else random.randint(1, 5)

    bytes_fwd = fwd_pkts * random.randint(60, 1460)
    bytes_bwd = bwd_pkts * random.randint(60, 1460)

    event = {
        "timestamp": now,
        "flow_id": flow_id,
        "in_iface": "eth0",
        "event_type": "flow",
        "src_ip": src_ip,
        "src_port": random.randint(1024, 65535),
        "dest_ip": dest_ip,
        "dest_port": dest_port,
        "proto": proto,
        "flow": {
            "pkts_toserver": fwd_pkts,
            "pkts_toclient": bwd_pkts,
            "bytes_toserver": bytes_fwd,
            "bytes_toclient": bytes_bwd,
            "start": now,
            "end": now,
            "age": round(duration, 3),
            "state": "closed",
            "reason": "timeout",
            "alerted": is_attack
        },
        "tcp": {
            "syn": True,
            "fin": True if not is_attack else False,
            "ack": True,
            "rst": True if is_attack else False,
            "psh": random.choice([True, False]),
            "urg": False
        }
    }
    return flow_id, event


def generate_alert_event(flow_id, src_ip, dest_ip, dest_port):
    now = datetime.now(timezone.utc).isoformat()
    alert_info = random.choice(ALERT_SIGNATURES)
    return {
        "timestamp": now,
        "flow_id": flow_id,
        "in_iface": "eth0",
        "event_type": "alert",
        "src_ip": src_ip,
        "src_port": random.randint(1024, 65535),
        "dest_ip": dest_ip,
        "dest_port": dest_port,
        "proto": "TCP",
        "alert": alert_info
    }


def main(events_count=None, flows_per_second=50):
    print("=" * 70)
    print("SURICATA LIVE EVENT STREAM SIMULATOR")
    print("=" * 70)
    print(f"Target EVE File : {EVE_FILE}")
    print(f"Emitting ~{flows_per_second} flows/sec in real time (Press Ctrl+C to stop)...")
    print("=" * 70)

    total_emitted = 0
    start_time = time.time()

    try:
        with open(EVE_FILE, "a", encoding="utf-8") as f:
            while True:
                # Generate 1-second burst of flows
                burst_size = flows_per_second
                for _ in range(burst_size):
                    is_attack = random.random() < 0.20
                    flow_id, flow_event = generate_flow_event(is_attack)

                    if is_attack:
                        alert_event = generate_alert_event(
                            flow_id, flow_event["src_ip"], flow_event["dest_ip"], flow_event["dest_port"]
                        )
                        f.write(json.dumps(alert_event) + "\n")

                    f.write(json.dumps(flow_event) + "\n")

                f.flush()
                total_emitted += burst_size
                elapsed = time.time() - start_time
                print(f"[{elapsed:6.1f}s] Streamed +{burst_size} flows (Total: {total_emitted:,})")

                if events_count and total_emitted >= events_count:
                    break

                time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nSimulator stopped.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Suricata Live Stream Simulator")
    parser.add_argument("--rate", type=int, default=500, help="Flows per second to emit (default: 500)")
    parser.add_argument("--count", type=int, default=None, help="Total flows to emit before stopping (default: infinite)")
    args = parser.parse_args()
    main(events_count=args.count, flows_per_second=args.rate)

