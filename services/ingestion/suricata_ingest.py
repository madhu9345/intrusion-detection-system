"""
suricata_ingest.py
------------------
Real-time Suricata EVE JSON Ingestion Service for IR-IDS.

Features:
✓ Actively tails Suricata's eve.json in real time (streaming tail -f)
✓ Parses Suricata 'flow' and 'alert' event records
✓ Maps Suricata metrics into standard IR-IDS network flow schema
✓ Ingests live network traffic events directly into Kafka topic 'raw-traffic'
✓ Maintains active alert correlation for accurate label assignment
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SuricataIngest")

# ============================================================
# CONFIGURATION
# ============================================================
SERVICE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) > 2 else SERVICE_ROOT

# Default Suricata log location: datasets/suricata/eve.json
# Can be overridden via SURICATA_EVE_PATH environment variable
DEFAULT_EVE_PATH = PROJECT_ROOT / "datasets" / "suricata" / "eve.json"
EVE_FILE_PATH = Path(os.getenv("SURICATA_EVE_PATH", str(DEFAULT_EVE_PATH)))

BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
TOPIC_NAME = os.getenv("RAW_TRAFFIC_TOPIC", "raw-traffic")


class SuricataIngestionService:

    def __init__(self, eve_path=EVE_FILE_PATH, bootstrap_server=BOOTSTRAP_SERVER):
        self.eve_path = Path(eve_path)
        self.bootstrap_server = bootstrap_server
        self.running = True
        self.processed_flows = 0
        self.processed_alerts = 0
        self.start_time = None
        self.last_log_time = time.time()
        self.new_flows_since_log = 0

        logger.info("=" * 70)
        logger.info("SURICATA REAL-TIME INGESTION SERVICE")
        logger.info("=" * 70)
        logger.info(f"Suricata EVE Log : {self.eve_path}")
        logger.info(f"Kafka Broker     : {self.bootstrap_server}")
        logger.info(f"Target Topic     : {TOPIC_NAME}")
        logger.info("=" * 70)

        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",
            acks=1
        )

        # Cache recent alerts by flow_id / 5-tuple for correlation
        self.recent_alerts = {}

    def parse_suricata_flow(self, event):
        """Converts a Suricata 'flow' event into the IR-IDS unified record schema."""
        flow_data = event.get("flow", {})
        tcp_data = event.get("tcp", {})

        src_ip = event.get("src_ip", "")
        src_port = event.get("src_port", 0)
        dest_ip = event.get("dest_ip", "")
        dest_port = event.get("dest_port", 0)

        proto_str = event.get("proto", "TCP").upper()
        protocol_num = 6 if proto_str == "TCP" else (17 if proto_str == "UDP" else 1)

        duration_sec = float(flow_data.get("age", 0))
        duration_us = duration_sec * 1_000_000

        fwd_pkts = int(flow_data.get("pkts_toserver", 0))
        bwd_pkts = int(flow_data.get("pkts_toclient", 0))
        total_pkts = fwd_pkts + bwd_pkts

        fwd_bytes = int(flow_data.get("bytes_toserver", 0))
        bwd_bytes = int(flow_data.get("bytes_toclient", 0))
        total_bytes = fwd_bytes + bwd_bytes

        # Flow rates
        flow_bytes_per_sec = (total_bytes / duration_sec) if duration_sec > 0 else 0.0
        flow_packets_per_sec = (total_pkts / duration_sec) if duration_sec > 0 else 0.0

        # TCP Flags
        syn_cnt = 1 if tcp_data.get("syn") else 0
        fin_cnt = 1 if tcp_data.get("fin") else 0
        rst_cnt = 1 if tcp_data.get("rst") else 0
        psh_cnt = 1 if tcp_data.get("psh") else 0
        ack_cnt = 1 if tcp_data.get("ack") else 0
        urg_cnt = 1 if tcp_data.get("urg") else 0

        # Down/Up Ratio
        down_up_ratio = (bwd_pkts / fwd_pkts) if fwd_pkts > 0 else 0.0

        # Average packet size
        pkt_size_avg = (total_bytes / total_pkts) if total_pkts > 0 else 0.0

        # Check for correlated alert
        flow_id = event.get("flow_id")
        alert_info = self.recent_alerts.pop(flow_id, None)

        if alert_info:
            label = alert_info.get("signature", "ATTACK")
        elif flow_data.get("alerted", False):
            label = "SURICATA_ALERTED"
        else:
            label = "BENIGN"

        record = {
            "Dst Port": dest_port,
            "Src Port": src_port,
            "Protocol": protocol_num,
            "Timestamp": event.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "Flow Duration": duration_us,
            "Tot Fwd Pkts": fwd_pkts,
            "Tot Bwd Pkts": bwd_pkts,
            "TotLen Fwd Pkts": fwd_bytes,
            "TotLen Bwd Pkts": bwd_bytes,
            "Flow Byts/s": flow_bytes_per_sec,
            "Flow Pkts/s": flow_packets_per_sec,
            "Flow IAT Mean": (duration_us / total_pkts) if total_pkts > 0 else 0.0,
            "Flow IAT Std": 0.0,
            "Flow IAT Max": duration_us,
            "Flow IAT Min": 0.0,
            "Fwd IAT Tot": duration_us,
            "Bwd IAT Tot": duration_us if bwd_pkts > 0 else 0.0,
            "Fwd PSH Flags": psh_cnt,
            "Bwd PSH Flags": 0,
            "Fwd URG Flags": urg_cnt,
            "Bwd URG Flags": 0,
            "Fwd Header Len": 20 if protocol_num == 6 else 8,
            "Bwd Header Len": 20 if protocol_num == 6 else 8,
            "Fwd Pkts/s": (fwd_pkts / duration_sec) if duration_sec > 0 else 0.0,
            "Bwd Pkts/s": (bwd_pkts / duration_sec) if duration_sec > 0 else 0.0,
            "Pkt Len Min": 40.0,
            "Pkt Len Max": float(max(fwd_bytes, bwd_bytes, 40)),
            "Pkt Len Mean": pkt_size_avg,
            "Pkt Len Std": 0.0,
            "Pkt Len Var": 0.0,
            "FIN Flag Cnt": fin_cnt,
            "SYN Flag Cnt": syn_cnt,
            "RST Flag Cnt": rst_cnt,
            "PSH Flag Cnt": psh_cnt,
            "ACK Flag Cnt": ack_cnt,
            "URG Flag Cnt": urg_cnt,
            "Down/Up Ratio": down_up_ratio,
            "Pkt Size Avg": pkt_size_avg,
            "Init Fwd Win Byts": 65535,
            "Init Bwd Win Byts": 65535 if bwd_pkts > 0 else 0,
            "Fwd Seg Size Min": 20,
            "Active Mean": duration_us,
            "Active Min": duration_us,
            "Idle Mean": 0.0,
            "Idle Min": 0.0,
            "Label": label,
            "source_engine": "suricata"
        }

        return record

    def tail_eve_file(self):
        """Continuously tails the eve.json file line-by-line as Suricata writes it."""
        if not self.eve_path.parent.exists():
            self.eve_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.eve_path.exists():
            logger.info(f"Waiting for Suricata log file at: {self.eve_path} ...")
            while self.running and not self.eve_path.exists():
                time.sleep(1.0)

        logger.info(f"Opened Suricata log stream: {self.eve_path}")
        with open(self.eve_path, "r", encoding="utf-8", errors="ignore") as f:
            # By default seek to start if file is small, or allow configuring via env / cli
            from_start = os.environ.get("SURICATA_FROM_START", "true").lower() in ("true", "1", "yes")
            if from_start:
                logger.info("Reading from beginning of eve.json (SURICATA_FROM_START=true)...")
                f.seek(0, os.SEEK_SET)
            else:
                logger.info("Seeking to end of eve.json to stream only new live events...")
                f.seek(0, os.SEEK_END)

            while self.running:
                line = f.readline()
                if not line:
                    now = time.time()
                    if self.new_flows_since_log > 0 and (now - self.last_log_time >= 1.0):
                        rate = self.new_flows_since_log / max(now - self.last_log_time, 0.001)
                        logger.info(
                            f"Live Stream | +{self.new_flows_since_log} flows in last sec "
                            f"(Rate: {rate:6.1f} flows/s) | "
                            f"Total Flows: {self.processed_flows:,} | "
                            f"Total Alerts: {self.processed_alerts:,}"
                        )
                        self.last_log_time = now
                        self.new_flows_since_log = 0
                    time.sleep(0.1)
                    continue

                line = line.strip()
                if not line:
                    continue

                try:
                    event = json.loads(line)
                except Exception:
                    continue

                event_type = event.get("event_type")

                # Track alert events
                if event_type == "alert":
                    self.processed_alerts += 1
                    flow_id = event.get("flow_id")
                    if flow_id:
                        self.recent_alerts[flow_id] = event.get("alert", {})
                    continue

                # Process flow events
                if event_type == "flow":
                    record = self.parse_suricata_flow(event)
                    self.producer.send(TOPIC_NAME, value=record)
                    self.processed_flows += 1
                    self.new_flows_since_log += 1

                # Periodic real-time progress logging (every 1 second or every 500 flows)
                now = time.time()
                if (now - self.last_log_time >= 1.0 and self.new_flows_since_log > 0) or (self.new_flows_since_log >= 500):
                    rate = self.new_flows_since_log / max(now - self.last_log_time, 0.001)
                    logger.info(
                        f"Live Stream | +{self.new_flows_since_log} flows in last sec "
                        f"(Rate: {rate:6.1f} flows/s) | "
                        f"Total Flows: {self.processed_flows:,} | "
                        f"Total Alerts: {self.processed_alerts:,}"
                    )
                    self.last_log_time = now
                    self.new_flows_since_log = 0

    def run(self):
        self.start_time = time.time()
        self.last_log_time = time.time()
        self.new_flows_since_log = 0
        logger.info("Listening for live Suricata flow events...")
        try:
            self.tail_eve_file()
        except KeyboardInterrupt:
            logger.info("Ctrl+C detected.")
        finally:
            self.shutdown()

    def shutdown(self):
        logger.info("Shutting down Suricata Ingestion Service...")
        try:
            self.producer.flush()
            self.producer.close()
        except Exception:
            pass

        elapsed = max(time.time() - self.start_time, 0.001) if self.start_time else 1
        logger.info("=" * 70)
        logger.info("SURICATA INGESTION FINISHED")
        logger.info(f"Total Flows Streamed : {self.processed_flows:,}")
        logger.info(f"Total Alerts Handled : {self.processed_alerts:,}")
        logger.info(f"Elapsed Time         : {elapsed:.2f} sec")
        logger.info("=" * 70)


def main():
    service = SuricataIngestionService()

    def handle_sig(sig, frame):
        service.running = False

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    service.run()


if __name__ == "__main__":
    main()
