import json
import time

from scapy.all import sniff, IP, TCP, UDP
from kafka import KafkaProducer

from flow_tracker import Flow


# ============================================================
# CONFIGURATION
# ============================================================

INTERFACE = r"\Device\NPF_{796F9A93-0B01-42BB-85EF-C8AFC8520425}"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "selected-features"


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),

    # Stable configuration for local Kafka testing
    acks=1,
    retries=3,
    max_in_flight_requests_per_connection=1,
    batch_size=0,
    linger_ms=0
)


# ============================================================
# FLOW STORAGE
# ============================================================

flows = {}


# ============================================================
# GET PACKET INFORMATION
# ============================================================

def get_packet_info(packet):

    if not packet.haslayer(IP):
        return None

    ip = packet[IP]

    protocol = ip.proto

    # --------------------------------------------------------
    # TCP
    # --------------------------------------------------------

    if packet.haslayer(TCP):

        tcp = packet[TCP]

        src_port = tcp.sport
        dst_port = tcp.dport

        header_length = tcp.dataofs * 4

        window = tcp.window

        rst = bool(tcp.flags & 0x04)

    # --------------------------------------------------------
    # UDP
    # --------------------------------------------------------

    elif packet.haslayer(UDP):

        udp = packet[UDP]

        src_port = udp.sport
        dst_port = udp.dport

        header_length = 8

        window = 0

        rst = False

    else:

        return None

    return (
        ip.src,
        src_port,
        ip.dst,
        dst_port,
        protocol,
        len(packet),
        header_length,
        window,
        rst,
        float(packet.time)
    )


# ============================================================
# PROCESS PACKET
# ============================================================

def process_packet(packet):

    info = get_packet_info(packet)

    if info is None:
        return

    (
        src_ip,
        src_port,
        dst_ip,
        dst_port,
        protocol,
        length,
        header_length,
        window,
        rst,
        timestamp
    ) = info


    # ========================================================
    # BIDIRECTIONAL FLOW KEY
    # ========================================================

    forward_key = (
        src_ip,
        src_port,
        dst_ip,
        dst_port,
        protocol
    )

    backward_key = (
        dst_ip,
        dst_port,
        src_ip,
        src_port,
        protocol
    )


    # ========================================================
    # FIND EXISTING FLOW
    # ========================================================

    if forward_key in flows:

        flow = flows[forward_key]

        direction = "forward"

    elif backward_key in flows:

        flow = flows[backward_key]

        direction = "backward"

    else:

        flow = Flow(
            src_ip,
            src_port,
            dst_ip,
            dst_port,
            protocol
        )

        flows[forward_key] = flow

        direction = "forward"


    # ========================================================
    # ADD PACKET TO FLOW
    # ========================================================

    flow.add_packet(
        timestamp=timestamp,
        length=length,
        direction=direction,
        header_length=header_length,
        window=window,
        rst=rst
    )


    # ========================================================
    # CALCULATE 16 SELECTED FEATURES
    # ========================================================

    features = flow.calculate_features()


    # ========================================================
    # DISPLAY LIVE FLOW
    # ========================================================

    print("\n======================================")
    print("LIVE FLOW")
    print("======================================")

    print(
        f"{src_ip}:{src_port}"
        f" -> "
        f"{dst_ip}:{dst_port}"
    )

    print("--------------------------------------")

    for name, value in features.items():

        print(
            f"{name:25s}: {value}"
        )


    # ========================================================
    # SEND 16 FEATURES TO KAFKA
    # ========================================================

    try:

        future = producer.send(
            KAFKA_TOPIC,
            value=features
        )

        # Wait for Kafka acknowledgement
        future.get(timeout=10)

        print("--------------------------------------")
        print("KAFKA STATUS : SENT")
        print("TOPIC        :", KAFKA_TOPIC)

    except Exception as e:

        print("--------------------------------------")
        print("KAFKA ERROR  :", e)


# ============================================================
# START
# ============================================================

print("======================================")
print(" REAL-TIME FLOW FEATURE EXTRACTION")
print("======================================")

print("Interface:", INTERFACE)
print("Kafka:", KAFKA_BOOTSTRAP_SERVERS)
print("Topic:", KAFKA_TOPIC)
print("Press CTRL+C to stop")
print()


# ============================================================
# START PACKET CAPTURE
# ============================================================

try:

    sniff(
        iface=INTERFACE,
        prn=process_packet,
        store=False
    )

except KeyboardInterrupt:

    print("\nStopping packet capture...")

finally:

    try:
        producer.flush()
        producer.close()
    except Exception:
        pass

    print("Kafka producer closed.")