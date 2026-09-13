from scapy.all import sniff, IP, TCP, UDP

from flow_tracker import Flow


INTERFACE = "eth0"

flows = {}


def get_packet_info(packet):

    if not packet.haslayer(IP):
        return None

    ip = packet[IP]

    protocol = ip.proto

    if packet.haslayer(TCP):

        tcp = packet[TCP]

        src_port = tcp.sport
        dst_port = tcp.dport

        header_length = tcp.dataofs * 4

        window = tcp.window

        rst = bool(tcp.flags & 0x04)

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

    # -----------------------------
    # Bidirectional flow key
    # -----------------------------

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

    # -----------------------------
    # Find existing flow
    # -----------------------------

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

    # -----------------------------
    # Add packet
    # -----------------------------

    flow.add_packet(
        timestamp=timestamp,
        length=length,
        direction=direction,
        header_length=header_length,
        window=window,
        rst=rst
    )

    # -----------------------------
    # Calculate current features
    # -----------------------------

    features = flow.calculate_features()

    # -----------------------------
    # Display
    # -----------------------------

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


print("======================================")
print(" REAL-TIME FLOW FEATURE EXTRACTION")
print("======================================")

print("Interface:", INTERFACE)
print("Press CTRL+C to stop")
print()


sniff(
    iface=INTERFACE,
    prn=process_packet,
    store=False
)