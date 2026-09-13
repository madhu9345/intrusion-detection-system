from collections import defaultdict
from statistics import mean, pstdev


class Flow:

    def __init__(self, src_ip, src_port, dst_ip, dst_port, protocol):

        self.src_ip = src_ip
        self.src_port = src_port

        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.protocol = protocol

        self.start_time = None
        self.last_time = None

        # Forward direction
        self.fwd_lengths = []
        self.fwd_times = []
        self.fwd_headers = []
        self.fwd_windows = []

        # Backward direction
        self.bwd_lengths = []
        self.bwd_times = []
        self.bwd_headers = []
        self.bwd_windows = []

        # TCP
        self.rst_count = 0

    def add_packet(
        self,
        timestamp,
        length,
        direction,
        header_length=0,
        window=0,
        rst=False
    ):

        if self.start_time is None:
            self.start_time = timestamp

        self.last_time = timestamp

        if direction == "forward":

            self.fwd_lengths.append(length)
            self.fwd_times.append(timestamp)
            self.fwd_headers.append(header_length)
            self.fwd_windows.append(window)

        else:

            self.bwd_lengths.append(length)
            self.bwd_times.append(timestamp)
            self.bwd_headers.append(header_length)
            self.bwd_windows.append(window)

        if rst:
            self.rst_count += 1

    @staticmethod
    def calculate_iat(timestamps):

        if len(timestamps) < 2:
            return []

        return [
            timestamps[i] - timestamps[i - 1]
            for i in range(1, len(timestamps))
        ]

    def calculate_features(self):

        # -----------------------------
        # Duration
        # -----------------------------

        if self.start_time is not None:

            duration = self.last_time - self.start_time

        else:

            duration = 0

        # -----------------------------
        # IAT
        # -----------------------------

        fwd_iat = self.calculate_iat(self.fwd_times)

        bwd_iat = self.calculate_iat(self.bwd_times)

        # -----------------------------
        # 1. Bwd Pkt Len Max
        # -----------------------------

        bwd_pkt_len_max = (
            max(self.bwd_lengths)
            if self.bwd_lengths
            else 0
        )

        # -----------------------------
        # 2. Bwd Header Len
        # -----------------------------

        bwd_header_len = sum(self.bwd_headers)

        # -----------------------------
        # 3. Active Min
        # -----------------------------

        active_min = 0

        # -----------------------------
        # 4. Initial Bwd Window
        # -----------------------------

        init_bwd_win = (
            self.bwd_windows[0]
            if self.bwd_windows
            else 0
        )

        # -----------------------------
        # 5. Initial Fwd Window
        # -----------------------------

        init_fwd_win = (
            self.fwd_windows[0]
            if self.fwd_windows
            else 0
        )

        # -----------------------------
        # 6. Bwd IAT Min
        # -----------------------------

        bwd_iat_min = (
            min(bwd_iat)
            if bwd_iat
            else 0
        )

        # -----------------------------
        # 7. Fwd Pkt Len Max
        # -----------------------------

        fwd_pkt_len_max = (
            max(self.fwd_lengths)
            if self.fwd_lengths
            else 0
        )

        # -----------------------------
        # 8. Idle Mean
        # -----------------------------

        idle_mean = 0

        # -----------------------------
        # 9. RST Flag Count
        # -----------------------------

        rst_flag_cnt = self.rst_count

        # -----------------------------
        # 10. Fwd IAT Std
        # -----------------------------

        fwd_iat_std = (
            pstdev(fwd_iat)
            if len(fwd_iat) > 1
            else 0
        )

        # -----------------------------
        # 11. Flow Duration
        # -----------------------------

        flow_duration = duration

        # -----------------------------
        # 12. Total Forward Packets
        # -----------------------------

        total_forward_packets = len(self.fwd_lengths)

        # -----------------------------
        # 13. Packet Size Average
        # -----------------------------

        all_lengths = (
            self.fwd_lengths +
            self.bwd_lengths
        )

        pkt_size_avg = (
            mean(all_lengths)
            if all_lengths
            else 0
        )

        # -----------------------------
        # 14. Down / Up Ratio
        # -----------------------------

        if total_forward_packets > 0:

            down_up_ratio = (
                len(self.bwd_lengths) /
                total_forward_packets
            )

        else:

            down_up_ratio = 0

        # -----------------------------
        # 15. Fwd IAT Min
        # -----------------------------

        fwd_iat_min = (
            min(fwd_iat)
            if fwd_iat
            else 0
        )

        # -----------------------------
        # 16. Packets Per Second
        # -----------------------------

        total_packets = len(all_lengths)

        if duration > 0:

            flow_packets_per_sec = (
                total_packets / duration
            )

        else:

            flow_packets_per_sec = 0

        # -----------------------------
        # Return exactly 16 features
        # -----------------------------

        return {

            "Bwd Pkt Len Max":
                bwd_pkt_len_max,

            "Bwd Header Len":
                bwd_header_len,

            "Active Min":
                active_min,

            "Init Bwd Win Byts":
                init_bwd_win,

            "Init Fwd Win Byts":
                init_fwd_win,

            "Bwd IAT Min":
                bwd_iat_min,

            "Fwd Pkt Len Max":
                fwd_pkt_len_max,

            "Idle Mean":
                idle_mean,

            "RST Flag Cnt":
                rst_flag_cnt,

            "Fwd IAT Std":
                fwd_iat_std,

            "flow_duration":
                flow_duration,

            "total_forward_packets":
                total_forward_packets,

            "Pkt Size Avg":
                pkt_size_avg,

            "Down/Up Ratio":
                down_up_ratio,

            "Fwd IAT Min":
                fwd_iat_min,

            "flow_packets_per_sec":
                flow_packets_per_sec
        }