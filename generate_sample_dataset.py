"""
generate_sample_dataset.py
--------------------------
Generates a realistic synthetic sample dataset in CSE-CIC-IDS2018 format
for testing and developing the IR-IDS pipeline locally.
"""

from pathlib import Path
import csv
import random

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "datasets" / "raw" / "CSE-CIC-IDS2018-50K"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "sample_traffic.csv"

COLUMNS = [
    "Dst Port", "Protocol", "Timestamp", "Flow Duration", "Tot Fwd Pkts",
    "Tot Bwd Pkts", "TotLen Fwd Pkts", "TotLen Bwd Pkts", "Fwd Pkt Len Max",
    "Fwd Pkt Len Min", "Fwd Pkt Len Mean", "Fwd Pkt Len Std", "Bwd Pkt Len Max",
    "Bwd Pkt Len Min", "Bwd Pkt Len Mean", "Bwd Pkt Len Std", "Flow Byts/s",
    "Flow Pkts/s", "Flow IAT Mean", "Flow IAT Std", "Flow IAT Max", "Flow IAT Min",
    "Fwd IAT Tot", "Fwd IAT Mean", "Fwd IAT Std", "Fwd IAT Max", "Fwd IAT Min",
    "Bwd IAT Tot", "Bwd IAT Mean", "Bwd IAT Std", "Bwd IAT Max", "Bwd IAT Min",
    "Fwd PSH Flags", "Bwd PSH Flags", "Fwd URG Flags", "Bwd URG Flags",
    "Fwd Header Len", "Bwd Header Len", "Fwd Pkts/s", "Bwd Pkts/s",
    "Pkt Len Min", "Pkt Len Max", "Pkt Len Mean", "Pkt Len Std", "Pkt Len Var",
    "FIN Flag Cnt", "SYN Flag Cnt", "RST Flag Cnt", "PSH Flag Cnt", "ACK Flag Cnt",
    "URG Flag Cnt", "CWE Flag Count", "ECE Flag Cnt", "Down/Up Ratio",
    "Pkt Size Avg", "Fwd Seg Size Avg", "Bwd Seg Size Avg", "Fwd Byts/b Avg",
    "Fwd Pkts/b Avg", "Fwd Blk Rate Avg", "Bwd Byts/b Avg", "Bwd Pkts/b Avg",
    "Bwd Blk Rate Avg", "Subflow Fwd Pkts", "Subflow Fwd Byts", "Subflow Bwd Pkts",
    "Subflow Bwd Byts", "Init Fwd Win Byts", "Init Bwd Win Byts", "Fwd Act Data Pkts",
    "Fwd Seg Size Min", "Active Mean", "Active Std", "Active Max", "Active Min",
    "Idle Mean", "Idle Std", "Idle Max", "Idle Min", "Label"
]

LABELS = ["Benign", "DoS attacks-Hulk", "FTP-BruteForce", "SSH-Bruteforce"]

def generate_row(label):
    is_benign = (label == "Benign")
    return [
        random.choice([80, 443, 22, 21, 8080, 53]),                      # Dst Port
        random.choice([6, 17]),                                           # Protocol (TCP/UDP)
        "20/02/2018 08:30:00",                                            # Timestamp
        random.randint(1000, 500000) if is_benign else random.randint(100, 10000), # Flow Duration
        random.randint(2, 50) if is_benign else random.randint(50, 500),  # Tot Fwd Pkts
        random.randint(2, 50) if is_benign else random.randint(1, 10),   # Tot Bwd Pkts
        random.randint(100, 5000),                                        # TotLen Fwd Pkts
        random.randint(100, 8000),                                        # TotLen Bwd Pkts
        random.randint(64, 1460),                                         # Fwd Pkt Len Max
        random.randint(0, 64),                                            # Fwd Pkt Len Min
        random.uniform(50, 800),                                          # Fwd Pkt Len Mean
        random.uniform(10, 200),                                          # Fwd Pkt Len Std
        random.randint(64, 1460),                                         # Bwd Pkt Len Max
        random.randint(0, 64),                                            # Bwd Pkt Len Min
        random.uniform(50, 800),                                          # Bwd Pkt Len Mean
        random.uniform(10, 200),                                          # Bwd Pkt Len Std
        random.uniform(100, 50000),                                       # Flow Byts/s
        random.uniform(10, 5000),                                         # Flow Pkts/s
        random.uniform(100, 10000),                                       # Flow IAT Mean
        random.uniform(10, 500),                                          # Flow IAT Std
        random.randint(1000, 20000),                                      # Flow IAT Max
        random.randint(1, 50),                                            # Flow IAT Min
        random.randint(500, 30000),                                       # Fwd IAT Tot
        random.uniform(100, 5000),                                        # Fwd IAT Mean
        random.uniform(10, 300),                                          # Fwd IAT Std
        random.randint(500, 10000),                                       # Fwd IAT Max
        random.randint(1, 50),                                            # Fwd IAT Min
        random.randint(500, 30000),                                       # Bwd IAT Tot
        random.uniform(100, 5000),                                        # Bwd IAT Mean
        random.uniform(10, 300),                                          # Bwd IAT Std
        random.randint(500, 10000),                                       # Bwd IAT Max
        random.randint(1, 50),                                            # Bwd IAT Min
        0, 0, 0, 0,                                                       # PSH/URG flags
        random.randint(20, 40),                                           # Fwd Header Len
        random.randint(20, 40),                                           # Bwd Header Len
        random.uniform(5, 500),                                           # Fwd Pkts/s
        random.uniform(5, 500),                                           # Bwd Pkts/s
        random.randint(0, 64),                                            # Pkt Len Min
        random.randint(64, 1460),                                         # Pkt Len Max
        random.uniform(50, 1000),                                         # Pkt Len Mean
        random.uniform(10, 300),                                          # Pkt Len Std
        random.uniform(100, 90000),                                       # Pkt Len Var
        0,                                                                # FIN Flag
        1 if not is_benign else 0,                                        # SYN Flag
        random.choice([0, 1, 2]),                                         # RST Flag
        random.choice([0, 1]),                                            # PSH Flag
        1,                                                                # ACK Flag
        0, 0, 0,                                                          # URG, CWE, ECE
        random.uniform(0.5, 2.0),                                         # Down/Up Ratio
        random.uniform(50, 1000),                                         # Pkt Size Avg
        random.uniform(50, 500),                                          # Fwd Seg Size Avg
        random.uniform(50, 500),                                          # Bwd Seg Size Avg
        0, 0, 0, 0, 0, 0,                                                 # Rate Avgs
        random.randint(2, 50),                                            # Subflow Fwd Pkts
        random.randint(100, 5000),                                        # Subflow Fwd Byts
        random.randint(2, 50),                                            # Subflow Bwd Pkts
        random.randint(100, 8000),                                        # Subflow Bwd Byts
        random.randint(1024, 65535),                                      # Init Fwd Win Byts
        random.randint(1024, 65535),                                      # Init Bwd Win Byts
        random.randint(1, 30),                                            # Fwd Act Data Pkts
        20,                                                               # Fwd Seg Size Min
        random.uniform(0, 1000),                                          # Active Mean
        random.uniform(0, 200),                                           # Active Std
        random.uniform(0, 2000),                                          # Active Max
        random.uniform(0, 500),                                           # Active Min
        random.uniform(0, 10000),                                         # Idle Mean
        random.uniform(0, 500),                                           # Idle Std
        random.uniform(0, 20000),                                         # Idle Max
        random.uniform(0, 500),                                           # Idle Min
        label                                                             # Label
    ]

def main(num_rows=2000):
    print(f"Generating {num_rows} synthetic network traffic rows...")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        for i in range(num_rows):
            # 70% Benign, 30% Attacks
            if random.random() < 0.7:
                label = "Benign"
            else:
                label = random.choice(LABELS[1:])
            writer.writerow(generate_row(label))
    print(f"Dataset successfully created at: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
