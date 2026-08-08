IR-IDS Feature Selection Service
Overview
The Feature Selection Service is the third microservice in the IR-IDS pipeline.
It consumes cleaned network traffic records from Kafka, preprocesses candidate features using K-means, performs Double Markov Blanket (DMB) based feature selection using propensity scores and causal effects, and publishes selected features to Kafka for the Classifier Service.
Pipeline
```text
CSV Ingestion Service
        |
        v
   raw-traffic
      Kafka
        |
        v
Preprocessing Service
        |
        v
  clean-traffic
      Kafka
        |
        v
Feature Selection Service
        |
        +-- K-means Preprocessing
        +-- Propensity Score
        +-- Causal Effect
        +-- DMB Growth
        +-- DMB Shrink
        +-- Cumulative DMB
        |
        v
 selected-features
      Kafka
        |
        v
 Classifier Service
```
Folder Structure
```text
feature_selection/

├── config.py
├── logger.py
├── consumer.py
├── producer.py
├── preprocessor.py
├── propensity_score.py
├── causal_effect.py
├── dmb.py
├── feature_ranker.py
├── pipeline.py
├── requirements.txt
├── README.md
├── logs/
├── models/
└── tests/
```
Responsibilities
Consume cleaned network traffic records from Kafka
Process records in batches
Separate candidate features from the target variable
Apply K-means preprocessing
Calculate conditional target distributions
Calculate causal effects using KL divergence
Perform DMB Growth phase
Perform DMB Shrink phase
Generate the Markov Blanket for each batch
Maintain cumulative DMB results across batches
Rank selected features
Maintain minimum and maximum feature limits
Publish selected feature records to Kafka
Save the selected feature list
Generate execution and feature-selection logs
Input
Kafka Topic
```text
clean-traffic
```
The service consumes cleaned network traffic records produced by the Preprocessing Service.
Target Column
```text
label
```
The `label` column is the target variable and is never treated as a candidate feature.
Conceptually:
```text
X = Candidate Features
Y = label
```
Feature Selection Workflow
```text
clean-traffic
      |
      v
Batch Processing
      |
      v
Candidate Features + Target
      |
      v
K-means Preprocessing
      |
      v
Propensity Score
      |
      v
Causal Effect
      |
      v
DMB Growth Phase
      |
      v
DMB Shrink Phase
      |
      v
Batch DMB Result
      |
      v
Cumulative DMB
      |
      v
Selected Features
      |
      v
selected-features
```
K-means Preprocessing
The original feature values are transformed using K-means clustering before the DMB feature-selection process.
The target column `label` is not processed as a candidate feature.
Propensity Score
The Propensity Score component calculates conditional target distributions based on:
```text
P(Y | X, C)
```
where:
```text
X = Candidate Feature
Y = Target Label
C = Conditioning Set
```
The conditioning set is restricted using `MAX_CMB_CONDITIONING_COLUMNS` to avoid extremely sparse high-dimensional groupings.
Causal Effect
The Causal Effect component compares conditional target distributions using KL divergence.
Conceptually:
```text
P(Y | X=x1, C)
        |
        | KL Divergence
        v
P(Y | X=x2, C)
        |
        v
Causal Effect
```
DMB Growth Phase
The Growth phase identifies features that provide useful information about the target.
```text
Candidate Features
        |
        v
Causal Effect Test
        |
        +---- Independent ----> Drop
        |
        +---- Dependent -------> Keep
```
DMB Shrink Phase
The Shrink phase removes redundant features from the Growth result.
```text
Growth Markov Blanket
        |
        v
Conditional Independence Tests
        |
        v
Remove Redundant Features
        |
        v
Final Batch DMB
```
Cumulative DMB
DMB results are accumulated across multiple batches.
Duplicate features are not added again.
```text
Batch 1 -> Feature A, Feature B
Batch 2 -> Feature B, Feature C
Batch 3 -> Feature D

Cumulative DMB -> Feature A, Feature B, Feature C, Feature D
```
Batch Processing
The service processes records in batches.
Current configuration:
```text
Batch Size : 5000 records
```
The final batch can contain fewer than 5000 records and is also processed.
Example:
```text
495,000 records
    +
671 records
    =
495,671 total records
```
Feature Limits
Current configuration:
```text
Minimum Features      : 15
Maximum Features      : 25
Batch Size            : 5000
```
The DMB result remains the primary feature-selection mechanism.
Configuration
Important configuration parameters:
```text
BOOTSTRAP_SERVER = localhost:9092
INPUT_TOPIC = clean-traffic
OUTPUT_TOPIC = selected-features
BATCH_RECORDS = 5000
TARGET_COLUMN = label
MIN_FEATURES = 15
MAX_SELECTED_FEATURES = 25
NUM_CLASSES = 15
ALPHA_PERCENTILE = 75
MIN_GROUP_SIZE = 5
MAX_CMB_CONDITIONING_COLUMNS = 6
RANDOM_STATE = 42
```
Kafka Input
The Feature Selection Service consumes from:
```text
clean-traffic
```
Kafka configuration:
```text
Bootstrap Server : localhost:9092
Input Topic      : clean-traffic
```
Kafka Output
The Feature Selection Service publishes selected-feature records to:
```text
selected-features
```
The next service consumes this topic:
```text
selected-features
        |
        v
Classifier Service
```
The Classifier Service communicates with Feature Selection through Kafka and does not directly access the Feature Selection Service code or internal data structures.
Run
Install Dependencies
```bash
pip install -r requirements.txt
```
Start Kafka
```bash
docker compose up -d
```
Run Feature Selection Service
```bash
cd services/feature_selection
python pipeline.py
```
Verify Kafka Output
To view records from the `selected-features` topic:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic selected-features --from-beginning --max-messages 10
```
To check Kafka topic offsets:
```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic selected-features
```
Logs
Logs are stored in:
```text
logs/feature_selection.log
```
The service records:
```text
Received Records
Received Batch
Target Classes
Preprocessing
DMB Growth
DMB Shrink
Batch DMB Result
Cumulative DMB
Selected Features
Execution Time
Producer Statistics
```
Processing Result
The Feature Selection Service was successfully tested with:
```text
Total Input Records : 495,671
Batch Size          : 5,000
Total Batches       : 100
Cumulative DMB      : 16
Minimum Features    : 15
```
The cumulative DMB result reached the configured minimum feature requirement.
Example Selected Features
The selected feature list can contain features such as:
```text
Bwd Header Len
Init Fwd Win Byts
flow_duration
flow_iat_max
forward_iat_total
Fwd IAT Max
backward_iat_total
Idle Mean
Idle Max
Pkt Size Avg
Pkt Len Max
```
The exact selected feature set depends on the DMB results obtained from the processed traffic batches.
Output Data Flow
```text
Feature Selection Service
          |
          v
   selected-features
          |
          v
   Classifier Service
```
Technology Stack
Python
Pandas
NumPy
Scikit-learn
K-means
Apache Kafka
kafka-python
Docker
JSON
CSE-CIC-IDS2018 Dataset
Service Position
The Feature Selection Service is the third stage of the IR-IDS microservice architecture:
```text
1. CSV Ingestion Service
          |
          v
     raw-traffic
          |
          v
2. Preprocessing Service
          |
          v
    clean-traffic
          |
          v
3. Feature Selection Service
          |
          v
   selected-features
          |
          v
4. Classifier Service
```
Summary
The Feature Selection Service reduces the original network-traffic feature space using:
```text
K-means Preprocessing
        |
        v
Propensity Score
        |
        v
Causal Effect
        |
        v
DMB Growth
        |
        v
DMB Shrink
        |
        v
Batch DMB
        |
        v
Cumulative DMB
        |
        v
Selected Features
```
The final selected features are published to the Kafka `selected-features` topic and consumed by the Classifier Service.
