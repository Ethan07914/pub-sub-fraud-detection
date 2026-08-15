# Pub/Sub Fraud Detection

A learning/portfolio project that simulates a **card testing attack** flowing through an event-driven pipeline, and detects it using a hybrid rule-based + ML approach.

## Overview

Card testing is a common fraud pattern where an attacker with a batch of stolen or generated card numbers runs many small, rapid transactions against a merchant to see which cards are still valid — before using the working ones for larger purchases elsewhere. This project simulates that pattern end-to-end: a producer publishes a steady stream of normal transactions, randomly interrupted by bursts that look like card testing, and a subscriber service consumes that stream in real time to flag the attack.

The goal is to demonstrate a realistic event-driven architecture (pub/sub, streaming consumption, hybrid detection logic) rather than to build a production-grade fraud system.

## Architecture

```
[Event Producer] --publishes--> [Pub/Sub Topic] --consumes--> [Detection Service]
                                                                      |
                                                     +----------------+----------------+
                                                     |                                 |
                                              [Alerting/Output]                  [Dashboard]
```

- **Event Producer** — generates transaction events and publishes them to the topic
- **Pub/Sub Topic** — cloud-managed message bus decoupling producer from consumer(s)
- **Detection Service** — subscribes to the topic, scores each event, flags suspicious activity
- **Alerting/Output** — surfaces flagged transactions/attack windows downstream
- **Dashboard** — visualizes the live transaction stream and flagged activity

## Components

### Event Producer
Simulates transaction events with fields like: amount, merchant, card/account id, source IP or device id, approval/decline status, and timestamp.

Runs at a **steady baseline volume** of normal-looking transactions, and randomly triggers **card-testing attack spikes** carrying these warning signs:
- **High velocity, low value** — many transactions in a short window, each for a small amount
- **High decline rate** — a large share of the spike's transactions get declined, as the attacker tests which cards still work
- **Same source, many cards** — one IP address / device fingerprint attempting many distinct card numbers in quick succession

### Detection Service
Subscribes to the topic and applies a **hybrid** detection approach:
1. **Rule-based checks** (fast, explainable, catch the obvious cases):
   - Transaction velocity per source (IP/device)
   - Low-value + high-frequency pattern
   - Decline-rate threshold within a rolling window
   - Distinct-card-count per source in a rolling window
2. **ML model** for subtler or borderline events that don't trip the hard-coded rules

Emits a fraud score/verdict per event.

### Alerting/Output
Flagged transactions and detected attack windows are pushed to a secondary queue/topic or webhook for downstream consumption.

### Dashboard
Lightweight UI showing the live transaction stream, with attack spikes and flagged transactions visibly called out — e.g. a volume chart with spikes highlighted, plus a table of flagged events.

## Tech Stack

- **Language**: Python
- **Pub/Sub**: Google Pub/Sub (`google-cloud-pubsub`)
- **ML**: Random Forest classifier (`scikit-learn`) for the "subtler pattern" detection layer, alongside the rule-based checks
- **Service/API**: Flask or FastAPI
- **Dashboard**: simple frontend (framework TBD) served alongside the API

## Open Questions

- What features feed the Random Forest model (e.g. rolling velocity, decline rate, distinct-card count, amount, time-of-day)?
- What labeled/simulated data is used to train it, since there's no real fraud data?
- Exact thresholds that define a "flagged" transaction for the rule-based layer (velocity/decline-rate/distinct-card cutoffs)
- How the dashboard will be built (framework, hosting, refresh mechanism)

## Status

Planning stage — no code written yet. This README captures the intended architecture and scope before implementation begins.

## Setup

**Select project**
```bash
gcloud config set project PROJECT_ID
```

**Create datasets**
```bash
bq mk --dataset --location=us-central1 fraud_staging

bq mk --dataset --location=us-central1 fraud_analytics
```

**Create tables**
```bash
bq mk --table \
  fraud_staging.transactions \
  transaction_id:STRING,timestamp:TIMESTAMP,amount:FLOAT,merchant_id:STRING,card_id:STRING,source_ip:STRING,status:STRING
```

**Create Pub/Sub topic and subscription**
```bash
gcloud pubsub topics create fraud-data

gcloud pubsub subscriptions create fraud-data_sub --topic=fraud-data
```

**Set Pub/Sub subscription to write to a staging table in the Cloud Console UI**

**Create the view for analysis**

```bash
bq query --use_legacy_sql=false < sql/vw_card_testing_monitor.sql
```


