# MLOps Take-Home Test

This repository contains my solutions for both tasks.

## Deliverables

### Task One: Data Architecture Design (2-5 pages)
**Document**: `Task_One_Unified_Payment_Intelligence_Strategy.pdf`

A unified payment intelligence table architecture covering:
- Real-time serving, batch processing, and feature computation strategy
- Unified table schema design
- Medallion architecture (Bronze/Silver/Gold layers)
- Data quality and monitoring strategy
- Handling delayed fraud labels (30-60 day lag)
- 6-month implementation roadmap
- Tech stack for small team (Airflow, Spark, Parquet, Great Expectations)

### Task Two: Data Consolidation Pipeline
**Document**: `Task_Two_Data_Consolidation_Pipeline.pdf`

Python implementation with two versions:
- `consolidate_payments.py` - Batch processing version
- `consolidate_payments_streaming.py` - Streaming patterns version (bonus)
- `data/` - Sample CSV files (4 data sources)
- `unified_table.csv` & `unified_table_streaming.csv` - Output files
- `requirements.txt` - Python dependencies

## Quick Start

### Running Task Two

Install dependencies:
```bash
pip install -r requirements.txt
```

Run the basic version:
```bash
python3 consolidate_payments.py data unified_table.csv
```

Run the streaming version:
```bash
python3 consolidate_payments_streaming.py data
```

Both scripts will process the CSV files in the `data/` directory and generate output files.

## File Structure

```
.
├── Task_One_Unified_Payment_Intelligence_Strategy.pdf    # Architecture document
├── Task_Two_Data_Consolidation_Pipeline.pdf              # Technical guide
├── consolidate_payments.py                               # Main script
├── consolidate_payments_streaming.py                     # Streaming version
├── requirements.txt                                      # Dependencies
├── README.md                                             # This file
├── data/                                                 # Input data
│   ├── payment_transactions.csv                         # 15 transactions
│   ├── account_details.csv                              # 22 accounts
│   ├── external_risk_feed.csv                           # 6 risk flags
│   └── historical_fraud_cases.csv                       # 4 fraud cases
├── unified_table.csv                                     # Output (basic)
└── unified_table_streaming.csv                           # Output (streaming)
```

## Task One: Architecture Strategy

The PDF document covers a medallion architecture (Bronze/Silver/Gold) for consolidating payment data. Key points:

- **Real-time serving strategy**: Payment API streaming (15-min batches), path to full real-time if needed
- **Batch processing strategy**: Medallion architecture with Airflow orchestration, daily Gold table refresh
- **Feature computation**: Point-in-time correctness to prevent data leakage in training
- **Data quality & monitoring**: Three-layer validation (Bronze/Silver/Gold), Prometheus + Grafana monitoring
- **Defensive design**: Assumes upstream systems will break, schema change detection, dead letter queue
- **Implementation plan**: 6-month incremental roadmap for small team
- **Tech stack**: Airflow, Spark, Parquet, Great Expectations, PostgreSQL

Designed for a first MLOps hire establishing capabilities in an environment where upstream systems change without notice.

## Task Two: Implementation

Two working Python implementations:

**Basic version** (`consolidate_payments.py`):
- Batch processing from CSV files
- LEFT joins for source and destination accounts
- Defaults: risk_flag=0 for clean accounts, fraud_flag=False for unlabeled
- Validation, logging, error handling
- Output: `unified_table.csv` (15 records, 4 fraud cases, 26.7% fraud rate)

**Streaming version** (`consolidate_payments_streaming.py`):
- Simulates different ingestion patterns (real-time API, daily batch, irregular updates, delayed labels)
- Generator patterns for streaming
- Stateful buffers and consolidation
- Same output, different architectural approach
- Output: `unified_table_streaming.csv` (identical results)

Both scripts are tested and produce identical, correct output. See `Task_Two_Data_Consolidation_Pipeline.pdf` for detailed technical documentation.
