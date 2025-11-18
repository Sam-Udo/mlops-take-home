"""
Consolidation pipeline with streaming patterns.
Simulates different ingestion patterns: real-time API, daily batch, irregular updates, delayed labels.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Iterator, Dict, Optional, List
import time
from datetime import datetime, timedelta
import random
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PaymentAPIStream:
    """Simulates a real-time payment API stream."""
    
    def __init__(self, source_file: str, chunk_size: int = 5, delay_seconds: float = 1.0):
        """Initialize payment stream simulator."""
        self.source_file = source_file
        self.chunk_size = chunk_size
        self.delay_seconds = delay_seconds
        self._data = None
    
    def connect(self):
        """Connect to API and load data."""
        logger.info(f"Connecting to payment API (simulated from {self.source_file})")
        self._data = pd.read_csv(self.source_file)
        self._data['timestamp'] = pd.to_datetime(self._data['timestamp'])
        logger.info(f"Connected. {len(self._data)} transactions available")
    
    def stream_transactions(self) -> Iterator[pd.DataFrame]:
        """Stream transactions in chunks."""
        if self._data is None:
            raise ValueError("API not connected. Call connect() first.")
        
        total_chunks = (len(self._data) + self.chunk_size - 1) // self.chunk_size
        
        for i in range(0, len(self._data), self.chunk_size):
            chunk = self._data.iloc[i:i + self.chunk_size].copy()
            chunk_num = i // self.chunk_size + 1
            
            logger.info(f"Streaming chunk {chunk_num}/{total_chunks} ({len(chunk)} transactions)")
            
            time.sleep(self.delay_seconds)  # Simulate API latency
            
            yield chunk


class FraudLabelAPIStream:
    """Simulates delayed fraud label API."""
    
    def __init__(self, source_file: str, delay_days: int = 45):
        """Initialize fraud label stream with delay."""
        self.source_file = source_file
        self.delay_days = delay_days
        self._data = None
    
    def connect(self):
        """Connect to fraud label API."""
        logger.info(f"Connecting to fraud label API ({self.delay_days}-day delay)")
        self._data = pd.read_csv(self.source_file)
        self._data['fraud_reported_date'] = pd.to_datetime(self._data['fraud_reported_date'])
        logger.info(f"Connected. {len(self._data)} labels available")
    
    def stream_labels(self, current_date: datetime) -> Iterator[Dict]:
        """Stream fraud labels available by current_date."""
        if self._data is None:
            raise ValueError("API not connected. Call connect() first.")
        
        available_labels = self._data[
            self._data['fraud_reported_date'] <= current_date
        ]
        
        logger.info(f"{len(available_labels)} labels available as of {current_date.date()}")
        
        for _, row in available_labels.iterrows():
            time.sleep(0.1)
            
            yield {
                'payment_id': row['payment_id'],
                'fraud_type': row['fraud_type'],
                'fraud_reported_date': row['fraud_reported_date'],
                'fraud_flag': True
            }


class RiskFeedBatchLoader:
    """Simulates irregular risk feed batch arrivals."""
    
    def __init__(self, data_dir: str):
        """
        Initialize risk feed loader.
        
        Args:
            data_dir: Directory containing risk feed batch files
        """
        self.data_dir = Path(data_dir)
    
    def list_batches(self) -> List[Path]:
        """List available risk feed batch files."""
        pattern = "risk_feed_batch_*.csv"
        batches = sorted(self.data_dir.glob(pattern))
        logger.info(f"Found {len(batches)} risk feed batch files")
        return batches
    
    def load_batch(self, batch_file: Path, delay_seconds: float = 2.0) -> pd.DataFrame:
        """
        Load a risk feed batch file.
        
        Args:
            batch_file: Path to batch file
            delay_seconds: Simulated network/processing delay
            
        Returns:
            DataFrame with risk data
        """
        logger.info(f"Loading risk feed batch: {batch_file.name}")
        
        # Simulate batch processing delay
        time.sleep(delay_seconds)
        
        df = pd.read_csv(batch_file)
        logger.info(f"Loaded {len(df)} risk records from {batch_file.name}")
        
        return df


# ============================================================================
# STREAMING CONSOLIDATOR
# ============================================================================

class StreamingPaymentConsolidator:
    """
    Consolidates payment data with streaming ingestion patterns.
    Maintains state for incremental updates.
    """
    
    def __init__(self):
        """Initialize streaming consolidator with empty state."""
        self.unified_table: Optional[pd.DataFrame] = None
        self.payment_buffer: List[pd.DataFrame] = []
        self.accounts_snapshot: Optional[pd.DataFrame] = None
        self.risk_feed_state: Optional[pd.DataFrame] = None
        self.fraud_labels_state: Optional[pd.DataFrame] = None
        
        logger.info("Initialized StreamingPaymentConsolidator")
    
    def update_account_snapshot(self, csv_file: str):
        """
        Update account details from daily CSV batch.
        
        Args:
            csv_file: Path to daily account snapshot CSV
        """
        logger.info(f"Updating account snapshot from {csv_file}")
        
        df = pd.read_csv(csv_file)
        df['opening_date'] = pd.to_datetime(df['opening_date'])
        df = df.drop_duplicates(subset=['account_id'], keep='last')
        
        self.accounts_snapshot = df
        logger.info(f"Account snapshot updated: {len(df)} accounts")
    
    def update_risk_feed(self, batch_df: pd.DataFrame):
        """
        Update risk feed from irregular batch.
        Maintains latest risk flag per account.
        
        Args:
            batch_df: DataFrame with new risk data
        """
        logger.info(f"Updating risk feed with {len(batch_df)} records")
        
        # Validate and clean
        batch_df = batch_df.copy()
        valid_flags = {0, 1, 2}
        batch_df.loc[~batch_df['risk_flag'].isin(valid_flags), 'risk_flag'] = 0
        
        if self.risk_feed_state is None:
            self.risk_feed_state = batch_df
        else:
            # Merge with existing state, keeping highest risk flag
            combined = pd.concat([self.risk_feed_state, batch_df])
            combined = combined.sort_values('risk_flag', ascending=False).drop_duplicates(
                subset=['account_id'], keep='first'
            )
            self.risk_feed_state = combined
        
        logger.info(f"Risk feed state: {len(self.risk_feed_state)} accounts")
    
    def update_fraud_labels(self, label_records: List[Dict]):
        """
        Update fraud labels from streaming API.
        
        Args:
            label_records: List of fraud label dictionaries
        """
        if not label_records:
            return
        
        logger.info(f"Updating fraud labels with {len(label_records)} records")
        
        new_labels = pd.DataFrame(label_records)
        new_labels['fraud_reported_date'] = pd.to_datetime(new_labels['fraud_reported_date'])
        
        if self.fraud_labels_state is None:
            self.fraud_labels_state = new_labels
        else:
            # Append and deduplicate (keep latest)
            combined = pd.concat([self.fraud_labels_state, new_labels])
            combined = combined.sort_values('fraud_reported_date', ascending=False).drop_duplicates(
                subset=['payment_id'], keep='first'
            )
            self.fraud_labels_state = combined
        
        logger.info(f"Fraud labels state: {len(self.fraud_labels_state)} labeled payments")
    
    def process_payment_stream(self, payment_chunk: pd.DataFrame):
        """
        Process a chunk of payment transactions from stream.
        
        Args:
            payment_chunk: DataFrame chunk from payment API
        """
        logger.info(f"Processing payment chunk: {len(payment_chunk)} transactions")
        
        # Add to buffer
        self.payment_buffer.append(payment_chunk)
        
        # Consolidate buffer when it reaches threshold
        total_buffered = sum(len(chunk) for chunk in self.payment_buffer)
        logger.info(f"Payment buffer: {total_buffered} transactions (across {len(self.payment_buffer)} chunks)")
    
    def consolidate_buffered_payments(self) -> pd.DataFrame:
        """
        Consolidate all buffered payments with latest state.
        
        Returns:
            Unified DataFrame
        """
        logger.info("Consolidating buffered payments...")
        
        if not self.payment_buffer:
            logger.warning("No payments in buffer")
            return pd.DataFrame()
        
        # Combine all buffered payments
        all_payments = pd.concat(self.payment_buffer, ignore_index=True)
        logger.info(f"Consolidating {len(all_payments)} payments")
        
        # Start with payments
        unified = all_payments.copy()
        
        # Join accounts (if available)
        if self.accounts_snapshot is not None:
            # Source accounts
            unified = unified.merge(
                self.accounts_snapshot.rename(columns={'opening_date': 'src_opening_date'}),
                left_on='src_account_id',
                right_on='account_id',
                how='left'
            ).drop(columns=['account_id'], errors='ignore')
            
            # Destination accounts
            unified = unified.merge(
                self.accounts_snapshot.rename(columns={'opening_date': 'dest_opening_date'}),
                left_on='dest_account_id',
                right_on='account_id',
                how='left'
            ).drop(columns=['account_id'], errors='ignore')
        else:
            unified['src_opening_date'] = pd.NaT
            unified['dest_opening_date'] = pd.NaT
        
        # Join risk flags (if available)
        if self.risk_feed_state is not None:
            # Source risk
            unified = unified.merge(
                self.risk_feed_state.rename(columns={'risk_flag': 'src_risk_flag'}),
                left_on='src_account_id',
                right_on='account_id',
                how='left'
            ).drop(columns=['account_id'], errors='ignore')
            
            # Dest risk
            unified = unified.merge(
                self.risk_feed_state.rename(columns={'risk_flag': 'dest_risk_flag'}),
                left_on='dest_account_id',
                right_on='account_id',
                how='left'
            ).drop(columns=['account_id'], errors='ignore')
        else:
            unified['src_risk_flag'] = 0
            unified['dest_risk_flag'] = 0
        
        # Fill missing risk flags with 0
        unified['src_risk_flag'] = unified['src_risk_flag'].fillna(0).astype(int)
        unified['dest_risk_flag'] = unified['dest_risk_flag'].fillna(0).astype(int)
        
        # Join fraud labels (if available)
        if self.fraud_labels_state is not None:
            unified = unified.merge(
                self.fraud_labels_state[['payment_id', 'fraud_flag', 'fraud_type', 'fraud_reported_date']],
                on='payment_id',
                how='left'
            )
        else:
            unified['fraud_flag'] = False
            unified['fraud_type'] = None
            unified['fraud_reported_date'] = pd.NaT
        
        # Fill missing fraud flags
        unified['fraud_flag'] = unified['fraud_flag'].fillna(False).astype(bool)
        
        # Reorder columns
        column_order = [
            'payment_id', 'src_account_id', 'dest_account_id', 'payment_reference',
            'amount', 'timestamp', 'src_opening_date', 'dest_opening_date',
            'src_risk_flag', 'dest_risk_flag', 'fraud_flag', 'fraud_type',
            'fraud_reported_date'
        ]
        unified = unified[column_order]
        
        logger.info(f"Consolidation complete: {len(unified)} records")
        logger.info(f"  - Fraud cases: {unified['fraud_flag'].sum()}")
        logger.info(f"  - Accounts with details: {unified['src_opening_date'].notna().sum()}")
        logger.info(f"  - Flagged accounts (src): {(unified['src_risk_flag'] > 0).sum()}")
        
        self.unified_table = unified
        return unified
    
    def save_unified_table(self, output_file: str = "unified_table_streaming.csv"):
        """Save current unified table to CSV."""
        if self.unified_table is None or len(self.unified_table) == 0:
            logger.warning("No unified table to save")
            return
        
        logger.info(f"Saving unified table to {output_file}")
        self.unified_table.to_csv(output_file, index=False)
        logger.info(f"Saved {len(self.unified_table)} records")


# ============================================================================
# STREAMING PIPELINE
# ============================================================================

def run_streaming_pipeline(data_dir: str = "data"):
    """
    Run the complete streaming consolidation pipeline.
    
    Simulates:
    1. Real-time payment stream processing
    2. Daily account batch update
    3. Irregular risk feed batches
    4. Delayed fraud label stream
    """
    logger.info("="*80)
    logger.info("STREAMING PAYMENT DATA CONSOLIDATION PIPELINE")
    logger.info("="*80)
    
    data_path = Path(data_dir)
    consolidator = StreamingPaymentConsolidator()
    
    # ========================================================================
    # STEP 1: Load daily account snapshot (simulates daily 6 AM batch)
    # ========================================================================
    logger.info("\n" + "="*80)
    logger.info("STEP 1: Loading daily account snapshot (batch update)")
    logger.info("="*80)
    consolidator.update_account_snapshot(data_path / "account_details.csv")
    
    # ========================================================================
    # STEP 2: Stream payment transactions (simulates real-time API)
    # ========================================================================
    logger.info("\n" + "="*80)
    logger.info("STEP 2: Streaming payment transactions (real-time API)")
    logger.info("="*80)
    
    payment_stream = PaymentAPIStream(
        source_file=data_path / "payment_transactions.csv",
        chunk_size=5,
        delay_seconds=0.5  # Simulate API latency
    )
    payment_stream.connect()
    
    # Process payment stream in chunks
    for chunk in payment_stream.stream_transactions():
        consolidator.process_payment_stream(chunk)
    
    # ========================================================================
    # STEP 3: Load risk feed batches (simulates irregular arrivals)
    # ========================================================================
    logger.info("\n" + "="*80)
    logger.info("STEP 3: Loading risk feed batches (irregular timing)")
    logger.info("="*80)
    
    risk_loader = RiskFeedBatchLoader(data_dir=data_path)
    
    # Check if batch files exist, if not use main file
    risk_batches = risk_loader.list_batches()
    if not risk_batches:
        logger.info("No batch files found, loading main risk feed file")
        risk_df = pd.read_csv(data_path / "external_risk_feed.csv")
        consolidator.update_risk_feed(risk_df)
    else:
        for batch_file in risk_batches:
            batch_df = risk_loader.load_batch(batch_file, delay_seconds=1.0)
            consolidator.update_risk_feed(batch_df)
    
    # ========================================================================
    # STEP 4: Stream fraud labels (simulates delayed API with 45-day lag)
    # ========================================================================
    logger.info("\n" + "="*80)
    logger.info("STEP 4: Streaming fraud labels (delayed 45 days)")
    logger.info("="*80)
    
    fraud_stream = FraudLabelAPIStream(
        source_file=data_path / "historical_fraud_cases.csv",
        delay_days=45
    )
    fraud_stream.connect()
    
    # Simulate current date (45+ days after transactions)
    current_date = datetime.now()
    
    fraud_records = list(fraud_stream.stream_labels(current_date))
    consolidator.update_fraud_labels(fraud_records)
    
    # ========================================================================
    # STEP 5: Consolidate all buffered data
    # ========================================================================
    logger.info("\n" + "="*80)
    logger.info("STEP 5: Consolidating all data sources")
    logger.info("="*80)
    
    unified_df = consolidator.consolidate_buffered_payments()
    
    # ========================================================================
    # STEP 6: Save unified table
    # ========================================================================
    logger.info("\n" + "="*80)
    logger.info("STEP 6: Saving unified table")
    logger.info("="*80)
    
    consolidator.save_unified_table("unified_table_streaming.csv")
    
    logger.info("\n" + "="*80)
    logger.info("STREAMING PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("="*80)
    
    # Display summary
    print("\n" + "="*80)
    print("UNIFIED TABLE SUMMARY (Streaming Mode):")
    print("="*80)
    print(f"Total records: {len(unified_df)}")
    print(f"Date range: {unified_df['timestamp'].min()} to {unified_df['timestamp'].max()}")
    print(f"Fraud rate: {unified_df['fraud_flag'].sum() / len(unified_df) * 100:.2f}%")
    print(f"Accounts with details: {unified_df['src_opening_date'].notna().sum()} / {len(unified_df)}")
    print(f"Flagged source accounts: {(unified_df['src_risk_flag'] > 0).sum()}")
    print(f"Flagged dest accounts: {(unified_df['dest_risk_flag'] > 0).sum()}")
    print("="*80)
    
    print("\nFirst 3 records:")
    print(unified_df.head(3).to_string())
    
    return unified_df


if __name__ == "__main__":
    import sys
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "data"
    run_streaming_pipeline(data_dir)
