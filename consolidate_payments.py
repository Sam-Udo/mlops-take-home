"""
Consolidates payment transactions, account details, risk feeds, and fraud labels
into a unified table for fraud detection.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PaymentDataConsolidator:
    """
    Consolidates payment data from multiple sources into a unified table.
    Handles payments, accounts, risk feeds, and fraud labels.
    """
    
    def __init__(self, data_dir: str = "data"):
        """Initialize with data directory path."""
        self.data_dir = Path(data_dir)
        self.payments_df: Optional[pd.DataFrame] = None
        self.accounts_df: Optional[pd.DataFrame] = None
        self.risk_df: Optional[pd.DataFrame] = None
        self.fraud_df: Optional[pd.DataFrame] = None
        
    def load_payment_transactions(self, filename: str = "payment_transactions.csv") -> pd.DataFrame:
        """Load payment transactions from CSV."""
        filepath = self.data_dir / filename
        logger.info(f"Loading payment transactions from {filepath}")
        
        try:
            df = pd.read_csv(filepath)
            
            required_cols = ['payment_id', 'src_account_id', 'dest_account_id', 
                           'payment_reference', 'amount', 'timestamp']
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            logger.info(f"Loaded {len(df)} payment transactions")
            logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
            self.payments_df = df
            return df
            
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            raise
        except Exception as e:
            logger.error(f"Error loading payment transactions: {e}")
            raise
    
    def load_account_details(self, filename: str = "account_details.csv") -> pd.DataFrame:
        """Load account details from CSV."""
        filepath = self.data_dir / filename
        logger.info(f"Loading account details from {filepath}")
        
        try:
            df = pd.read_csv(filepath)
            
            required_cols = ['account_id', 'opening_date']
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            df['opening_date'] = pd.to_datetime(df['opening_date'])
            df = df.drop_duplicates(subset=['account_id'], keep='last')
            
            logger.info(f"Loaded {len(df)} unique accounts")
            
            self.accounts_df = df
            return df
            
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            raise
        except Exception as e:
            logger.error(f"Error loading account details: {e}")
            raise
    
    def load_risk_feed(self, filename: str = "external_risk_feed.csv") -> pd.DataFrame:
        """Load external risk feed. risk_flag: 0=clean, 1=suspicious, 2=confirmed."""
        filepath = self.data_dir / filename
        logger.info(f"Loading risk feed from {filepath}")
        
        try:
            df = pd.read_csv(filepath)
            
            required_cols = ['account_id', 'risk_flag']
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            valid_flags = {0, 1, 2}
            invalid_flags = set(df['risk_flag'].unique()) - valid_flags
            if invalid_flags:
                logger.warning(f"Invalid risk_flag values: {invalid_flags}, setting to 0")
                df.loc[~df['risk_flag'].isin(valid_flags), 'risk_flag'] = 0
            
            # Keep highest risk flag per account
            df = df.sort_values('risk_flag', ascending=False).drop_duplicates(
                subset=['account_id'], keep='first'
            )
            
            logger.info(f"Loaded risk flags for {len(df)} accounts")
            logger.info(f"Risk distribution: {df['risk_flag'].value_counts().to_dict()}")
            
            self.risk_df = df
            return df
            
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            raise
        except Exception as e:
            logger.error(f"Error loading risk feed: {e}")
            raise
    
    def load_fraud_cases(self, filename: str = "historical_fraud_cases.csv") -> pd.DataFrame:
        """Load historical fraud cases."""
        filepath = self.data_dir / filename
        logger.info(f"Loading fraud cases from {filepath}")
        
        try:
            df = pd.read_csv(filepath)
            
            required_cols = ['payment_id', 'fraud_type', 'fraud_reported_date']
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            df['fraud_reported_date'] = pd.to_datetime(df['fraud_reported_date'])
            df['fraud_flag'] = True
            
            # Keep latest report for each payment
            df = df.sort_values('fraud_reported_date', ascending=False).drop_duplicates(
                subset=['payment_id'], keep='first'
            )
            
            logger.info(f"Loaded {len(df)} fraud cases")
            logger.info(f"Fraud types: {df['fraud_type'].value_counts().to_dict()}")
            
            self.fraud_df = df
            return df
            
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            raise
        except Exception as e:
            logger.error(f"Error loading fraud cases: {e}")
            raise
    
    def consolidate(self) -> pd.DataFrame:
        """Consolidate all data sources into a unified table."""
        logger.info("Starting consolidation...")
        
        if self.payments_df is None:
            raise ValueError("Payment transactions not loaded")
        if self.accounts_df is None:
            raise ValueError("Account details not loaded")
        if self.risk_df is None:
            raise ValueError("Risk feed not loaded")
        if self.fraud_df is None:
            raise ValueError("Fraud cases not loaded")
        
        unified = self.payments_df.copy()
        logger.info(f"Starting with {len(unified)} payment transactions")
        
        # Join source account details
        unified = unified.merge(
            self.accounts_df.rename(columns={'opening_date': 'src_opening_date'}),
            left_on='src_account_id',
            right_on='account_id',
            how='left'
        ).drop(columns=['account_id'])
        logger.info(f"Joined source account details: {unified['src_opening_date'].notna().sum()} matches")
        
        # Join destination account details
        unified = unified.merge(
            self.accounts_df.rename(columns={'opening_date': 'dest_opening_date'}),
            left_on='dest_account_id',
            right_on='account_id',
            how='left'
        ).drop(columns=['account_id'])
        logger.info(f"Joined dest account details: {unified['dest_opening_date'].notna().sum()} matches")
        
        # Join risk flags for source accounts
        unified = unified.merge(
            self.risk_df.rename(columns={'risk_flag': 'src_risk_flag'}),
            left_on='src_account_id',
            right_on='account_id',
            how='left'
        ).drop(columns=['account_id'])
        
        unified['src_risk_flag'] = unified['src_risk_flag'].fillna(0).astype(int)
        logger.info(f"Joined source risk flags: {(unified['src_risk_flag'] > 0).sum()} flagged")
        
        # Join risk flags for destination accounts
        unified = unified.merge(
            self.risk_df.rename(columns={'risk_flag': 'dest_risk_flag'}),
            left_on='dest_account_id',
            right_on='account_id',
            how='left'
        ).drop(columns=['account_id'])
        
        unified['dest_risk_flag'] = unified['dest_risk_flag'].fillna(0).astype(int)
        logger.info(f"Joined dest risk flags: {(unified['dest_risk_flag'] > 0).sum()} flagged")
        
        # Join fraud labels
        unified = unified.merge(
            self.fraud_df[['payment_id', 'fraud_flag', 'fraud_type', 'fraud_reported_date']],
            on='payment_id',
            how='left'
        )
        
        unified['fraud_flag'] = unified['fraud_flag'].fillna(False).astype(bool)
        logger.info(f"Joined fraud labels: {unified['fraud_flag'].sum()} fraud cases")
        
        # Reorder columns
        column_order = [
            'payment_id', 'src_account_id', 'dest_account_id', 'payment_reference',
            'amount', 'timestamp', 'src_opening_date', 'dest_opening_date',
            'src_risk_flag', 'dest_risk_flag', 'fraud_flag', 'fraud_type', 
            'fraud_reported_date'
        ]
        unified = unified[column_order]
        
        logger.info(f"Consolidation complete: {len(unified)} records")
        logger.info(f"  - Source accounts with details: {unified['src_opening_date'].notna().sum()} "
                   f"({unified['src_opening_date'].notna().sum() / len(unified) * 100:.1f}%)")
        logger.info(f"  - Dest accounts with details: {unified['dest_opening_date'].notna().sum()} "
                   f"({unified['dest_opening_date'].notna().sum() / len(unified) * 100:.1f}%)")
        logger.info(f"  - Flagged source accounts: {(unified['src_risk_flag'] > 0).sum()}")
        logger.info(f"  - Flagged dest accounts: {(unified['dest_risk_flag'] > 0).sum()}")
        logger.info(f"  - Confirmed fraud: {unified['fraud_flag'].sum()} "
                   f"({unified['fraud_flag'].sum() / len(unified) * 100:.1f}%)")
        
        return unified
    
    def save_unified_table(self, df: pd.DataFrame, output_file: str = "unified_table.csv"):
        """Save the unified table to CSV."""
        output_path = Path(output_file)
        logger.info(f"Saving unified table to {output_path}")
        
        try:
            df.to_csv(output_path, index=False)
            logger.info(f"Successfully saved {len(df)} records to {output_path}")
        except Exception as e:
            logger.error(f"Error saving unified table: {e}")
            raise
    
    def run_pipeline(self, output_file: str = "unified_table.csv") -> pd.DataFrame:
        """Run the complete consolidation pipeline."""
        logger.info("="*80)
        logger.info("Payment Data Consolidation Pipeline")
        logger.info("="*80)
        
        try:
            self.load_payment_transactions()
            self.load_account_details()
            self.load_risk_feed()
            self.load_fraud_cases()
            
            unified_df = self.consolidate()
            self.save_unified_table(unified_df, output_file)
            
            logger.info("="*80)
            logger.info("Pipeline completed successfully")
            logger.info("="*80)
            
            return unified_df
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


def main():
    """Run the consolidation pipeline."""
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "data"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "unified_table.csv"
    
    consolidator = PaymentDataConsolidator(data_dir=data_dir)
    unified_df = consolidator.run_pipeline(output_file=output_file)
    
    print("\n" + "="*80)
    print("Sample of unified table (first 5 rows):")
    print("="*80)
    print(unified_df.head().to_string())
    print("\n" + "="*80)
    print("Summary:")
    print("="*80)
    print(f"Total records: {len(unified_df)}")
    print(f"Date range: {unified_df['timestamp'].min()} to {unified_df['timestamp'].max()}")
    print(f"Fraud cases: {unified_df['fraud_flag'].sum()} ({unified_df['fraud_flag'].sum() / len(unified_df) * 100:.2f}%)")
    print(f"Missing source account details: {unified_df['src_opening_date'].isna().sum()}")
    print(f"Missing dest account details: {unified_df['dest_opening_date'].isna().sum()}")
    print("="*80)


if __name__ == "__main__":
    main()
