"""Database service layer for transaction management system."""

import sqlite3
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class DatabaseService:
    """Service class for database operations."""
    
    def __init__(self, database_path: str = 'transactions.db'):
        self.database_path = database_path
    
    @contextmanager
    def get_connection(self):
        """Get database connection with automatic cleanup."""
        conn = None
        try:
            conn = sqlite3.connect(self.database_path)
            conn.row_factory = sqlite3.Row
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: Tuple = ()) -> List[sqlite3.Row]:
        """Execute a SELECT query and return results."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def execute_single(self, query: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
        """Execute a SELECT query and return single result."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchone()
    
    def execute_update(self, query: str, params: Tuple = ()) -> int:
        """Execute an INSERT/UPDATE/DELETE query and return affected rows."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a table."""
        query = f"PRAGMA table_info({table_name})"
        columns_info = self.execute_query(query)
        return [column[1] for column in columns_info]
    
    def get_total_records(self) -> int:
        """Get total number of transaction records."""
        result = self.execute_single("SELECT COUNT(*) FROM transactions")
        return result[0] if result else 0
    
    def get_date_range(self) -> Tuple[Optional[str], Optional[str]]:
        """Get min and max dates from transactions."""
        result = self.execute_single("SELECT MIN(Date), MAX(Date) FROM transactions")
        return (result[0], result[1]) if result else (None, None)
    
    def get_recent_transactions(self, limit: int = 10) -> List[sqlite3.Row]:
        """Get recent transactions ordered by date."""
        query = "SELECT rowid as rowid, * FROM transactions ORDER BY Date DESC LIMIT ?"
        return self.execute_query(query, (limit,))
    
    def get_transactions_by_symbol(self, symbol: str) -> List[sqlite3.Row]:
        """Get all transactions for a specific symbol."""
        query = """
            SELECT Date, Symbol, Action, Quantity, Average_Price, Total, Fees, Commissions
            FROM transactions 
            WHERE Type = 'Trade' AND Symbol = ?
            ORDER BY Date ASC
        """
        return self.execute_query(query, (symbol,))
    
    def get_all_trade_transactions(self) -> List[sqlite3.Row]:
        """Get all trade transactions ordered by date."""
        query = """
            SELECT Date, Symbol, Action, Quantity, Average_Price, Total, Fees, Commissions
            FROM transactions 
            WHERE Type = 'Trade'
            ORDER BY Date ASC
        """
        return self.execute_query(query)
    
    def save_stock_price(self, symbol: str, price: float, source: str) -> None:
        """Save stock price to database."""
        query = """
            INSERT OR REPLACE INTO stock_prices (symbol, price, source, timestamp)
            VALUES (?, ?, ?, datetime('now'))
        """
        self.execute_update(query, (symbol, price, source))
    
    def get_last_stock_price(self, symbol: str) -> Optional[float]:
        """Get the most recent stock price for a symbol."""
        query = """
            SELECT price FROM stock_prices 
            WHERE symbol = ? 
            ORDER BY timestamp DESC 
            LIMIT 1
        """
        result = self.execute_single(query, (symbol,))
        return result[0] if result else None
    
    def get_setting(self, key: str) -> Optional[str]:
        """Get a setting value by key."""
        query = "SELECT value FROM settings WHERE key = ?"
        result = self.execute_single(query, (key,))
        return result[0] if result else None
    
    def save_setting(self, key: str, value: str) -> None:
        """Save a setting value."""
        query = "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)"
        self.execute_update(query, (key, value))