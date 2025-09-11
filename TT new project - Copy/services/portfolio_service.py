"""Portfolio service for calculating gains, losses, and positions."""

from typing import Dict, List, Any, Optional
from datetime import datetime
import time
from .database_service import DatabaseService

class PortfolioService:
    """Service class for portfolio calculations and analysis."""
    
    def __init__(self, db_service: DatabaseService):
        self.db_service = db_service
        self._price_cache = {}
        self._cache_timeout = 300  # 5 minutes
    
    def calculate_unrealized_gains_losses(self, fetch_current_prices: bool = False) -> Dict[str, Any]:
        """Calculate unrealized gains and losses for current positions."""
        transactions = self._get_trade_transactions()
        dividend_data = self._get_dividend_data()
        
        positions_by_symbol = self._group_transactions_by_symbol(transactions)
        unrealized_positions = []
        total_cost_basis = 0
        total_unrealized_value = 0
        
        for symbol, symbol_transactions in positions_by_symbol.items():
            position_data = self._calculate_symbol_position(
                symbol, symbol_transactions, dividend_data, fetch_current_prices
            )
            
            if position_data:
                unrealized_positions.append(position_data)
                total_cost_basis += position_data['cost_basis']
                total_unrealized_value += position_data['current_value']
        
        total_unrealized_gain_loss = total_unrealized_value - total_cost_basis
        
        return {
            'total_unrealized_gain_loss': total_unrealized_gain_loss,
            'total_cost_basis': total_cost_basis,
            'total_current_value': total_unrealized_value,
            'positions': unrealized_positions
        }
    
    def _get_trade_transactions(self) -> List[Dict[str, Any]]:
        """Get all trade transactions from database."""
        query = """
            SELECT Date, Symbol, Action, Quantity, Average_Price, Total, Fees, Commissions
            FROM transactions 
            WHERE Type = 'Trade'
            ORDER BY Date ASC
        """
        rows = self.db_service.execute_query(query)
        
        transactions = []
        for row in rows:
            transactions.append({
                'date': row['Date'],
                'symbol': row['Symbol'],
                'action': row['Action'],
                'quantity': float(row['Quantity']) if row['Quantity'] else 0,
                'price': float(row['Average_Price']) if row['Average_Price'] else 0,
                'total': float(row['Total']) if row['Total'] else 0,
                'fees': float(row['Fees']) if row['Fees'] else 0,
                'commissions': float(row['Commissions']) if row['Commissions'] else 0
            })
        
        return transactions
    
    def _get_dividend_data(self) -> Dict[str, Dict[str, float]]:
        """Get dividend data grouped by symbol."""
        query = """
            SELECT Symbol, SUM(CAST(Total AS REAL)) as net_dividends
            FROM transactions 
            WHERE Type = 'Money Movement' AND Sub_Type = 'Dividend'
            GROUP BY Symbol
        """
        rows = self.db_service.execute_query(query)
        
        dividend_data = {}
        for row in rows:
            dividend_data[row['Symbol']] = {
                'net_dividends': float(row['net_dividends']) if row['net_dividends'] else 0
            }
        
        return dividend_data
    
    def _group_transactions_by_symbol(self, transactions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group transactions by symbol."""
        positions_by_symbol = {}
        
        for transaction in transactions:
            symbol = transaction['symbol']
            if symbol not in positions_by_symbol:
                positions_by_symbol[symbol] = []
            positions_by_symbol[symbol].append(transaction)
        
        return positions_by_symbol
    
    def _calculate_symbol_position(self, symbol: str, transactions: List[Dict[str, Any]], 
                                 dividend_data: Dict[str, Dict[str, float]], 
                                 fetch_current_prices: bool) -> Optional[Dict[str, Any]]:
        """Calculate position data for a specific symbol."""
        buys, sells = self._separate_buy_sell_transactions(transactions)
        remaining_buys = self._process_fifo_matching(buys, sells)
        
        if not remaining_buys:
            return None
        
        return self._build_position_data(symbol, remaining_buys, dividend_data, fetch_current_prices)
    
    def _separate_buy_sell_transactions(self, transactions: List[Dict[str, Any]]) -> tuple:
        """Separate buy and sell transactions."""
        buys = []
        sells = []
        
        for transaction in transactions:
            transaction_data = {
                'date': transaction['date'],
                'quantity': transaction['quantity'],
                'price': transaction['price'],
                'total_cost': abs(transaction['total']),
                'fees': transaction['fees'] + transaction['commissions']
            }
            
            if transaction['action'].upper() in ['BUY', 'DEPOSIT']:
                buys.append(transaction_data)
            elif transaction['action'].upper() in ['SELL', 'WITHDRAWAL']:
                sells.append(transaction_data)
        
        return buys, sells
    
    def _process_fifo_matching(self, buys: List[Dict[str, Any]], sells: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process FIFO matching of buy and sell transactions."""
        remaining_buys = buys.copy()
        
        for sell in sells:
            remaining_to_sell = sell['quantity']
            
            while remaining_to_sell > 0 and remaining_buys:
                buy = remaining_buys[0]
                
                if buy['quantity'] <= remaining_to_sell:
                    # Complete position sold
                    remaining_to_sell -= buy['quantity']
                    remaining_buys.pop(0)
                else:
                    # Partial position sold
                    buy['quantity'] -= remaining_to_sell
                    buy['total_cost'] = (buy['quantity'] / (buy['quantity'] + remaining_to_sell)) * buy['total_cost']
                    buy['fees'] = (buy['quantity'] / (buy['quantity'] + remaining_to_sell)) * buy['fees']
                    remaining_to_sell = 0
        
        return remaining_buys
    
    def _build_position_data(self, symbol: str, buys: List[Dict[str, Any]], 
                           dividend_data: Dict[str, Dict[str, float]], 
                           fetch_current_prices: bool) -> Dict[str, Any]:
        """Build position data dictionary for a symbol."""
        total_quantity = sum(buy['quantity'] for buy in buys)
        total_cost_basis = sum(buy['total_cost'] + buy['fees'] for buy in buys)
        
        if total_quantity <= 0:
            return None
        
        weighted_avg_cost = total_cost_basis / total_quantity
        current_price = self._get_current_price(symbol, weighted_avg_cost, fetch_current_prices)
        current_value = total_quantity * current_price
        unrealized_gain_loss = current_value - total_cost_basis
        
        earliest_date = min(buy['date'] for buy in buys)
        net_dividends = dividend_data.get(symbol, {}).get('net_dividends', 0)
        dividends_per_share = net_dividends / total_quantity if total_quantity > 0 else 0
        adjusted_avg_cost = weighted_avg_cost - dividends_per_share
        
        return {
            'symbol': symbol,
            'quantity': total_quantity,
            'avg_cost': weighted_avg_cost,
            'current_price': current_price,
            'cost_basis': total_cost_basis,
            'current_value': current_value,
            'unrealized_gain_loss': unrealized_gain_loss,
            'buy_date': earliest_date,
            'net_dividends': net_dividends,
            'adjusted_avg_cost': adjusted_avg_cost
        }
    
    def _get_current_price(self, symbol: str, fallback_price: float, fetch_current_prices: bool) -> float:
        """Get current price for a symbol with various fallback options."""
        if fetch_current_prices:
            # This would call the external price API
            # For now, return fallback price
            return fallback_price
        
        # Check cache first
        cache_key = symbol.strip().upper()
        if cache_key in self._price_cache:
            current_time = time.time()
            cached_data = self._price_cache[cache_key]
            if current_time - cached_data['timestamp'] < self._cache_timeout:
                return cached_data['price']
        
        # Try database
        db_price = self.db_service.get_last_stock_price(symbol)
        if db_price is not None:
            return db_price
        
        # Use fallback price
        return fallback_price
    
    def cache_price(self, symbol: str, price: float) -> None:
        """Cache a stock price."""
        cache_key = symbol.strip().upper()
        self._price_cache[cache_key] = {
            'price': price,
            'timestamp': time.time()
        }