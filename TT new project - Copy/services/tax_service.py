"""Tax calculation service for handling realized gains/losses and tax reporting."""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from .database_service import DatabaseService

class TaxService:
    """Service class for tax calculations and reporting."""
    
    def __init__(self, db_service: DatabaseService):
        self.db_service = db_service
    
    def calculate_realized_gains_losses(self, year: Optional[str] = None) -> Dict[str, Any]:
        """Calculate realized gains and losses for a specific year or YTD."""
        transactions = self._get_trade_transactions_for_period(year)
        dividend_data = self._get_dividend_data_for_period(year)
        
        positions_by_symbol = self._group_transactions_by_symbol(transactions)
        realized_transactions = []
        
        total_realized_gains_eur = 0
        total_realized_gains_usd = 0
        total_realized_losses_eur = 0
        total_realized_losses_usd = 0
        
        for symbol, symbol_transactions in positions_by_symbol.items():
            symbol_realized = self._calculate_symbol_realized_gains(
                symbol, symbol_transactions, year
            )
            
            realized_transactions.extend(symbol_realized['transactions'])
            total_realized_gains_eur += symbol_realized['gains_eur']
            total_realized_gains_usd += symbol_realized['gains_usd']
            total_realized_losses_eur += symbol_realized['losses_eur']
            total_realized_losses_usd += symbol_realized['losses_usd']
        
        return {
            'realized_gains': {
                'eur': total_realized_gains_eur,
                'usd': total_realized_gains_usd
            },
            'realized_losses': {
                'eur': abs(total_realized_losses_eur),
                'usd': abs(total_realized_losses_usd)
            },
            'net_realized': {
                'eur': total_realized_gains_eur + total_realized_losses_eur,
                'usd': total_realized_gains_usd + total_realized_losses_usd
            },
            'transactions': realized_transactions,
            'dividend_data': dividend_data
        }
    
    def _get_trade_transactions_for_period(self, year: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get trade transactions for a specific period."""
        base_query = """
            SELECT Date, Symbol, Action, Quantity, Average_Price, Total, Fees, Commissions
            FROM transactions 
            WHERE Type = 'Trade'
        """
        
        params = []
        if year and year != 'ytd':
            base_query += " AND substr(Date, 1, 4) = ?"
            params.append(str(year))
        elif year == 'ytd':
            current_year = str(datetime.now().year)
            base_query += " AND substr(Date, 1, 4) = ?"
            params.append(current_year)
        
        base_query += " ORDER BY Date ASC"
        
        rows = self.db_service.execute_query(base_query, tuple(params))
        
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
    
    def _get_dividend_data_for_period(self, year: Optional[str] = None) -> Dict[str, Any]:
        """Get dividend data for a specific period."""
        base_conditions = ["Type = 'Money Movement'", "Sub_Type = 'Dividend'"]
        params = []
        
        if year and year != 'ytd':
            base_conditions.append("substr(Date, 1, 4) = ?")
            params.append(str(year))
        elif year == 'ytd':
            current_year = str(datetime.now().year)
            base_conditions.append("substr(Date, 1, 4) = ?")
            params.append(current_year)
        
        where_clause = " AND ".join(base_conditions)
        
        # Get total dividends
        total_query = f"""
            SELECT SUM(CAST(Total AS REAL)) as total_dividends
            FROM transactions 
            WHERE {where_clause}
        """
        
        total_result = self.db_service.execute_single(total_query, tuple(params))
        total_dividends = float(total_result[0]) if total_result and total_result[0] else 0
        
        # Get dividends by symbol
        symbol_query = f"""
            SELECT Symbol, SUM(CAST(Total AS REAL)) as net_dividends
            FROM transactions 
            WHERE {where_clause}
            GROUP BY Symbol
        """
        
        symbol_rows = self.db_service.execute_query(symbol_query, tuple(params))
        
        dividends_by_symbol = {}
        for row in symbol_rows:
            dividends_by_symbol[row['Symbol']] = {
                'net_dividends': float(row['net_dividends']) if row['net_dividends'] else 0
            }
        
        return {
            'total_dividends': total_dividends,
            'by_symbol': dividends_by_symbol
        }
    
    def _group_transactions_by_symbol(self, transactions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group transactions by symbol."""
        positions_by_symbol = {}
        
        for transaction in transactions:
            symbol = transaction['symbol']
            if symbol not in positions_by_symbol:
                positions_by_symbol[symbol] = []
            positions_by_symbol[symbol].append(transaction)
        
        return positions_by_symbol
    
    def _calculate_symbol_realized_gains(self, symbol: str, transactions: List[Dict[str, Any]], 
                                       year: Optional[str] = None) -> Dict[str, Any]:
        """Calculate realized gains/losses for a specific symbol."""
        buys, sells = self._separate_buy_sell_transactions(transactions)
        
        realized_transactions = []
        total_gains_eur = 0
        total_gains_usd = 0
        total_losses_eur = 0
        total_losses_usd = 0
        
        # Process sells using FIFO method
        for sell in sells:
            if year and not self._is_transaction_in_period(sell['date'], year):
                continue
            
            remaining_to_sell = sell['quantity']
            sell_price_per_share = sell['price']
            
            while remaining_to_sell > 0 and buys:
                buy = buys[0]
                
                if buy['quantity'] <= remaining_to_sell:
                    # Complete position sold
                    sold_quantity = buy['quantity']
                    cost_basis = buy['total_cost'] + buy['fees']
                    proceeds = sold_quantity * sell_price_per_share
                    
                    gain_loss_eur = proceeds - cost_basis
                    gain_loss_usd = gain_loss_eur  # Simplified conversion
                    
                    if gain_loss_eur > 0:
                        total_gains_eur += gain_loss_eur
                        total_gains_usd += gain_loss_usd
                    else:
                        total_losses_eur += gain_loss_eur
                        total_losses_usd += gain_loss_usd
                    
                    realized_transactions.append({
                        'symbol': symbol,
                        'buy_date': buy['date'],
                        'sell_date': sell['date'],
                        'quantity': sold_quantity,
                        'buy_price': buy['price'],
                        'sell_price': sell_price_per_share,
                        'cost_basis': cost_basis,
                        'proceeds': proceeds,
                        'gain_loss_eur': gain_loss_eur,
                        'gain_loss_usd': gain_loss_usd
                    })
                    
                    remaining_to_sell -= buy['quantity']
                    buys.pop(0)
                else:
                    # Partial position sold
                    sold_quantity = remaining_to_sell
                    proportion = sold_quantity / buy['quantity']
                    cost_basis = (buy['total_cost'] + buy['fees']) * proportion
                    proceeds = sold_quantity * sell_price_per_share
                    
                    gain_loss_eur = proceeds - cost_basis
                    gain_loss_usd = gain_loss_eur  # Simplified conversion
                    
                    if gain_loss_eur > 0:
                        total_gains_eur += gain_loss_eur
                        total_gains_usd += gain_loss_usd
                    else:
                        total_losses_eur += gain_loss_eur
                        total_losses_usd += gain_loss_usd
                    
                    realized_transactions.append({
                        'symbol': symbol,
                        'buy_date': buy['date'],
                        'sell_date': sell['date'],
                        'quantity': sold_quantity,
                        'buy_price': buy['price'],
                        'sell_price': sell_price_per_share,
                        'cost_basis': cost_basis,
                        'proceeds': proceeds,
                        'gain_loss_eur': gain_loss_eur,
                        'gain_loss_usd': gain_loss_usd
                    })
                    
                    # Update remaining buy position
                    buy['quantity'] -= remaining_to_sell
                    buy['total_cost'] *= (1 - proportion)
                    buy['fees'] *= (1 - proportion)
                    remaining_to_sell = 0
        
        return {
            'transactions': realized_transactions,
            'gains_eur': total_gains_eur,
            'gains_usd': total_gains_usd,
            'losses_eur': total_losses_eur,
            'losses_usd': total_losses_usd
        }
    
    def _separate_buy_sell_transactions(self, transactions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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
    
    def _is_transaction_in_period(self, transaction_date: str, year: Optional[str]) -> bool:
        """Check if transaction is in the specified period."""
        if not year:
            return True
        
        transaction_year = transaction_date[:4]
        
        if year == 'ytd':
            current_year = str(datetime.now().year)
            return transaction_year == current_year
        else:
            return transaction_year == str(year)