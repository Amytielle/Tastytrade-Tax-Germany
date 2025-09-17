from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file, make_response
import sqlite3
from datetime import datetime
import os
import csv
import io
import logging
import traceback
import secrets
from werkzeug.utils import secure_filename
import requests
import time
from contextlib import contextmanager
from functools import wraps

app = Flask(__name__)
# Use environment variable for secret key with secure fallback
app.secret_key = os.environ.get('SECRET_KEY') or secrets.token_hex(16)

# Database configuration
DATABASE = 'transactions.db'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def log_error(error, context=""):
    """Log error with context information"""
    error_msg = f"Error in {context}: {str(error)}"
    logger.error(error_msg)
    logger.error(f"Traceback: {traceback.format_exc()}")

def handle_database_error(func):
    """Decorator to handle database errors consistently"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except sqlite3.Error as e:
            log_error(e, f"Database operation in {func.__name__}")
            flash(f'Database error occurred. Please try again.', 'error')
            return redirect(url_for('index'))
        except Exception as e:
            log_error(e, f"Unexpected error in {func.__name__}")
            flash(f'An unexpected error occurred. Please try again.', 'error')
            return redirect(url_for('index'))
    return wrapper

def validate_request_data(required_fields=None, optional_fields=None):
    """Decorator to validate request data"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if request.method == 'POST':
                # Check for required fields
                if required_fields:
                    missing_fields = []
                    for field in required_fields:
                        if not request.form.get(field):
                            missing_fields.append(field)
                    
                    if missing_fields:
                        flash(f'Missing required fields: {", ".join(missing_fields)}', 'error')
                        return redirect(request.url)
                
                # Validate and sanitize all form data
                sanitized_data = {}
                for key, value in request.form.items():
                    sanitized_data[key] = sanitize_input(value)
                
                # Replace request.form with sanitized data
                request.form = sanitized_data
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

@contextmanager
def db_connection():
    """Database connection context manager for automatic connection handling"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row  # This enables column access by name
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()

def get_table_columns():
    """Get all column names from the transactions table"""
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(transactions)")
        columns = [column[1] for column in cursor.fetchall()]
        return columns

@app.route('/')
def index():
    """Main dashboard page"""
    with db_connection() as conn:
        cursor = conn.cursor()
        
        # Total records
        cursor.execute("SELECT COUNT(*) FROM transactions")
        total_records = cursor.fetchone()[0]
        
        # Get current holdings data
        holdings_data = calculate_unrealized_gains_losses()
        current_holdings = holdings_data['positions']
        
        # Prepare data for holdings pie chart
        holdings_chart_data = []
        holdings_chart_data_eur = []
        holdings_chart_labels = []
        holdings_chart_colors = []
        
        # Color palette for pie chart
        colors = [
            '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
            '#FF9F40', '#FF6384', '#C9CBCF', '#4BC0C0', '#FF6384'
        ]
        
        # Get current date for EUR conversion
        from datetime import datetime
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # Calculate total wire funds received using historical exchange rates
        cursor.execute("""
            SELECT Date, Value 
            FROM transactions 
            WHERE Type = 'Money Movement' 
            AND Sub_Type = 'Deposit' 
            AND Description = 'Wire Funds Received'
        """)
        wire_funds_transactions = cursor.fetchall()
        
        wire_funds_total_usd = 0
        wire_funds_total_eur = 0
        
        for transaction in wire_funds_transactions:
            date_str = transaction[0][:10] if transaction[0] else current_date  # Extract date part
            usd_amount = transaction[1] or 0
            
            # Convert each transaction using its historical exchange rate
            eur_amount = convert_usd_to_eur(usd_amount, date_str) or 0
            
            wire_funds_total_usd += usd_amount
            wire_funds_total_eur += eur_amount
        
        # Calculate difference between total value and wire funds
        total_value_usd = holdings_data.get('total_current_value', 0)
        total_value_eur = convert_usd_to_eur(total_value_usd, current_date) or 0
        
        difference_usd = total_value_usd - wire_funds_total_usd
        difference_eur = total_value_eur - wire_funds_total_eur
        
        if current_holdings:
            for i, holding in enumerate(current_holdings):
                if holding['current_value'] and holding['current_value'] > 0:
                    holdings_chart_labels.append(holding['symbol'])
                    usd_value = float(holding['current_value'])
                    holdings_chart_data.append(usd_value)
                    
                    # Convert to EUR using current exchange rate
                    eur_value = convert_usd_to_eur(usd_value, current_date) or 0
                    holdings_chart_data_eur.append(eur_value)
                    
                    holdings_chart_colors.append(colors[i % len(colors)])
        
        # Date range
        cursor.execute("SELECT MIN(Date), MAX(Date) FROM transactions")
        date_range = cursor.fetchone()
        
        # Get recent transactions with EUR conversions
        cursor.execute("SELECT rowid as rowid, * FROM transactions ORDER BY Date DESC LIMIT 10")
        recent_transactions = cursor.fetchall()
        
        # Add EUR conversions for USD transactions
        transactions_with_eur = []
        for transaction in recent_transactions:
            transaction_dict = dict(transaction)
            
            if transaction_dict.get('Currency') == 'USD' and transaction_dict.get('Date'):
                date_str = transaction_dict['Date']
                
                # Convert key USD amounts to EUR
                if transaction_dict.get('Value'):
                    transaction_dict['Value_EUR'] = convert_usd_to_eur(transaction_dict['Value'], date_str)
                if transaction_dict.get('Total'):
                    transaction_dict['Total_EUR'] = convert_usd_to_eur(transaction_dict['Total'], date_str)
                if transaction_dict.get('Average_Price'):
                    transaction_dict['Average_Price_EUR'] = convert_usd_to_eur(transaction_dict['Average_Price'], date_str)
                if transaction_dict.get('Commissions'):
                    transaction_dict['Commissions_EUR'] = convert_usd_to_eur(transaction_dict['Commissions'], date_str)
                if transaction_dict.get('Fees'):
                    transaction_dict['Fees_EUR'] = convert_usd_to_eur(transaction_dict['Fees'], date_str)
            
            transactions_with_eur.append(transaction_dict)
        
        return render_template('index.html', 
                             total_records=total_records,
                             current_holdings=current_holdings,
                             date_range=date_range,
                             recent_transactions=transactions_with_eur,
                             holdings_chart_data=holdings_chart_data,
                             holdings_chart_data_eur=holdings_chart_data_eur,
                             holdings_chart_labels=holdings_chart_labels,
                             holdings_chart_colors=holdings_chart_colors,
                             wire_funds_total_usd=wire_funds_total_usd,
                             wire_funds_total_eur=wire_funds_total_eur,
                             difference_usd=difference_usd,
                             difference_eur=difference_eur)

@app.route('/transactions')
def transactions():
    """View all transactions with pagination and filtering"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    search = request.args.get('search', '')
    filter_type = request.args.get('type', '')
    filter_symbol = request.args.get('symbol', '')
    
    with db_connection() as conn:
        cursor = conn.cursor()
    
        # Build query with filters
        query = "SELECT rowid as rowid, * FROM transactions WHERE 1=1"
        params = []
    
        if search:
            query += " AND (Symbol LIKE ? OR Description LIKE ? OR Action LIKE ?)"
            search_param = f"%{search}%"
            params.extend([search_param, search_param, search_param])
    
        if filter_type:
            query += " AND Type = ?"
            params.append(filter_type)
    
        if filter_symbol:
            if filter_symbol == 'Option':
                # Filter for all option transactions
                query += " AND Asset_Category = 'Option'"
            else:
                # Filter for specific symbol
                query += " AND Symbol = ?"
                params.append(filter_symbol)
    
        # Count total records for pagination
        count_query = query.replace("SELECT rowid as rowid, *", "SELECT COUNT(*)")
        cursor.execute(count_query, params)
        total_records = cursor.fetchone()[0]
    
        # Add pagination
        query += " ORDER BY Date DESC LIMIT ? OFFSET ?"
        params.extend([per_page, (page - 1) * per_page])
    
        cursor.execute(query, params)
        transactions = cursor.fetchall()
    
        # Convert to list of dictionaries and add USD_EUR_Rate for each transaction
        transactions_with_rates = []
        for transaction in transactions:
            transaction_dict = dict(transaction)
        
            # Get exchange rate for this transaction's date
            if transaction_dict.get('Date'):
                try:
                    # Extract date from datetime string
                    date_str = transaction_dict['Date'][:10]  # Get YYYY-MM-DD part
                    exchange_rate = get_exchange_rate(date_str)
                    transaction_dict['USD_EUR_Rate'] = exchange_rate
                except:
                    transaction_dict['USD_EUR_Rate'] = None
            else:
                transaction_dict['USD_EUR_Rate'] = None
            
            transactions_with_rates.append(transaction_dict)
    
        # Get unique types and symbols for filters
        cursor.execute("SELECT DISTINCT Type FROM transactions WHERE Type IS NOT NULL ORDER BY Type")
        types = [row[0] for row in cursor.fetchall()]

        # Get symbols, but group options under 'Option' category
        cursor.execute("SELECT DISTINCT Symbol, Asset_Category FROM transactions WHERE Symbol IS NOT NULL ORDER BY Symbol")
        symbol_rows = cursor.fetchall()
        
        symbols = []
        has_options = False
        
        for row in symbol_rows:
            symbol, asset_category = row
            if asset_category == 'Option':
                has_options = True
            else:
                symbols.append(symbol)
        
        # Add 'Option' as a grouped category if there are any options
        if has_options:
            symbols.insert(0, 'Option')
        
        symbols.sort()
    
    # Replace transactions with the enhanced version
    transactions = transactions_with_rates
    
    # Calculate pagination info
    total_pages = (total_records + per_page - 1) // per_page
    has_prev = page > 1
    has_next = page < total_pages
    
    columns = get_table_columns()
    
    return render_template('transactions.html',
                         transactions=transactions,
                         columns=columns,
                         page=page,
                         per_page=per_page,
                         total_records=total_records,
                         total_pages=total_pages,
                         has_prev=has_prev,
                         has_next=has_next,
                         search=search,
                         filter_type=filter_type,
                         filter_symbol=filter_symbol,
                         types=types,
                         symbols=symbols)

@app.route('/tax')
def tax():
    """Tax reporting and analysis page"""
    # Get query parameters
    tax_year = request.args.get('taxYear', 'ytd')
    report_type = request.args.get('reportType', 'summary')
    
    # Get available years for dropdown
    year_data = get_available_tax_years()
    
    # Calculate tax data
    realized_gains_losses = calculate_realized_gains_losses(year=tax_year)
    unrealized_gains_losses = calculate_unrealized_gains_losses()
    dividend_data = get_dividend_data(year=tax_year)
    fees_data = get_fees_data(year=tax_year)
    
    # Convert USD values to EUR using current date
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # Calculate EUR values for realized gains/losses using historical rates from individual transactions
    realized_gains_eur = sum([
        realized_gains_losses.get('stock_gains_eur', 0),
        realized_gains_losses.get('option_gains_eur', 0),
        realized_gains_losses.get('other_gains_eur', 0)
    ])
    realized_losses_eur = sum([
        realized_gains_losses.get('stock_losses_eur', 0),
        realized_gains_losses.get('option_losses_eur', 0),
        realized_gains_losses.get('other_losses_eur', 0)
    ])
    unrealized_gain_loss_eur = convert_usd_to_eur(unrealized_gains_losses['total_unrealized_gain_loss'], current_date) or 0
    dividends_eur = dividend_data.get('total_dividends_eur', 0)
    fees_eur = fees_data.get('total_fees_eur', 0)
    
    # Use actual source tax from database (negative dividend entries)
    source_tax_usd = dividend_data['total_source_tax']
    source_tax_eur = dividend_data.get('total_source_tax_eur', 0)
    
    # Calculate EUR values for German tax separation using historical rates from individual transactions
    # This ensures consistency with the detailed transaction EUR values
    stock_gains_eur = realized_gains_losses.get('stock_gains_eur', 0)
    stock_losses_eur = realized_gains_losses.get('stock_losses_eur', 0)
    option_gains_eur = realized_gains_losses.get('option_gains_eur', 0)
    option_losses_eur = realized_gains_losses.get('option_losses_eur', 0)
    other_gains_eur = realized_gains_losses.get('other_gains_eur', 0)
    other_losses_eur = realized_gains_losses.get('other_losses_eur', 0)
    
    tax_summary = {
        'realized_gains': {
            'usd': realized_gains_losses['total_gains'],
            'eur': realized_gains_eur
        },
        'realized_losses': {
            'usd': realized_gains_losses['total_losses'],
            'eur': realized_losses_eur
        },
        'unrealized_gain_loss': {
            'usd': unrealized_gains_losses['total_unrealized_gain_loss'],
            'eur': unrealized_gain_loss_eur
        },
        'dividends': {
            'usd': dividend_data['total_dividends'],
            'eur': dividends_eur
        },
        'source_tax': {
            'usd': source_tax_usd,
            'eur': source_tax_eur
        },
        'total_fees': {
            'usd': fees_data['total_fees'],
            'eur': fees_eur
        },
        'net_realized_gain_loss': {
            'usd': realized_gains_losses['net_gain_loss'],
            'eur': realized_gains_eur - realized_losses_eur
        },
        # German tax separation
        'stock_gains': {
            'usd': realized_gains_losses.get('stock_gains', 0),
            'eur': stock_gains_eur
        },
        'stock_losses': {
            'usd': realized_gains_losses.get('stock_losses', 0),
            'eur': stock_losses_eur
        },
        'stock_net_gain_loss': {
            'usd': realized_gains_losses.get('stock_net_gain_loss', 0),
            'eur': stock_gains_eur - stock_losses_eur
        },
        'option_gains': {
            'usd': realized_gains_losses.get('option_gains', 0),
            'eur': option_gains_eur
        },
        'option_losses': {
            'usd': realized_gains_losses.get('option_losses', 0),
            'eur': option_losses_eur
        },
        'option_net_gain_loss': {
            'usd': realized_gains_losses.get('option_gains', 0) - realized_gains_losses.get('option_losses', 0),
            'eur': option_gains_eur - option_losses_eur
        },
        'other_gains': {
            'usd': realized_gains_losses.get('other_gains', 0),
            'eur': other_gains_eur
        },
        'other_losses': {
            'usd': realized_gains_losses.get('other_losses', 0),
            'eur': other_losses_eur
        },
        'other_net_gain_loss': {
            'usd': realized_gains_losses.get('other_net_gain_loss', 0),
            'eur': other_gains_eur - other_losses_eur
        }
    }
    
    return render_template('tax.html', 
                         tax_summary=tax_summary,
                         tax_year=tax_year,
                         report_type=report_type,
                         year_data=year_data,
                         detailed_transactions=realized_gains_losses['detailed_transactions'],
                         unrealized_positions=unrealized_gains_losses['positions'],
                         dividend_by_symbol=dividend_data['dividend_by_symbol'][:10],  # Top 10 dividend stocks
                         source_tax_by_symbol=dividend_data['source_tax_by_symbol'][:10])  # Top 10 source tax stocks

@app.route('/transaction/<int:rowid>')
def view_transaction(rowid):
    """View single transaction details"""
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT rowid as rowid, * FROM transactions WHERE rowid = ?", (rowid,))
        transaction = cursor.fetchone()
    
    if transaction is None:
        flash('Transaction not found', 'error')
        return redirect(url_for('transactions'))
    
    # Convert to dictionary and add USD_EUR_Rate
    transaction_dict = dict(transaction)
    if transaction_dict.get('Date'):
        try:
            # Extract date from datetime string
            date_str = transaction_dict['Date'][:10]  # Get YYYY-MM-DD part
            exchange_rate = get_exchange_rate(date_str)
            transaction_dict['USD_EUR_Rate'] = exchange_rate
        except:
            transaction_dict['USD_EUR_Rate'] = None
    else:
        transaction_dict['USD_EUR_Rate'] = None
    
    columns = get_table_columns()
    return render_template('view_transaction.html', transaction=transaction_dict, columns=columns)

def get_dropdown_data():
    """Get data for dropdown options from database"""
    with db_connection() as conn:
        cursor = conn.cursor()
    
        # Get unique transaction types
        cursor.execute("SELECT DISTINCT Type FROM transactions WHERE Type IS NOT NULL ORDER BY Type")
        transaction_types = [row[0] for row in cursor.fetchall()]
    
        # Get unique sub types
        cursor.execute("SELECT DISTINCT Sub_Type FROM transactions WHERE Sub_Type IS NOT NULL ORDER BY Sub_Type")
        sub_types = [row[0] for row in cursor.fetchall()]
    
        # Get unique instrument types
        cursor.execute("SELECT DISTINCT Instrument_Type FROM transactions WHERE Instrument_Type IS NOT NULL ORDER BY Instrument_Type")
        instrument_types = [row[0] for row in cursor.fetchall()]
    
        # Get unique currencies
        cursor.execute("SELECT DISTINCT Currency FROM transactions WHERE Currency IS NOT NULL ORDER BY Currency")
        currencies = [row[0] for row in cursor.fetchall()]
    
        # Get unique actions
        cursor.execute("SELECT DISTINCT Action FROM transactions WHERE Action IS NOT NULL ORDER BY Action")
        actions = [row[0] for row in cursor.fetchall()]
    
    # Sub-type mapping based on existing data
    sub_type_mapping = {
        'Trade': ['Buy to Open', 'Buy to Close', 'Sell to Open', 'Sell to Close', 'Buy', 'Sell'],
        'Money Movement': ['Deposit', 'Withdrawal', 'Transfer', 'Fee', 'Interest', 'Dividend'],
        'Receive Deliver': ['Assignment', 'Exercise', 'Expiration']
    }
    
    return {
        'transaction_types': transaction_types if transaction_types else ['Trade', 'Money Movement', 'Receive Deliver'],
        'sub_types': sub_types,
        'sub_type_mapping': sub_type_mapping,
        'instrument_types': instrument_types if instrument_types else ['Stock', 'Option', 'Future', 'Bond'],
        'currencies': currencies if currencies else ['USD', 'EUR', 'GBP', 'CAD'],
        'actions': actions if actions else ['BUY', 'SELL', 'ASSIGN', 'EXERCISE']
    }

def sanitize_input(value):
    """Sanitize input to prevent XSS and other injection attacks"""
    if not value:
        return value
    
    # Convert to string and strip whitespace
    value = str(value).strip()
    
    # Remove potentially dangerous characters but preserve numeric formatting
    # Keep parentheses, dots, commas, hyphens, and plus signs for numeric values
    dangerous_chars = ['<', '>', '"', "'", '&', ';', '{', '}', '[', ']']
    for char in dangerous_chars:
        value = value.replace(char, '')
    
    # Limit length to prevent buffer overflow attacks
    if len(value) > 500:
        value = value[:500]
    
    return value

def validate_file_upload(file):
    """Validate uploaded file for security and format"""
    errors = []
    
    if not file:
        errors.append('No file provided')
        return errors
    
    if not file.filename:
        errors.append('No file selected')
        return errors
    
    # Check file extension
    allowed_extensions = ['.csv', '.txt']
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in allowed_extensions:
        errors.append(f'Invalid file type. Only {', '.join(allowed_extensions)} files are allowed')
    
    # Check file size (limit to 10MB)
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)  # Reset file pointer
    
    max_size = 10 * 1024 * 1024  # 10MB
    if file_size > max_size:
        errors.append(f'File too large. Maximum size is {max_size // (1024*1024)}MB')
    
    if file_size == 0:
        errors.append('File is empty')
    
    # Check filename for dangerous characters
    dangerous_chars = ['..', '/', '\\', '<', '>', ':', '"', '|', '?', '*']
    for char in dangerous_chars:
        if char in file.filename:
            errors.append('Filename contains invalid characters')
            break
    
    return errors

def validate_numeric_field(value, field_name, min_val=None, max_val=None, allow_negative=True):
    """Validate numeric fields with range checking"""
    errors = []
    
    if value is None or value == '':
        return errors
    
    try:
        num_value = float(value)
        
        # Check for negative values if not allowed
        if not allow_negative and num_value < 0:
            errors.append(f'{field_name} cannot be negative')
        
        # Check minimum value
        if min_val is not None and num_value < min_val:
            errors.append(f'{field_name} must be at least {min_val}')
        
        # Check maximum value
        if max_val is not None and num_value > max_val:
            errors.append(f'{field_name} cannot exceed {max_val}')
        
        # Check for reasonable precision (max 4 decimal places)
        if len(str(num_value).split('.')[-1]) > 4:
            errors.append(f'{field_name} cannot have more than 4 decimal places')
            
    except (ValueError, TypeError):
        errors.append(f'{field_name} must be a valid number')
    
    return errors

def validate_date_field(date_str, field_name, allow_future=True):
    """Validate date fields with format and range checking"""
    errors = []
    
    if not date_str:
        return errors
    
    try:
        # Handle timezone formats by removing timezone info first
        cleaned_date_str = date_str
        
        # Remove timezone info (e.g., +0200, -0500, Z)
        import re
        # Match timezone patterns: +HHMM, -HHMM, +HH:MM, -HH:MM, Z
        timezone_pattern = r'([+-]\d{2}:?\d{2}|Z)$'
        cleaned_date_str = re.sub(timezone_pattern, '', date_str)
        
        # Try multiple date formats (most common first)
        date_formats = [
            '%Y-%m-%dT%H:%M:%S',  # 2024-01-15T14:30:00 (ISO format)
            '%Y-%m-%d %H:%M:%S',  # 2024-01-15 14:30:00
            '%Y-%m-%d %H:%M',     # 2024-01-15 14:30
            '%Y-%m-%dT%H:%M',     # 2024-01-15T14:30
            '%Y-%m-%d',           # 2024-01-15
            '%m/%d/%Y %H:%M:%S',  # 01/15/2024 14:30:00
            '%m/%d/%Y %H:%M',     # 01/15/2024 14:30
            '%m/%d/%Y',           # 01/15/2024
            '%m/%d/%y',           # 4/11/25 (MM/DD/YY format)
            '%d/%m/%Y %H:%M:%S',  # 15/01/2024 14:30:00
            '%d/%m/%Y %H:%M',     # 15/01/2024 14:30
            '%d/%m/%Y',           # 15/01/2024
            '%d/%m/%y',           # 15/01/25 (DD/MM/YY format)
            '%m-%d-%Y',           # 01-15-2024
            '%d-%m-%Y',           # 15-01-2024
            '%m-%d-%y',           # 4-11-25 (MM-DD-YY format)
            '%d-%m-%y',           # 15-01-25 (DD-MM-YY format)
            '%Y/%m/%d',           # 2024/01/15
            '%y/%m/%d',           # 25/01/15 (YY/MM/DD format)
            '%d.%m.%Y',           # 15.01.2024
            '%Y.%m.%d',           # 2024.01.15
            '%d.%m.%y',           # 15.01.25 (DD.MM.YY format)
            '%y.%m.%d'            # 25.01.15 (YY.MM.DD format)
        ]
        parsed_date = None
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(cleaned_date_str, fmt)
                break
            except ValueError:
                continue
        
        if not parsed_date:
            errors.append(f'{field_name} has invalid date format. Expected formats: YYYY-MM-DD, MM/DD/YYYY, DD/MM/YYYY, etc.')
            return errors
        
        # Check if date is too far in the past (before 1900)
        if parsed_date.year < 1900:
            errors.append(f'{field_name} cannot be before year 1900')
        
        # Check if date is in the future (if not allowed)
        if not allow_future and parsed_date > datetime.now():
            errors.append(f'{field_name} cannot be in the future')
        
        # Check if date is too far in the future (more than 10 years)
        future_limit = datetime.now().replace(year=datetime.now().year + 10)
        if parsed_date > future_limit:
            errors.append(f'{field_name} cannot be more than 10 years in the future')
            
    except Exception:
        errors.append(f'{field_name} has invalid date format')
    
    return errors

def validate_symbol_field(symbol):
    """Validate stock/option symbol format"""
    errors = []
    
    if not symbol:
        return errors
    
    # Convert to uppercase but preserve spaces for option symbols
    symbol = symbol.upper()
    
    # Check length (1-25 characters to accommodate option symbols)
    if len(symbol) < 1 or len(symbol) > 25:
        errors.append('Symbol must be 1-25 characters long')
    
    # Check for valid characters (letters, numbers, dots, hyphens, spaces for options)
    import re
    if not re.match(r'^[A-Z0-9.\-\s]+$', symbol):
        errors.append('Symbol can only contain letters, numbers, dots, hyphens, and spaces')
    
    return errors

def validate_transaction_data(data, is_edit=False):
    """Enhanced validation for transaction data"""
    errors = []
    
    # Sanitize all input fields
    sanitized_data = {}
    for key, value in data.items():
        sanitized_data[key] = sanitize_input(value)
    data = sanitized_data
    
    # Required fields validation
    required_fields = ['Date', 'Type', 'Sub_Type']
    for field in required_fields:
        if not data.get(field):
            errors.append(f'{field.replace("_", " ")} is required')
    
    # Date validation
    if data.get('Date'):
        errors.extend(validate_date_field(data['Date'], 'Date', allow_future=False))
    
    # Numeric field validation with ranges
    # Value, Total, Average_Price, and Fees can be negative for certain transaction types
    numeric_validations = {
        'Quantity': {'min_val': 0, 'max_val': 1000000, 'allow_negative': False},
        'Value': {'max_val': 10000000, 'allow_negative': True},  # Allow negative for dividends, fees
        'Average_Price': {'max_val': 100000, 'allow_negative': True},  # Allow negative for short sales, cost basis
        'Total': {'max_val': 10000000, 'allow_negative': True},  # Allow negative for dividends, fees
        'Commissions': {'max_val': 10000, 'allow_negative': True},  # Allow negative for rebates/adjustments
        'Fees': {'max_val': 10000, 'allow_negative': True},  # Allow negative for fee rebates/adjustments
        'Strike_Price': {'min_val': 0, 'max_val': 100000, 'allow_negative': False},
        'Multiplier': {'min_val': 1, 'max_val': 1000, 'allow_negative': False}
    }
    
    for field, validation_params in numeric_validations.items():
        if data.get(field) is not None and data[field] != '':
            # Clean the numeric value before validation
            cleaned_value = clean_numeric_value(data[field])
            if cleaned_value is not None:
                errors.extend(validate_numeric_field(
                    cleaned_value, 
                    field.replace('_', ' '), 
                    **validation_params
                ))
    
    # Symbol validation
    if data.get('Symbol'):
        errors.extend(validate_symbol_field(data['Symbol']))
    
    # Business logic validation
    if data.get('Type') == 'Trade':
        if not data.get('Symbol'):
            errors.append('Symbol is required for Trade transactions')
        if not data.get('Action'):
            errors.append('Action is required for Trade transactions')
        
        # Validate action values
        valid_actions = ['BUY_TO_OPEN', 'SELL_TO_CLOSE', 'BUY_TO_CLOSE', 'SELL_TO_OPEN']
        if data.get('Action') and data['Action'] not in valid_actions:
            errors.append(f'Action must be one of: {", ".join(valid_actions)}')
    
    # Options-specific validation
    if data.get('Instrument_Type') and 'option' in data['Instrument_Type'].lower():
        if not data.get('Expiration_Date'):
            errors.append('Expiration Date is required for Options')
        else:
            errors.extend(validate_date_field(data['Expiration_Date'], 'Expiration Date', allow_future=True))
        
        if not data.get('Strike_Price'):
            errors.append('Strike Price is required for Options')
        
        if not data.get('Call_or_Put'):
            errors.append('Call or Put selection is required for Options')
        elif data['Call_or_Put'].upper() not in ['CALL', 'PUT']:
            errors.append('Call or Put must be either "Call" or "Put"')
    
    # Currency validation
    if data.get('Currency'):
        valid_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF']
        if data['Currency'] not in valid_currencies:
            errors.append(f'Currency must be one of: {", ".join(valid_currencies)}')
    
    # Cross-field validation
    if data.get('Quantity') and data.get('Average_Price') and data.get('Total'):
        try:
            quantity = float(clean_numeric_value(data['Quantity']) or 0)
            avg_price = float(clean_numeric_value(data['Average_Price']) or 0)
            total = float(clean_numeric_value(data['Total']) or 0)
            commissions = float(clean_numeric_value(data.get('Commissions', 0)) or 0)
            fees = float(clean_numeric_value(data.get('Fees', 0)) or 0)
            
            # Expected total should include fees and commissions: Total = (Quantity × Average Price) + Commissions + Fees
            base_amount = quantity * avg_price
            expected_total = base_amount + commissions + fees
            
            # Allow 2% tolerance for rounding differences and broker calculation variations
            tolerance = max(abs(expected_total * 0.02), 0.05)  # At least 5 cents tolerance
            if abs(total - expected_total) > tolerance:
                errors.append('Total amount does not match Quantity × Average Price + Commissions + Fees (within 2% tolerance)')
        except (ValueError, TypeError):
            pass  # Skip validation if conversion fails
    
    return errors

@app.route('/transaction/add', methods=['GET', 'POST'])
@handle_database_error
@validate_request_data(required_fields=['Date', 'Type', 'Sub_Type'])
def add_transaction():
    """Add new transaction"""
    if request.method == 'POST':
        with db_connection() as conn:
            cursor = conn.cursor()
        
            # Get form data
            data = {}
            columns = get_table_columns()
        
            for column in columns:
                value = request.form.get(column, '').strip()
                if value == '':
                    data[column] = None
                else:
                    data[column] = value
        
            # Validate data
            validation_errors = validate_transaction_data(data)
            if validation_errors:
                for error in validation_errors:
                    flash(error, 'error')
                dropdown_data = get_dropdown_data()
                return render_template('add_transaction.html', columns=columns, transaction=data, **dropdown_data)
        
            # Convert numeric fields after validation
            for column in columns:
                value = data[column]
                if value is not None:
                    # Convert numeric fields
                    if column.lower() in ['value', 'quantity', 'average_price', 'commissions', 'fees', 'multiplier', 'strike_price', 'total']:
                        try:
                            data[column] = float(value) if value else None
                        except ValueError:
                            data[column] = None
                    elif column.lower() in ['order_number']:
                        try:
                            data[column] = int(value) if value else None
                        except ValueError:
                            data[column] = None
            
            # Automatically categorize the asset
            if 'Asset_Category' in data and data.get('Symbol'):
                data['Asset_Category'] = categorize_asset(
                    symbol=data.get('Symbol'),
                    instrument_type=data.get('Instrument_Type'),
                    call_or_put=data.get('Call_or_Put'),
                    strike_price=data.get('Strike_Price'),
                    expiration_date=data.get('Expiration_Date')
                )
        
            # Insert into database
            placeholders = ', '.join(['?' for _ in columns])
            column_names = ', '.join([f'"{col}"' for col in columns])
            values = [data[col] for col in columns]
        
            try:
                cursor.execute(f'INSERT INTO transactions ({column_names}) VALUES ({placeholders})', values)
                conn.commit()
                flash('Transaction added successfully!', 'success')
                return redirect(url_for('transactions'))
            except Exception as e:
                flash(f'Error adding transaction: {str(e)}', 'error')
    
    columns = get_table_columns()
    dropdown_data = get_dropdown_data()
    return render_template('add_transaction.html', columns=columns, **dropdown_data)

@app.route('/transaction/edit/<int:rowid>', methods=['GET', 'POST'])
@handle_database_error
@validate_request_data(required_fields=['Date', 'Type', 'Sub_Type'])
def edit_transaction(rowid):
    """Edit existing transaction"""
    with db_connection() as conn:
        cursor = conn.cursor()
    
        if request.method == 'POST':
            # Get form data
            data = {}
            columns = get_table_columns()
        
            for column in columns:
                value = request.form.get(column, '').strip()
                if value == '':
                    data[column] = None
                else:
                    data[column] = value
        
            # Validate data
            validation_errors = validate_transaction_data(data, is_edit=True)
            if validation_errors:
                for error in validation_errors:
                    flash(error, 'error')
                # Get existing transaction data for re-rendering form
                cursor.execute("SELECT rowid as rowid, * FROM transactions WHERE rowid = ?", (rowid,))
                transaction = cursor.fetchone()
            # Merge form data with existing transaction for display
            transaction_dict = dict(transaction) if transaction else {}
            transaction_dict.update({k: v for k, v in data.items() if v is not None})
            transaction_dict['rowid'] = rowid
            columns = get_table_columns()
            dropdown_data = get_dropdown_data()
            return render_template('edit_transaction.html', transaction=transaction_dict, columns=columns, **dropdown_data)
        
        # Convert numeric fields after validation
        for column in columns:
            value = data[column]
            if value is not None:
                # Convert numeric fields
                if column.lower() in ['value', 'quantity', 'average_price', 'commissions', 'fees', 'multiplier', 'strike_price', 'total']:
                    try:
                        data[column] = float(value) if value else None
                    except ValueError:
                        data[column] = None
                elif column.lower() in ['order_number']:
                    try:
                        data[column] = int(value) if value else None
                    except ValueError:
                        data[column] = None
        
        # Automatically categorize the asset
        if 'Asset_Category' in data and data.get('Symbol'):
            data['Asset_Category'] = categorize_asset(
                symbol=data.get('Symbol'),
                instrument_type=data.get('Instrument_Type'),
                call_or_put=data.get('Call_or_Put'),
                strike_price=data.get('Strike_Price'),
                expiration_date=data.get('Expiration_Date')
            )
        
        # Update database
        set_clause = ', '.join([f'"{col}" = ?' for col in columns])
        values = [data[col] for col in columns] + [rowid]
        
        try:
            cursor.execute(f'UPDATE transactions SET {set_clause} WHERE rowid = ?', values)
            conn.commit()
            flash('Transaction updated successfully!', 'success')
            return redirect(url_for('view_transaction', rowid=rowid))
        except Exception as e:
            flash(f'Error updating transaction: {str(e)}', 'error')
    
    # Get existing transaction data
    cursor.execute("SELECT rowid as rowid, * FROM transactions WHERE rowid = ?", (rowid,))
    transaction = cursor.fetchone()
    
    if transaction is None:
        flash('Transaction not found', 'error')
        return redirect(url_for('transactions'))
    
    columns = get_table_columns()
    dropdown_data = get_dropdown_data()
    return render_template('edit_transaction.html', transaction=transaction, columns=columns, **dropdown_data)

@app.route('/transaction/delete/<int:rowid>', methods=['POST'])
def delete_transaction(rowid):
    """Delete transaction"""
    with db_connection() as conn:
        cursor = conn.cursor()
    
        try:
            cursor.execute("DELETE FROM transactions WHERE rowid = ?", (rowid,))
            conn.commit()
            flash('Transaction deleted successfully!', 'success')
        except Exception as e:
            flash(f'Error deleting transaction: {str(e)}', 'error')
    
    return redirect(url_for('transactions'))

@app.route('/clear_database', methods=['POST'])
def clear_database():
    """Clear all transactions from the database"""
    with db_connection() as conn:
        cursor = conn.cursor()
    
        try:
            # Delete all transactions
            cursor.execute("DELETE FROM transactions")
            # Reset the auto-increment counter
            cursor.execute("DELETE FROM sqlite_sequence WHERE name='transactions'")
            conn.commit()
        
            flash('Database cleared successfully! All transactions have been deleted.', 'success')
        except Exception as e:
            flash(f'Error clearing database: {str(e)}', 'error')
    
    return redirect(url_for('transactions'))

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics"""
    with db_connection() as conn:
        cursor = conn.cursor()
    
        # Get various statistics
        stats = {}
    
        # Total records
        cursor.execute("SELECT COUNT(*) FROM transactions")
        stats['total_records'] = cursor.fetchone()[0]
    
        # Transaction types
        cursor.execute("SELECT Type, COUNT(*) FROM transactions GROUP BY Type ORDER BY COUNT(*) DESC")
        stats['transaction_types'] = dict(cursor.fetchall())
    
        # Monthly transaction count
        cursor.execute("""
            SELECT strftime('%Y-%m', Date) as month, COUNT(*) 
            FROM transactions 
            WHERE Date IS NOT NULL 
            GROUP BY month 
            ORDER BY month DESC 
            LIMIT 12
        """)
        stats['monthly_counts'] = dict(cursor.fetchall())
    return jsonify(stats)

@app.route('/api/dropdown-data')
def api_dropdown_data():
    """API endpoint for dropdown data"""
    dropdown_data = get_dropdown_data()
    return jsonify(dropdown_data)

@app.route('/export')
def export_transactions():
    """Export all transactions to CSV file"""
    try:
        with db_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM transactions ORDER BY Date DESC')
            transactions = cursor.fetchall()
        
        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        if transactions:
            writer.writerow(transactions[0].keys())
            
            # Write data
            for transaction in transactions:
                writer.writerow(transaction)
        
        # Create response
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=transactions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        flash('Transactions exported successfully!', 'success')
        return response
        
    except Exception as e:
        flash(f'Error exporting transactions: {str(e)}', 'error')
        return redirect(url_for('index'))

def parse_and_import_exchange_rates():
    """Parse the latest exchange rates CSV and import into database"""
    try:
        filepath = os.path.join(os.getcwd(), 'latest_usd_eur_rates.csv')
        
        if not os.path.exists(filepath):
            return False, "Exchange rates file not found. Please update rates first."
        
        imported_count = 0
        updated_count = 0
        
        with db_connection() as conn:
            cursor = conn.cursor()
            
            with open(filepath, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    # Skip header lines (first 10 lines contain metadata)
                    if line_num <= 10:
                        continue
                    
                    line = line.strip()
                    if not line or line.startswith('""'):
                        continue
                    
                    # Parse the line: date;rate;flag
                    parts = line.split(';')
                    if len(parts) >= 2:
                        date_str = parts[0]
                        rate_str = parts[1]
                        
                        # Skip entries with no value
                        if rate_str == '.' or rate_str == 'Kein Wert vorhanden':
                            continue
                        
                        try:
                            # Convert German decimal format (comma) to English (dot)
                            rate_value = float(rate_str.replace(',', '.'))
                            
                            # Try to insert, if exists then update
                            cursor.execute('''
                                INSERT OR REPLACE INTO exchange_rates (date, usd_to_eur_rate)
                                VALUES (?, ?)
                            ''', (date_str, rate_value))
                            
                            if cursor.rowcount > 0:
                                imported_count += 1
                            
                        except ValueError:
                            continue  # Skip invalid rate values
            
            conn.commit()
            
        return True, f"Successfully imported {imported_count} exchange rates"
        
    except Exception as e:
        return False, f"Error importing exchange rates: {str(e)}"

def get_exchange_rate(date_str):
    """Get USD to EUR exchange rate for a specific date"""
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            
            # Try to get exact date match first
            cursor.execute('SELECT usd_to_eur_rate FROM exchange_rates WHERE date = ?', (date_str,))
            result = cursor.fetchone()
            
            if result:
                return result[0]
            
            # If no exact match, get the closest previous date
            cursor.execute('''
                SELECT usd_to_eur_rate FROM exchange_rates 
                WHERE date <= ? 
                ORDER BY date DESC 
                LIMIT 1
            ''', (date_str,))
            result = cursor.fetchone()
            
            if result:
                return result[0]
            else:
                return None
                
    except Exception as e:
        return None

def convert_usd_to_eur(usd_amount, date_str):
    """Convert USD amount to EUR using exchange rate for given date"""
    if not usd_amount:
        return None
        
    rate = get_exchange_rate(date_str)
    if rate:
        # The rate is 1 EUR = X USD, so to convert USD to EUR: USD / rate
        return round(usd_amount / rate, 2)
    else:
        return None

def clean_numeric_value(value):
    """Clean and convert string values to float, handling commas and special cases"""
    if not value or value.strip() == '' or value.strip().lower() == 'none' or value.strip() == '--':
        return None
    
    # Clean the value
    try:
        cleaned = str(value).strip()
        
        # Remove surrounding quotes if present
        if (cleaned.startswith('"') and cleaned.endswith('"')) or (cleaned.startswith("'") and cleaned.endswith("'")):
            cleaned = cleaned[1:-1]
        
        # Handle accounting format with parentheses for negative numbers
        is_negative = False
        if cleaned.startswith('(') and cleaned.endswith(')'):
            is_negative = True
            cleaned = cleaned[1:-1]  # Remove parentheses
        
        # Remove currency symbols, commas, and extra spaces
        cleaned = cleaned.replace('$', '').replace(',', '').replace(' ', '')
        
        # Convert to float
        result = float(cleaned)
        
        # Apply negative sign if needed
        if is_negative:
            result = -result
            
        return result
    except (ValueError, AttributeError):
        return None

@app.route('/import', methods=['GET', 'POST'])
@handle_database_error
def import_transactions():
    """Import transactions from CSV file with enhanced validation"""
    if request.method == 'GET':
        return render_template('import_transactions.html')
    
    try:
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        
        # Validate file upload
        file_errors = validate_file_upload(file)
        if file_errors:
            for error in file_errors:
                flash(error, 'error')
            return redirect(request.url)
        
        # Check if user wants to skip errors
        skip_errors = request.form.get('skipErrors') == 'on'
        
        # Read CSV file
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_input = csv.DictReader(stream)
        
        with db_connection() as conn:
            cursor = conn.cursor()
            
            imported_count = 0
            errors = []
            skipped_count = 0
            error_count = 0
            
            # First pass: collect all unique tickers for batch categorization
            unique_tickers = set()
            all_rows = []
            
            # Read all rows and collect unique tickers
            for row_num, row in enumerate(csv_input, start=2):
                # Store row with row number for later processing
                all_rows.append((row_num, row))
                
                # Extract and collect unique symbols
                symbol = row.get('Symbol', '').strip().upper() if row.get('Symbol') else None
                if symbol:
                    unique_tickers.add(symbol)
            
            # Batch categorize all unique tickers once
            ticker_categories = {}
            for ticker in unique_tickers:
                try:
                    category = categorize_asset(symbol=ticker)
                    ticker_categories[ticker] = category
                    logger.info(f"Categorized {ticker} as {category}")
                except Exception as e:
                    logger.warning(f"Failed to categorize {ticker}: {e}")
                    ticker_categories[ticker] = 'Stock'  # Default fallback
            
            logger.info(f"Batch categorized {len(ticker_categories)} unique tickers for import")
            
            # Second pass: process all transactions using cached categories
            for row_num, row in all_rows:
                try:
                    # Validate and clean data with column name mapping
                    cleaned_row = {}
                    for key, value in row.items():
                        # Map CSV column names (with spaces) to database column names (with underscores)
                        db_key = key.replace(' ', '_').replace('#', '')
                        
                        if value.strip() == '' or value.strip().lower() == 'none' or value.strip() == '--':
                            cleaned_row[db_key] = None
                        else:
                            cleaned_row[db_key] = sanitize_input(value.strip())
                    
                    # Validate row data using existing validation function
                    row_errors = validate_transaction_data(cleaned_row)
                    if row_errors:
                        error_count += 1
                        errors.extend([f'Row {row_num}: {error}' for error in row_errors])
                        
                        if not skip_errors:
                            # If not skipping errors, stop import and show all errors
                            break
                        else:
                            # Skip this row and continue
                            continue
                    
                    # Check for duplicate transaction using comprehensive field matching
                    # A transaction is considered duplicate if all key fields match exactly
                    # Using COALESCE to handle NULL values properly in comparisons
                    cursor.execute('''
                        SELECT COUNT(*) FROM transactions 
                        WHERE COALESCE(Date, '') = COALESCE(?, '') 
                        AND COALESCE(Type, '') = COALESCE(?, '') 
                        AND COALESCE(Sub_Type, '') = COALESCE(?, '') 
                        AND COALESCE(Symbol, '') = COALESCE(?, '') 
                        AND COALESCE(Instrument_Type, '') = COALESCE(?, '') 
                        AND COALESCE(Action, '') = COALESCE(?, '') 
                        AND COALESCE(Quantity, 0) = COALESCE(?, 0) 
                        AND COALESCE(Value, 0) = COALESCE(?, 0) 
                        AND COALESCE(Average_Price, 0) = COALESCE(?, 0) 
                        AND COALESCE(Total, 0) = COALESCE(?, 0) 
                        AND COALESCE(Commissions, 0) = COALESCE(?, 0) 
                        AND COALESCE(Fees, 0) = COALESCE(?, 0) 
                        AND COALESCE(Currency, '') = COALESCE(?, '') 
                        AND COALESCE(Root_Symbol, '') = COALESCE(?, '') 
                        AND COALESCE(Underlying_Symbol, '') = COALESCE(?, '') 
                        AND COALESCE(Expiration_Date, '') = COALESCE(?, '') 
                        AND COALESCE(Strike_Price, 0) = COALESCE(?, 0) 
                        AND COALESCE(Call_or_Put, '') = COALESCE(?, '') 
                        AND COALESCE(Description, '') = COALESCE(?, '')
                    ''', (
                        cleaned_row.get('Date'),
                        cleaned_row.get('Type'),
                        cleaned_row.get('Sub_Type'),
                        cleaned_row.get('Symbol'),
                        cleaned_row.get('Instrument_Type'),
                        cleaned_row.get('Action'),
                        clean_numeric_value(cleaned_row.get('Quantity')),
                        clean_numeric_value(cleaned_row.get('Value')),
                        clean_numeric_value(cleaned_row.get('Average_Price')),
                        clean_numeric_value(cleaned_row.get('Total')),
                        clean_numeric_value(cleaned_row.get('Commissions')),
                        clean_numeric_value(cleaned_row.get('Fees')),
                        cleaned_row.get('Currency'),
                        cleaned_row.get('Root_Symbol'),
                        cleaned_row.get('Underlying_Symbol'),
                        cleaned_row.get('Expiration_Date'),
                        clean_numeric_value(cleaned_row.get('Strike_Price')),
                        cleaned_row.get('Call_or_Put'),
                        cleaned_row.get('Description')
                    ))
                    
                    if cursor.fetchone()[0] > 0:
                        skipped_count += 1
                        continue  # Skip duplicate transaction
                    
                    # Get asset category from pre-categorized cache
                    asset_category = None
                    if cleaned_row.get('Symbol'):
                        symbol = cleaned_row.get('Symbol').strip().upper()
                        # Use cached category or fallback to individual categorization for complex cases
                        if (cleaned_row.get('Call_or_Put') or 
                            clean_numeric_value(cleaned_row.get('Strike_Price')) or 
                            cleaned_row.get('Expiration_Date') or
                            (cleaned_row.get('Instrument_Type') and 'option' in cleaned_row.get('Instrument_Type').lower())):
                            # For options and complex instruments, use individual categorization
                            asset_category = categorize_asset(
                                symbol=symbol,
                                instrument_type=cleaned_row.get('Instrument_Type'),
                                call_or_put=cleaned_row.get('Call_or_Put'),
                                strike_price=clean_numeric_value(cleaned_row.get('Strike_Price')),
                                expiration_date=cleaned_row.get('Expiration_Date')
                            )
                        else:
                            # For simple stocks/ETFs, use cached category
                            asset_category = ticker_categories.get(symbol, 'Stock')
                    
                    # Insert transaction
                    cursor.execute('''
                        INSERT INTO transactions (
                            Date, Type, Sub_Type, Symbol, Instrument_Type, Action,
                            Quantity, Value, Average_Price, Total, Commissions, Fees,
                            Currency, Root_Symbol, Underlying_Symbol, Expiration_Date,
                            Strike_Price, Call_or_Put, Description, Asset_Category
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        cleaned_row.get('Date'),
                        cleaned_row.get('Type'),
                        cleaned_row.get('Sub_Type'),
                        cleaned_row.get('Symbol'),
                        cleaned_row.get('Instrument_Type'),
                        cleaned_row.get('Action'),
                        clean_numeric_value(cleaned_row.get('Quantity')),
                        clean_numeric_value(cleaned_row.get('Value')),
                        clean_numeric_value(cleaned_row.get('Average_Price')),
                        clean_numeric_value(cleaned_row.get('Total')),
                        clean_numeric_value(cleaned_row.get('Commissions')),
                        clean_numeric_value(cleaned_row.get('Fees')),
                        cleaned_row.get('Currency'),
                        cleaned_row.get('Root_Symbol'),
                        cleaned_row.get('Underlying_Symbol'),
                        cleaned_row.get('Expiration_Date'),
                        clean_numeric_value(cleaned_row.get('Strike_Price')),
                        cleaned_row.get('Call_or_Put'),
                        cleaned_row.get('Description'),
                        asset_category
                    ))
                    imported_count += 1
                    
                except Exception as e:
                    errors.append(f'Row {row_num}: {str(e)}')
            
            # Commit all changes after processing all rows
            conn.commit()
        
        if imported_count > 0:
            message = f'Successfully imported {imported_count} transactions!'
            if skipped_count > 0:
                message += f' Skipped {skipped_count} duplicate transactions.'
            if error_count > 0 and skip_errors:
                message += f' Skipped {error_count} rows with validation errors.'
            flash(message, 'success')
        elif skipped_count > 0:
            message = f'No new transactions imported. Skipped {skipped_count} duplicate transactions.'
            if error_count > 0 and skip_errors:
                message += f' Also skipped {error_count} rows with validation errors.'
            flash(message, 'info')
        elif error_count > 0 and not skip_errors:
            flash('Import stopped due to validation errors. Enable "Skip rows with errors" to continue importing valid rows.', 'error')
        
        if errors:
            if skip_errors:
                flash(f'Validation errors encountered: {"; ".join(errors[:5])}{"; ..." if len(errors) > 5 else ""}', 'warning')
            else:
                flash(f'Validation errors: {"; ".join(errors[:10])}{"; ..." if len(errors) > 10 else ""}', 'error')
            
            # If there are errors, render the template with complete error details
            return render_template('import_transactions.html', import_errors=errors, error_count=error_count, imported_count=imported_count, skip_errors=skip_errors)
        
        return redirect(url_for('index'))
        
    except Exception as e:
        flash(f'Error importing transactions: {str(e)}', 'error')
        return redirect(request.url)

@app.route('/update-currency')
def update_currency():
    """Update USD to EUR exchange rates from Bundesbank API"""
    try:
        # Bundesbank API URL for USD to EUR exchange rates
        api_url = 'https://api.statistiken.bundesbank.de/rest/download/BBEX3/D.USD.EUR.BB.AC.000?format=csv&lang=de'
        
        # Make request to API
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        # Save the CSV data to file
        filename = f'usd_eur_rates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        filepath = os.path.join(os.getcwd(), filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        # Also save as the latest rates file for easy access
        latest_filepath = os.path.join(os.getcwd(), 'latest_usd_eur_rates.csv')
        with open(latest_filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        # Import the exchange rates into the database
        success, message = parse_and_import_exchange_rates()
        
        if success:
            flash(f'Currency rates updated and imported successfully! {message}', 'success')
        else:
            flash(f'Currency rates downloaded but import failed: {message}', 'warning')
        
    except requests.exceptions.RequestException as e:
        flash(f'Error fetching currency data: {str(e)}', 'error')
    except Exception as e:
        flash(f'Error updating currency rates: {str(e)}', 'error')
    
    return redirect(url_for('index'))

@app.route('/update-stock-prices')
def update_stock_prices():
    """Update stock prices from all available sources (Yahoo, Alpha Vantage, Finnhub)"""
    try:
        # Get all unique symbols from transactions
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT Symbol 
                FROM transactions 
                WHERE Symbol IS NOT NULL 
                AND Symbol != '' 
                AND Type = 'Trade'
                ORDER BY Symbol
            """)
            symbols = [row[0] for row in cursor.fetchall()]
        
        if not symbols:
            flash('No stock symbols found in transactions to update.', 'info')
            return redirect(url_for('index'))
        
        updated_count = 0
        failed_count = 0
        results = []
        
        # Update prices for each symbol
        for symbol in symbols:
            try:
                # Use the existing get_current_stock_price function which tries all sources
                price = get_current_stock_price(symbol, 'all')
                
                if price and price != 'N/A':
                    results.append(f"{symbol}: ${price:.2f}")
                    updated_count += 1
                else:
                    results.append(f"{symbol}: Failed to fetch")
                    failed_count += 1
                    
            except Exception as e:
                results.append(f"{symbol}: Error - {str(e)}")
                failed_count += 1
        
        # Create summary message
        if updated_count > 0:
            message = f'Stock prices updated! Successfully fetched {updated_count} prices'
            if failed_count > 0:
                message += f', {failed_count} failed'
            flash(message, 'success')
        elif failed_count > 0:
            flash(f'Failed to fetch prices for all {failed_count} symbols. This may be due to API rate limits.', 'warning')
        else:
            flash('No stock prices were updated.', 'info')
        
        # Log detailed results
        logger.info(f"Stock price update completed: {updated_count} successful, {failed_count} failed")
        for result in results[:10]:  # Log first 10 results
            logger.info(f"Price update result: {result}")
            
    except Exception as e:
        logger.error(f"Error in update_stock_prices: {str(e)}")
        flash(f'Error updating stock prices: {str(e)}', 'error')

    return redirect(url_for('index'))





@app.route('/update-stock-prices/finnhub')
def update_stock_prices_finnhub():
    """Update stock prices from Finnhub only"""
    return _update_stock_prices_source('finnhub', 'Finnhub')

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    """Settings page for API keys and configuration"""
    if request.method == 'POST':
        finnhub_api_key = request.form.get('finnhub_api_key', '').strip()
        
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                
                # Create settings table if it doesn't exist
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS settings (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                ''')
                
                # Update or insert Finnhub API key
                if finnhub_api_key:
                    cursor.execute('''
                        INSERT OR REPLACE INTO settings (key, value) 
                        VALUES (?, ?)
                    ''', ('finnhub_api_key', finnhub_api_key))
                else:
                    # Remove the key if empty
                    cursor.execute('DELETE FROM settings WHERE key = ?', ('finnhub_api_key',))
                
                conn.commit()
                flash('Settings saved successfully!', 'success')
                
        except Exception as e:
            flash(f'Error saving settings: {str(e)}', 'error')
            logger.error(f"Error saving settings: {e}")
        
        return redirect(url_for('settings'))
    
    # GET request - load current settings
    finnhub_api_key = ''
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT value FROM settings WHERE key = ?', ('finnhub_api_key',))
            result = cursor.fetchone()
            if result:
                finnhub_api_key = result['value']  # Use column name instead of index
    except Exception as e:
        logger.error(f"Error loading settings: {e}")
        # Log more details for debugging
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return render_template('settings.html', finnhub_api_key=finnhub_api_key)

@app.route('/reset_all_data', methods=['POST'])
def reset_all_data():
    """Reset all application data - transactions, settings, and files"""
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            
            # Delete all transactions
            cursor.execute("DELETE FROM transactions")
            
            # Delete all settings
            cursor.execute("DELETE FROM settings")
            
            # Reset auto-increment counters
            cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('transactions', 'settings')")
            
            conn.commit()
        
        # Remove uploaded CSV files
        import glob
        csv_files = glob.glob('*.csv')
        for csv_file in csv_files:
            try:
                if os.path.exists(csv_file):
                    os.remove(csv_file)
            except Exception as e:
                logger.warning(f"Could not remove file {csv_file}: {e}")
        
        # Remove log file
        try:
            if os.path.exists('app.log'):
                os.remove('app.log')
        except Exception as e:
            logger.warning(f"Could not remove log file: {e}")
        
        # Remove database file and recreate empty one
        try:
            if os.path.exists(DATABASE):
                os.remove(DATABASE)
            # Recreate database with tables
            from create_database import create_database
            create_database()
        except Exception as e:
            logger.warning(f"Could not reset database file: {e}")
        
        flash('All data has been successfully reset! The application is now in its initial state.', 'success')
        logger.info("All application data has been reset")
        
    except Exception as e:
        flash(f'Error resetting data: {str(e)}', 'error')
        logger.error(f"Error resetting all data: {e}")
    
    return redirect(url_for('settings'))

def _update_stock_prices_source(source, source_name):
    """Helper function to update stock prices from a specific source"""
    try:
        # Get all unique symbols from transactions
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT Symbol 
                FROM transactions 
                WHERE Symbol IS NOT NULL 
                AND Symbol != '' 
                AND Type = 'Trade'
                ORDER BY Symbol
            """)
            symbols = [row[0] for row in cursor.fetchall()]
        
        if not symbols:
            flash('No stock symbols found in transactions to update.', 'info')
            return redirect(url_for('index'))
        
        updated_count = 0
        failed_count = 0
        results = []
        
        # Update prices for each symbol using the specified source
        for symbol in symbols:
            try:
                price = get_current_stock_price(symbol, source)
                
                if price and price != 'N/A':
                    results.append(f"{symbol}: ${price:.2f}")
                    updated_count += 1
                else:
                    results.append(f"{symbol}: Failed to fetch")
                    failed_count += 1
                    
            except Exception as e:
                results.append(f"{symbol}: Error - {str(e)}")
                failed_count += 1
        
        # Create summary message
        if updated_count > 0:
            message = f'Stock prices updated from {source_name}! Successfully fetched {updated_count} prices'
            if failed_count > 0:
                message += f', {failed_count} failed'
            flash(message, 'success')
        elif failed_count > 0:
            flash(f'Failed to fetch prices for all {failed_count} symbols from {source_name}. This may be due to API rate limits or missing API keys.', 'warning')
        else:
            flash(f'No stock prices were updated from {source_name}.', 'info')
        
        # Log detailed results
        logger.info(f"Stock price update from {source_name} completed: {updated_count} successful, {failed_count} failed")
        for result in results[:10]:  # Log first 10 results
            logger.info(f"Price update result ({source_name}): {result}")
            
    except Exception as e:
        logger.error(f"Error in update_stock_prices_{source}: {str(e)}")
        flash(f'Error updating stock prices from {source_name}: {str(e)}', 'error')

    return redirect(url_for('index'))


def init_db():
    """Initialize the database with the transactions table"""
    with db_connection() as conn:
        cursor = conn.cursor()
        
        # Create transactions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Date TEXT,
                Type TEXT,
                Sub_Type TEXT,
                Symbol TEXT,
                Instrument_Type TEXT,
                Action TEXT,
                Quantity REAL,
                Value REAL,
                Average_Price REAL,
                Total REAL,
                Commissions REAL,
                Fees REAL,
                Currency TEXT,
                Root_Symbol TEXT,
                Underlying_Symbol TEXT,
                Expiration_Date TEXT,
                Strike_Price REAL,
                Call_or_Put TEXT,
                Description TEXT,
                Asset_Category TEXT
            )
        ''')
        
        # Create exchange rates table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS exchange_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                usd_to_eur_rate REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create stock_prices table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                source TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, source)
            )
        ''')
        
        # Migration: Add Asset_Category column if it doesn't exist
        try:
            cursor.execute("PRAGMA table_info(transactions)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'Asset_Category' not in columns:
                cursor.execute("ALTER TABLE transactions ADD COLUMN Asset_Category TEXT")
                logger.info("Added Asset_Category column to transactions table")
                
                # Update existing transactions with asset categories
                update_count = update_asset_categories()
                logger.info(f"Categorized {update_count} existing transactions")
        except Exception as e:
            log_error(e, "Error during Asset_Category migration")

def _build_gains_query_conditions(symbol=None, year=None):
    """Build query conditions and parameters for gains calculation"""
    conditions = ["(Type = 'Trade' OR Type = 'Receive Deliver')"]
    params = []
    
    if symbol:
        conditions.append("Symbol = ?")
        params.append(symbol)
    
    if year and year != 'ytd':
        conditions.append("substr(Date, 1, 4) = ?")
        params.append(str(year))
    elif year == 'ytd':
        conditions.append("substr(Date, 1, 4) = ?")
        params.append(str(datetime.now().year))
    
    return conditions, params

def _group_transactions_by_symbol(transactions):
    """Group transactions by symbol into buys and sells"""
    symbol_transactions = {}
    
    for trans in transactions:
        # Convert sqlite3.Row to dict if needed
        trans_dict = dict(trans) if hasattr(trans, 'keys') else trans
        
        symbol_key = trans_dict['Symbol']
        if symbol_key not in symbol_transactions:
            symbol_transactions[symbol_key] = {'buys': [], 'sells': [], 'asset_category': trans_dict.get('Asset_Category', 'Unknown')}
        
        if trans_dict['Action'] in ['BUY_TO_OPEN', 'BUY_TO_CLOSE']:
            symbol_transactions[symbol_key]['buys'].append({
                'date': trans_dict['Date'],
                'quantity': abs(trans_dict['Quantity'] or 0),
                'price': abs(trans_dict['Average_Price'] or 0),
                'total_cost': abs(trans_dict['Total'] or 0),
                'fees': abs(trans_dict['Fees'] or 0) + abs(trans_dict['Commissions'] or 0),
                'asset_category': trans_dict.get('Asset_Category', 'Unknown')
            })
        elif trans_dict['Action'] in ['SELL_TO_CLOSE', 'SELL_TO_OPEN']:
            symbol_transactions[symbol_key]['sells'].append({
                'date': trans_dict['Date'],
                'quantity': abs(trans_dict['Quantity'] or 0),
                'price': abs(trans_dict['Average_Price'] or 0),
                'total_proceeds': abs(trans_dict['Total'] or 0),
                'fees': abs(trans_dict['Fees'] or 0) + abs(trans_dict['Commissions'] or 0),
                'asset_category': trans_dict.get('Asset_Category', 'Unknown')
            })
    
    return symbol_transactions

def _calculate_fifo_match(buy, sell, matched_qty, sell_qty):
    """Calculate gain/loss for a FIFO match between buy and sell"""
    cost_basis = (matched_qty / buy['quantity']) * (buy['total_cost'] + buy['fees']) if buy['quantity'] > 0 else 0
    proceeds = (matched_qty / sell_qty) * sell['total_proceeds'] if sell_qty > 0 else 0
    gain_loss = proceeds - cost_basis
    
    return {
        'cost_basis': cost_basis,
        'proceeds': proceeds,
        'gain_loss': gain_loss
    }

def _process_fifo_gains(symbol_transactions):
    """Process FIFO gains/losses for all symbols"""
    total_gains = 0
    total_losses = 0
    detailed_transactions = []
    
    for symbol_key, data in symbol_transactions.items():
        buys = data['buys'].copy()
        sells = data['sells']
        
        for sell in sells:
            remaining_sell_qty = sell['quantity']
            
            while remaining_sell_qty > 0 and buys:
                buy = buys[0]
                
                if buy['quantity'] <= remaining_sell_qty:
                    # Use entire buy position
                    matched_qty = buy['quantity']
                    match_result = _calculate_fifo_match(buy, sell, matched_qty, sell['quantity'])
                    
                    # Get exchange rates for EUR conversion
                    buy_rate = get_exchange_rate(buy['date'][:10]) or 1.0
                    sell_rate = get_exchange_rate(sell['date'][:10]) or 1.0
                    
                    detailed_transactions.append({
                        'symbol': symbol_key,
                        'buy_date': buy['date'],
                        'sell_date': sell['date'],
                        'quantity': matched_qty,
                        'buy_price': buy['price'],
                        'sell_price': sell['price'],
                        'cost_basis': match_result['cost_basis'],
                        'proceeds': match_result['proceeds'],
                        'gain_loss': match_result['gain_loss'],
                        'asset_category': buy.get('asset_category', 'Unknown'),
                        # EUR conversions (rate is 1 EUR = X USD, so divide USD by rate to get EUR)
                        'buy_price_eur': buy['price'] / buy_rate,
                        'sell_price_eur': sell['price'] / sell_rate,
                        'cost_basis_eur': match_result['cost_basis'] / buy_rate,
                        'proceeds_eur': match_result['proceeds'] / sell_rate,
                        'gain_loss_eur': match_result['proceeds'] / sell_rate - match_result['cost_basis'] / buy_rate
                    })
                    
                    if match_result['gain_loss'] > 0:
                        total_gains += match_result['gain_loss']
                    else:
                        total_losses += abs(match_result['gain_loss'])
                    
                    remaining_sell_qty -= matched_qty
                    buys.pop(0)
                else:
                    # Partial buy position
                    matched_qty = remaining_sell_qty
                    match_result = _calculate_fifo_match(buy, sell, matched_qty, sell['quantity'])
                    
                    # Get exchange rates for EUR conversion
                    buy_rate = get_exchange_rate(buy['date'][:10]) or 1.0
                    sell_rate = get_exchange_rate(sell['date'][:10]) or 1.0
                    
                    detailed_transactions.append({
                        'symbol': symbol_key,
                        'buy_date': buy['date'],
                        'sell_date': sell['date'],
                        'quantity': matched_qty,
                        'buy_price': buy['price'],
                        'sell_price': sell['price'],
                        'cost_basis': match_result['cost_basis'],
                        'proceeds': match_result['proceeds'],
                        'gain_loss': match_result['gain_loss'],
                        'asset_category': buy.get('asset_category', 'Unknown'),
                        # EUR conversions (rate is 1 EUR = X USD, so divide USD by rate to get EUR)
                        'buy_price_eur': buy['price'] / buy_rate,
                        'sell_price_eur': sell['price'] / sell_rate,
                        'cost_basis_eur': match_result['cost_basis'] / buy_rate,
                        'proceeds_eur': match_result['proceeds'] / sell_rate,
                        'gain_loss_eur': match_result['proceeds'] / sell_rate - match_result['cost_basis'] / buy_rate
                    })
                    
                    if match_result['gain_loss'] > 0:
                        total_gains += match_result['gain_loss']
                    else:
                        total_losses += abs(match_result['gain_loss'])
                    
                    # Update remaining buy quantity
                    buy['quantity'] -= matched_qty
                    if buy['quantity'] > 0:
                        ratio = buy['quantity'] / (buy['quantity'] + matched_qty)
                        buy['total_cost'] *= ratio
                        buy['fees'] *= ratio
                    
                    remaining_sell_qty = 0
    
    return total_gains, total_losses, detailed_transactions

def _process_fifo_gains_by_category(symbol_transactions):
    """Process FIFO gains/losses for all symbols with German tax category separation"""
    total_gains = 0
    total_losses = 0
    stock_gains = 0
    stock_losses = 0
    option_gains = 0
    option_losses = 0
    other_gains = 0
    other_losses = 0
    detailed_transactions = []
    
    for symbol_key, data in symbol_transactions.items():
        buys = data['buys'].copy()
        sells = data['sells']
        
        for sell in sells:
            remaining_sell_qty = sell['quantity']
            
            while remaining_sell_qty > 0 and buys:
                buy = buys[0]
                
                if buy['quantity'] <= remaining_sell_qty:
                    # Use entire buy position
                    matched_qty = buy['quantity']
                    match_result = _calculate_fifo_match(buy, sell, matched_qty, sell['quantity'])
                    
                    asset_category = buy.get('asset_category', 'Unknown')
                    
                    # Get exchange rates for EUR conversion
                    buy_rate = get_exchange_rate(buy['date'][:10]) or 1.0
                    sell_rate = get_exchange_rate(sell['date'][:10]) or 1.0
                    
                    detailed_transactions.append({
                        'symbol': symbol_key,
                        'buy_date': buy['date'],
                        'sell_date': sell['date'],
                        'quantity': matched_qty,
                        'buy_price': buy['price'],
                        'sell_price': sell['price'],
                        'cost_basis': match_result['cost_basis'],
                        'proceeds': match_result['proceeds'],
                        'gain_loss': match_result['gain_loss'],
                        'asset_category': asset_category,
                        # EUR conversions (rate is 1 EUR = X USD, so divide USD by rate to get EUR)
                        'buy_price_eur': buy['price'] / buy_rate,
                        'sell_price_eur': sell['price'] / sell_rate,
                        'cost_basis_eur': match_result['cost_basis'] / buy_rate,
                        'proceeds_eur': match_result['proceeds'] / sell_rate,
                        'gain_loss_eur': match_result['proceeds'] / sell_rate - match_result['cost_basis'] / buy_rate
                    })
                    
                    # Categorize gains/losses for German tax
                    if match_result['gain_loss'] > 0:
                        total_gains += match_result['gain_loss']
                        if asset_category == 'Stock':
                            stock_gains += match_result['gain_loss']
                        elif asset_category == 'Option':
                            option_gains += match_result['gain_loss']
                        else:
                            other_gains += match_result['gain_loss']
                    else:
                        loss_amount = abs(match_result['gain_loss'])
                        total_losses += loss_amount
                        if asset_category == 'Stock':
                            stock_losses += loss_amount
                        elif asset_category == 'Option':
                            option_losses += loss_amount
                        else:
                            other_losses += loss_amount
                    
                    remaining_sell_qty -= matched_qty
                    buys.pop(0)
                else:
                    # Partial buy position
                    matched_qty = remaining_sell_qty
                    match_result = _calculate_fifo_match(buy, sell, matched_qty, sell['quantity'])
                    
                    asset_category = buy.get('asset_category', 'Unknown')
                    
                    # Get exchange rates for EUR conversion
                    buy_rate = get_exchange_rate(buy['date'][:10]) or 1.0
                    sell_rate = get_exchange_rate(sell['date'][:10]) or 1.0
                    
                    detailed_transactions.append({
                        'symbol': symbol_key,
                        'buy_date': buy['date'],
                        'sell_date': sell['date'],
                        'quantity': matched_qty,
                        'buy_price': buy['price'],
                        'sell_price': sell['price'],
                        'cost_basis': match_result['cost_basis'],
                        'proceeds': match_result['proceeds'],
                        'gain_loss': match_result['gain_loss'],
                        'asset_category': asset_category,
                        # EUR conversions (rate is 1 EUR = X USD, so divide USD by rate to get EUR)
                        'buy_price_eur': buy['price'] / buy_rate,
                        'sell_price_eur': sell['price'] / sell_rate,
                        'cost_basis_eur': match_result['cost_basis'] / buy_rate,
                        'proceeds_eur': match_result['proceeds'] / sell_rate,
                        'gain_loss_eur': match_result['proceeds'] / sell_rate - match_result['cost_basis'] / buy_rate
                    })
                    
                    # Categorize gains/losses for German tax
                    if match_result['gain_loss'] > 0:
                        total_gains += match_result['gain_loss']
                        if asset_category == 'Stock':
                            stock_gains += match_result['gain_loss']
                        elif asset_category == 'Option':
                            option_gains += match_result['gain_loss']
                        else:
                            other_gains += match_result['gain_loss']
                    else:
                        loss_amount = abs(match_result['gain_loss'])
                        total_losses += loss_amount
                        if asset_category == 'Stock':
                            stock_losses += loss_amount
                        elif asset_category == 'Option':
                            option_losses += loss_amount
                        else:
                            other_losses += loss_amount
                    
                    # Update remaining buy quantity
                    buy['quantity'] -= matched_qty
                    if buy['quantity'] > 0:
                        ratio = buy['quantity'] / (buy['quantity'] + matched_qty)
                        buy['total_cost'] *= ratio
                        buy['fees'] *= ratio
                    
                    remaining_sell_qty = 0
    
    return {
        'total_gains': total_gains,
        'total_losses': total_losses,
        'stock_gains': stock_gains,
        'stock_losses': stock_losses,
        'option_gains': option_gains,
        'option_losses': option_losses,
        'other_gains': other_gains,
        'other_losses': other_losses,
        'detailed_transactions': detailed_transactions,
        # Add EUR totals calculated from individual transactions
        'stock_gains_eur': sum(tx['gain_loss_eur'] for tx in detailed_transactions if tx['asset_category'] == 'Stock' and tx['gain_loss_eur'] > 0),
        'stock_losses_eur': sum(abs(tx['gain_loss_eur']) for tx in detailed_transactions if tx['asset_category'] == 'Stock' and tx['gain_loss_eur'] < 0),
        'option_gains_eur': sum(tx['gain_loss_eur'] for tx in detailed_transactions if tx['asset_category'] == 'Option' and tx['gain_loss_eur'] > 0),
        'option_losses_eur': sum(abs(tx['gain_loss_eur']) for tx in detailed_transactions if tx['asset_category'] == 'Option' and tx['gain_loss_eur'] < 0),
        'other_gains_eur': sum(tx['gain_loss_eur'] for tx in detailed_transactions if tx['asset_category'] not in ['Stock', 'Option'] and tx['gain_loss_eur'] > 0),
        'other_losses_eur': sum(abs(tx['gain_loss_eur']) for tx in detailed_transactions if tx['asset_category'] not in ['Stock', 'Option'] and tx['gain_loss_eur'] < 0)
    }

def calculate_realized_gains_losses(symbol=None, year=None):
    """Calculate realized capital gains/losses using FIFO method (only completed buy/sell pairs)
    For German tax calculation, separates stock earnings from other asset earnings.
    """
    with db_connection() as conn:
        cursor = conn.cursor()
        
        # Build query conditions
        conditions, params = _build_gains_query_conditions(symbol, year)
        where_clause = " AND ".join(conditions)
        
        # Get all buy and sell transactions ordered by date (FIFO)
        query = f"""
            SELECT Date, Symbol, Action, Quantity, Average_Price, Total, Fees, Commissions, Asset_Category
            FROM transactions 
            WHERE {where_clause}
            ORDER BY Date ASC
        """
        
        cursor.execute(query, params)
        transactions = cursor.fetchall()
    
    # Group transactions by symbol
    symbol_transactions = _group_transactions_by_symbol(transactions)
    
    # Calculate FIFO gains/losses with asset category separation
    gains_losses_result = _process_fifo_gains_by_category(symbol_transactions)
    
    return {
        'total_gains': gains_losses_result['total_gains'],
        'total_losses': gains_losses_result['total_losses'],
        'net_gain_loss': gains_losses_result['total_gains'] - gains_losses_result['total_losses'],
        'detailed_transactions': gains_losses_result['detailed_transactions'],
        # German tax separation
        'stock_gains': gains_losses_result['stock_gains'],
        'stock_losses': gains_losses_result['stock_losses'],
        'stock_net_gain_loss': gains_losses_result['stock_gains'] - gains_losses_result['stock_losses'],
        'option_gains': gains_losses_result['option_gains'],
        'option_losses': gains_losses_result['option_losses'],
        'option_net_gain_loss': gains_losses_result['option_gains'] - gains_losses_result['option_losses'],
        'other_gains': gains_losses_result['other_gains'],
        'other_losses': gains_losses_result['other_losses'],
        'other_net_gain_loss': gains_losses_result['other_gains'] - gains_losses_result['other_losses'],
        # EUR values from historical exchange rates
        'stock_gains_eur': gains_losses_result['stock_gains_eur'],
        'stock_losses_eur': gains_losses_result['stock_losses_eur'],
        'option_gains_eur': gains_losses_result['option_gains_eur'],
        'option_losses_eur': gains_losses_result['option_losses_eur'],
        'other_gains_eur': gains_losses_result['other_gains_eur'],
        'other_losses_eur': gains_losses_result['other_losses_eur']
    }

# Simple in-memory cache for stock prices (valid for 5 minutes)
_price_cache = {}
_cache_timeout = 300  # 5 minutes

def get_current_stock_price(symbol, source='all'):
    """
    Fetch current stock price using specified data source:
    - 'all': Try all sources with fallbacks (default)
    - 'finnhub': Finnhub only
    """
    import time
    import requests
    
    cache_key = symbol.strip().upper()
    current_time = time.time()
    
    # Check cache first (unless source is specified and not 'all')
    if source == 'all' and cache_key in _price_cache:
        cached_data = _price_cache[cache_key]
        if current_time - cached_data['timestamp'] < _cache_timeout:
            print(f"Using cached price for {symbol}: ${cached_data['price']}")
            return cached_data['price']
    
    clean_symbol = symbol.strip().upper()
    
    # Route to specific source functions
    if source == 'finnhub':
        return _fetch_price_finnhub(clean_symbol, current_time, cache_key)
    else:  # source == 'all'
        # Try Finnhub
        price = _fetch_price_finnhub(clean_symbol, current_time, cache_key)
        if price != 'N/A':
            return price
    
    # If all sources fail, check for expired cache as last resort
    if cache_key in _price_cache:
        cached_price = _price_cache[cache_key]['price']
        print(f"Using expired cached price for {symbol}: ${cached_price}")
        return cached_price
    
    # If cache is also unavailable, try to get last downloaded price from database
    last_downloaded_price = _get_last_downloaded_price(symbol)
    if last_downloaded_price is not None:
        return last_downloaded_price
    
    print(f"Could not fetch price for {symbol} from any source")
    return 'N/A'

def _save_stock_price_to_db(symbol, price, source):
    """Save stock price to database for persistent storage"""
    try:
        from datetime import datetime
        with db_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().isoformat()
            cursor.execute('''
                INSERT OR REPLACE INTO stock_prices 
                (symbol, price, timestamp, source)
                VALUES (?, ?, ?, ?)
            ''', (symbol.upper(), price, current_time, source))
            conn.commit()
            print(f"Saved {symbol} price ${price} to database from {source}")
    except Exception as e:
        print(f"Error saving price to database for {symbol}: {e}")

def _get_last_downloaded_price(symbol):
    """Get the last downloaded price from database for use as placeholder"""
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT price, timestamp, source FROM stock_prices 
                WHERE symbol = ? 
                ORDER BY timestamp DESC 
                LIMIT 1
            ''', (symbol.upper(),))
            result = cursor.fetchone()
            if result:
                price, timestamp, source = result
                print(f"Using last downloaded price for {symbol}: ${price} (from {source} at {timestamp})")
                return float(price)
    except Exception as e:
        print(f"Error retrieving last downloaded price for {symbol}: {e}")
    return None


def _fetch_price_finnhub(symbol, current_time, cache_key):
    """Fetch price from Finnhub using stored API key"""
    try:
        # Get API key from database settings
        api_key = None
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT value FROM settings WHERE key = ?', ('finnhub_api_key',))
                result = cursor.fetchone()
                if result:
                    api_key = result[0]
        except Exception as db_error:
            print(f"Error retrieving Finnhub API key from database: {db_error}")
        
        if api_key:
            url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={api_key}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if 'c' in data and data['c'] is not None:
                price = float(data['c'])
                print(f"Successfully fetched price for {symbol} from Finnhub: ${price}")
                
                _price_cache[cache_key] = {
                    'price': price,
                    'timestamp': current_time
                }
                
                # Save to database for persistent storage
                _save_stock_price_to_db(symbol, price, 'finnhub')
                
                return price
        else:
            print(f"Finnhub API key not configured for {symbol}. Please add it in Settings.")
    except Exception as e:
        print(f"Finnhub failed for {symbol}: {e}")
    
    return 'N/A'

def get_net_dividends_by_symbol():
    """Get net dividends (dividends - source tax) for each symbol, only for shares still held"""
    with db_connection() as conn:
        cursor = conn.cursor()
    
        # Get all symbols that have trades
        cursor.execute("""
            SELECT DISTINCT Symbol FROM transactions WHERE Type = 'Trade'
        """)
        symbols = [row['Symbol'] for row in cursor.fetchall()]
    
        dividend_dict = {}
    
        for symbol in symbols:
            # Get all transactions for this symbol in chronological order
            cursor.execute("""
                SELECT Date, Type, Action, Quantity, Average_Price, Total, Value, Sub_Type, Fees, Commissions
                FROM transactions 
                WHERE Symbol = ?
                ORDER BY Date ASC
            """, (symbol,))
        
            transactions = cursor.fetchall()
        
            # Track share lots and dividends using FIFO
            share_lots = []  # Each lot: {'date', 'quantity', 'dividends_earned'}
            total_gross_dividends = 0
            total_source_tax = 0
        
            for trans in transactions:
                date, trans_type, action, qty, price, total, value, sub_type, fees, commissions = trans
            
                if trans_type == 'Trade':
                    if action == 'BUY_TO_OPEN':
                        # Add new share lot
                        shares_bought = abs(qty or 0)
                        share_lots.append({
                            'date': date,
                            'quantity': shares_bought,
                            'gross_dividends': 0,
                            'source_tax': 0
                        })
                    
                    elif action == 'SELL_TO_CLOSE':
                        # Remove sold shares using FIFO
                        shares_sold = abs(qty or 0)
                        remaining_to_sell = shares_sold
                    
                        while remaining_to_sell > 0 and share_lots:
                            lot = share_lots[0]
                            if lot['quantity'] <= remaining_to_sell:
                                # Entire lot is sold
                                remaining_to_sell -= lot['quantity']
                                share_lots.pop(0)
                            else:
                                # Partial lot sold
                                sold_ratio = remaining_to_sell / lot['quantity']
                                lot['quantity'] -= remaining_to_sell
                                lot['gross_dividends'] *= (1 - sold_ratio)
                                lot['source_tax'] *= (1 - sold_ratio)
                                remaining_to_sell = 0
            
                elif trans_type == 'Money Movement' and (sub_type == 'Dividend' or sub_type is None):
                    # Distribute dividend to current share lots proportionally
                    dividend_value = value or 0
                    total_shares = sum(lot['quantity'] for lot in share_lots)
                
                    if total_shares > 0:
                        for lot in share_lots:
                            lot_ratio = lot['quantity'] / total_shares
                            lot_dividend = dividend_value * lot_ratio
                        
                            if dividend_value > 0:
                                lot['gross_dividends'] += lot_dividend
                            else:
                                lot['source_tax'] += abs(lot_dividend)
        
            # Sum up dividends from remaining (unsold) lots
            remaining_gross_dividends = sum(lot['gross_dividends'] for lot in share_lots)
            remaining_source_tax = sum(lot['source_tax'] for lot in share_lots)
            remaining_net_dividends = remaining_gross_dividends - remaining_source_tax
        
            if remaining_gross_dividends > 0 or remaining_source_tax > 0:
                dividend_dict[symbol] = {
                    'gross_dividends': remaining_gross_dividends,
                    'source_tax': remaining_source_tax,
                    'net_dividends': remaining_net_dividends
                }
    return dividend_dict

def calculate_unrealized_gains_losses(symbol=None, fetch_current_prices=False):
    """Calculate unrealized gains/losses for current holdings (unsold positions)
    
    Args:
        symbol: Optional symbol to filter by
        fetch_current_prices: If True, fetch current market prices. If False, use cached or fallback prices only.
    """
    with db_connection() as conn:
        cursor = conn.cursor()
    
        # Build query conditions
        conditions = ["Type = 'Trade'"]
        params = []
    
        if symbol:
            conditions.append("Symbol = ?")
            params.append(symbol)
    
        where_clause = " AND ".join(conditions)
    
        # Get all buy and sell transactions ordered by date (FIFO)
        query = f"""
            SELECT Date, Symbol, Action, Quantity, Average_Price, Total, Fees, Commissions, Asset_Category
            FROM transactions 
            WHERE {where_clause}
            ORDER BY Date ASC
        """
    
        cursor.execute(query, params)
        transactions = cursor.fetchall()
    
    # Get dividend data for all symbols
    dividend_data = get_net_dividends_by_symbol()
    
    # Group transactions by symbol and calculate remaining holdings
    symbol_holdings = {}
    for trans in transactions:
        # Convert sqlite3.Row to dict if needed
        trans_dict = dict(trans) if hasattr(trans, 'keys') else trans
        
        symbol_key = trans_dict['Symbol']
        if symbol_key not in symbol_holdings:
            symbol_holdings[symbol_key] = {'buys': [], 'total_sold': 0, 'asset_category': trans_dict.get('Asset_Category', 'Unknown')}
        
        if trans_dict['Action'] == 'BUY_TO_OPEN':
            symbol_holdings[symbol_key]['buys'].append({
                'date': trans_dict['Date'],
                'quantity': abs(trans_dict['Quantity'] or 0),
                'price': abs(trans_dict['Average_Price'] or 0),
                'total_cost': abs(trans_dict['Total'] or 0),
                'fees': abs(trans_dict['Fees'] or 0) + abs(trans_dict['Commissions'] or 0),
                'asset_category': trans_dict.get('Asset_Category', 'Unknown')
            })
        elif trans_dict['Action'] == 'SELL_TO_CLOSE':
            symbol_holdings[symbol_key]['total_sold'] += abs(trans_dict['Quantity'] or 0)
    
    # Calculate unrealized gains/losses for remaining holdings
    unrealized_positions = []
    total_unrealized_value = 0
    total_cost_basis = 0
    
    for symbol_key, data in symbol_holdings.items():
        buys = data['buys'].copy()
        total_sold = data['total_sold']
        
        # Remove sold quantities using FIFO
        remaining_to_sell = total_sold
        while remaining_to_sell > 0 and buys:
            buy = buys[0]
            if buy['quantity'] <= remaining_to_sell:
                remaining_to_sell -= buy['quantity']
                buys.pop(0)
            else:
                # Partial position sold
                buy['quantity'] -= remaining_to_sell
                buy['total_cost'] = (buy['quantity'] / (buy['quantity'] + remaining_to_sell)) * buy['total_cost']
                buy['fees'] = (buy['quantity'] / (buy['quantity'] + remaining_to_sell)) * buy['fees']
                remaining_to_sell = 0
        
        # Aggregate remaining positions by symbol
        if buys:  # Only process if there are remaining positions
            total_quantity = sum(buy['quantity'] for buy in buys)
            total_symbol_cost_basis = sum(buy['total_cost'] + buy['fees'] for buy in buys)
            
            if total_quantity > 0:
                # Calculate weighted average cost
                weighted_avg_cost = total_symbol_cost_basis / total_quantity
                
                # Get current market price based on fetch_current_prices flag
                if fetch_current_prices:
                    current_price = get_current_stock_price(symbol_key)
                    # Fallback to weighted average cost if market price unavailable
                    if current_price is None or current_price == 'N/A':
                        current_price = weighted_avg_cost
                        print(f"Using fallback price for {symbol_key}: ${current_price}")
                else:
                    # Use cached price if available, otherwise try database, then weighted average cost
                    cache_key = symbol_key.strip().upper()
                    if cache_key in _price_cache:
                        import time
                        current_time = time.time()
                        cached_data = _price_cache[cache_key]
                        if current_time - cached_data['timestamp'] < _cache_timeout:
                            current_price = cached_data['price']
                            print(f"Using cached price for {symbol_key}: ${current_price}")
                        else:
                            # Try to get last downloaded price from database
                            last_downloaded_price = _get_last_downloaded_price(symbol_key)
                            if last_downloaded_price is not None:
                                current_price = last_downloaded_price
                            else:
                                current_price = weighted_avg_cost
                                print(f"Using fallback price for {symbol_key}: ${current_price}")
                    else:
                        # Try to get last downloaded price from database
                        last_downloaded_price = _get_last_downloaded_price(symbol_key)
                        if last_downloaded_price is not None:
                            current_price = last_downloaded_price
                        else:
                            current_price = weighted_avg_cost
                            print(f"Using fallback price for {symbol_key}: ${current_price}")
                
                current_value = total_quantity * current_price
                unrealized_gain_loss = current_value - total_symbol_cost_basis
                
                # Get the earliest buy date for this symbol
                earliest_date = min(buy['date'] for buy in buys)
                
                # Get net dividends for this symbol
                symbol_dividends = dividend_data.get(symbol_key, {'net_dividends': 0})
                net_dividends = symbol_dividends['net_dividends']
                
                # Calculate adjusted average cost (avg cost - dividends per share)
                dividends_per_share = net_dividends / total_quantity if total_quantity > 0 else 0
                adjusted_avg_cost = weighted_avg_cost - dividends_per_share
                
                unrealized_positions.append({
                    'symbol': symbol_key,
                    'quantity': total_quantity,
                    'avg_cost': weighted_avg_cost,
                    'current_price': current_price,
                    'cost_basis': total_symbol_cost_basis,
                    'current_value': current_value,
                    'unrealized_gain_loss': unrealized_gain_loss,
                    'buy_date': earliest_date,
                    'net_dividends': net_dividends,
                    'adjusted_avg_cost': adjusted_avg_cost,
                    'asset_category': data.get('asset_category', 'Unknown')
                })
                
                total_cost_basis += total_symbol_cost_basis
                total_unrealized_value += current_value
    
    total_unrealized_gain_loss = total_unrealized_value - total_cost_basis
    
    return {
        'total_unrealized_gain_loss': total_unrealized_gain_loss,
        'total_cost_basis': total_cost_basis,
        'total_current_value': total_unrealized_value,
        'positions': unrealized_positions
    }

def get_dividend_data(year=None):
    """Get dividend data for tax calculations"""
    with db_connection() as conn:
        cursor = conn.cursor()
    
        base_conditions = ["Type = 'Money Movement'", "Sub_Type = 'Dividend'"]
        params = []
    
        if year and year != 'ytd':
            base_conditions.append("substr(Date, 1, 4) = ?")
            params.append(str(year))
        elif year == 'ytd':
            base_conditions.append("substr(Date, 1, 4) = ?")
            params.append(str(datetime.now().year))
    
        # Get positive dividends (gross dividend income)
        positive_conditions = base_conditions + ["Value > 0"]
        positive_where_clause = " AND ".join(positive_conditions)
    
        positive_query = f"""
            SELECT Symbol, SUM(Value) as total_dividends, COUNT(*) as payment_count
            FROM transactions 
            WHERE {positive_where_clause}
            GROUP BY Symbol
            ORDER BY total_dividends DESC
        """
    
        cursor.execute(positive_query, params)
        dividend_data = cursor.fetchall()
    
        # Get total positive dividends
        total_positive_query = f"""
            SELECT SUM(Value) as total_dividends
            FROM transactions 
            WHERE {positive_where_clause}
        """
    
        cursor.execute(total_positive_query, params)
        total_positive_result = cursor.fetchone()
        total_dividends = total_positive_result['total_dividends'] or 0
        
        # Calculate EUR values for positive dividends using historical exchange rates
        positive_eur_query = f"""
            SELECT Date, Value
            FROM transactions 
            WHERE {positive_where_clause}
        """
        
        cursor.execute(positive_eur_query, params)
        positive_transactions = cursor.fetchall()
        
        total_dividends_eur = 0
        for transaction in positive_transactions:
            eur_rate = get_exchange_rate(transaction['Date'])
            if eur_rate:
                total_dividends_eur += transaction['Value'] / eur_rate
    
        # Get negative dividends (source tax withholdings)
        negative_conditions = base_conditions + ["Value < 0"]
        negative_where_clause = " AND ".join(negative_conditions)
    
        negative_query = f"""
            SELECT Symbol, SUM(ABS(Value)) as total_source_tax, COUNT(*) as withholding_count
            FROM transactions 
            WHERE {negative_where_clause}
            GROUP BY Symbol
            ORDER BY total_source_tax DESC
        """
    
        cursor.execute(negative_query, params)
        source_tax_data = cursor.fetchall()
    
        # Get total source tax
        total_negative_query = f"""
            SELECT SUM(ABS(Value)) as total_source_tax
            FROM transactions 
            WHERE {negative_where_clause}
        """
    
        cursor.execute(total_negative_query, params)
        total_negative_result = cursor.fetchone()
        total_source_tax = total_negative_result['total_source_tax'] or 0
        
        # Calculate EUR values for source tax using historical exchange rates
        negative_eur_query = f"""
            SELECT Date, Value
            FROM transactions 
            WHERE {negative_where_clause}
        """
        
        cursor.execute(negative_eur_query, params)
        negative_transactions = cursor.fetchall()
        
        total_source_tax_eur = 0
        for transaction in negative_transactions:
            eur_rate = get_exchange_rate(transaction['Date'])
            if eur_rate:
                total_source_tax_eur += abs(transaction['Value']) / eur_rate
    
    return {
        'total_dividends': total_dividends,
        'total_dividends_eur': total_dividends_eur,
        'total_source_tax': total_source_tax,
        'total_source_tax_eur': total_source_tax_eur,
        'dividend_by_symbol': dividend_data,
        'source_tax_by_symbol': source_tax_data
    }

def get_fees_data(year=None):
    """Get total fees for tax calculations"""
    with db_connection() as conn:
        cursor = conn.cursor()
    
        conditions = []
        params = []
    
        if year and year != 'ytd':
            conditions.append("substr(Date, 1, 4) = ?")
            params.append(str(year))
        elif year == 'ytd':
            conditions.append("substr(Date, 1, 4) = ?")
            params.append(str(datetime.now().year))
    
        where_clause = " AND ".join(conditions) if conditions else "1=1"
    
        query = f"""
            SELECT 
                SUM(CASE WHEN Fees IS NOT NULL THEN ABS(Fees) ELSE 0 END) as total_fees,
                SUM(CASE WHEN Commissions IS NOT NULL THEN ABS(Commissions) ELSE 0 END) as total_commissions
            FROM transactions 
            WHERE {where_clause}
        """
    
        cursor.execute(query, params)
        result = cursor.fetchone()
    
        total_fees = (result['total_fees'] or 0) + (result['total_commissions'] or 0)
        
        # Calculate EUR values for fees using historical exchange rates
        eur_query = f"""
            SELECT Date, Fees, Commissions
            FROM transactions 
            WHERE {where_clause} AND (Fees IS NOT NULL OR Commissions IS NOT NULL)
        """
        
        cursor.execute(eur_query, params)
        fee_transactions = cursor.fetchall()
        
        total_fees_eur = 0
        for transaction in fee_transactions:
            eur_rate = get_exchange_rate(transaction['Date'])
            if eur_rate:
                fees = abs(transaction['Fees'] or 0)
                commissions = abs(transaction['Commissions'] or 0)
                total_fees_eur += (fees + commissions) / eur_rate
    
        return {
            'total_fees': total_fees,
            'total_fees_eur': total_fees_eur
        }

def get_available_tax_years(max_years=10):
    """Get available years for tax reporting dropdown
    
    Args:
        max_years (int): Maximum number of years to include (default: 10)
        
    Returns:
        dict: Contains 'current_year', 'ytd_label', and 'available_years' list
    """
    with db_connection() as conn:
        cursor = conn.cursor()
        
        # Get current year
        current_year = datetime.now().year
        
        # Get all unique years from transaction data
        cursor.execute("""
            SELECT DISTINCT substr(Date, 1, 4) as year 
            FROM transactions 
            WHERE Date IS NOT NULL AND Date != ''
            ORDER BY year DESC
        """)
        
        transaction_years = [int(row['year']) for row in cursor.fetchall() if row['year'].isdigit()]
        
        # Create year range: current year back to max_years or earliest transaction year
        earliest_year = min(transaction_years) if transaction_years else current_year
        start_year = max(earliest_year, current_year - max_years + 1)
        
        # Generate available years (current year down to start_year)
        available_years = []
        for year in range(current_year, start_year - 1, -1):
            # Only include years that have transaction data or are the current year
            if year == current_year or year in transaction_years:
                available_years.append(year)
        
        return {
            'current_year': current_year,
            'ytd_label': f'Year to Date ({current_year})',
            'available_years': available_years
        }

# Duplicate function removed - using the one defined earlier in the file

def get_profile(ticker):
    """Fetch stock profile from Finnhub API"""
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT value FROM settings WHERE key = ?', ('finnhub_api_key',))
            result = cursor.fetchone()
            
            if not result or not result[0]:
                logger.warning(f"Finnhub API key not configured for profile lookup: {ticker}")
                return {}
            
            api_key = result[0]
    except Exception as db_error:
        logger.error(f"Error retrieving Finnhub API key from database: {db_error}")
        return {}
    
    url = f"https://finnhub.io/api/v1/stock/profile2"
    params = {"symbol": ticker, "token": api_key}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            profile_data = response.json()
            logger.info(f"Successfully fetched profile for {ticker} from Finnhub")
            return profile_data
        else:
            logger.warning(f"Finnhub profile API returned status {response.status_code} for {ticker}")
            return {}
    except requests.exceptions.RequestException as e:
        logger.error(f"Finnhub profile API failed for {ticker}: {e}")
        return {}

def categorize_ticker(ticker):
    """Categorize a ticker using Finnhub API profile data"""
    profile = get_profile(ticker)
    if not profile:
        return "ETF"  # Default to ETF when no profile data available
    
    exchange = profile.get("exchange", "").lower()
    ipo = profile.get("ipo", "")
    industry = profile.get("finnhubIndustry", "").lower()
    ticker_type = profile.get("type", "").lower()  # e.g., Common Stock, ETF
    
    # Classification logic
    if "etf" in ticker_type or "etf" in industry:
        return "ETF"
    elif "closed" in industry or "cef" in ticker_type:
        return "CEF"
    elif "reit" in industry or "real estate" in industry:
        return "REIT"
    elif "stock" in ticker_type or ticker_type in ["common stock", "adr", "preferred stock"]:
        return "Stock"
    else:
        return "ETF"  # Default to ETF for unknown types

def categorize_asset(symbol, instrument_type=None, call_or_put=None, strike_price=None, expiration_date=None):
    """Categorize an asset as Stock, ETF, or Option based on various criteria"""
    if not symbol:
        return 'Unknown'
    
    symbol = symbol.strip().upper()
    
    # Check for options first (most specific)
    if (call_or_put or strike_price or expiration_date or 
        (instrument_type and 'option' in instrument_type.lower()) or
        any(char in symbol for char in ['P', 'C']) and len(symbol) > 10):
        return 'Option'
    
    # Known ETF symbols (comprehensive list)
    known_etfs = {
        # Major Index ETFs
        'SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'VEA', 'VWO', 'VTEB', 'VXUS',
        # Bond ETFs
        'AGG', 'BND', 'TLT', 'IEF', 'SHY', 'LQD', 'HYG', 'JNK', 'VCIT', 'VCSH',
        # Commodity ETFs
        'GLD', 'SLV', 'IAU', 'PDBC', 'DBA', 'USO', 'UNG', 'CORN', 'WEAT',
        # Sector ETFs
        'XLF', 'XLE', 'XLK', 'XLV', 'XLI', 'XLP', 'XLU', 'XLB', 'XLY', 'XLRE',
        'VGT', 'VHT', 'VIS', 'VCR', 'VDC', 'VDE', 'VFH', 'VAW', 'VNQ', 'VOX',
        # ARK ETFs
        'ARKK', 'ARKQ', 'ARKW', 'ARKG', 'ARKF', 'ARKX', 'PRNT', 'IZRL',
        # Leveraged/Inverse ETFs
        'SQQQ', 'TQQQ', 'SPXL', 'SPXS', 'UPRO', 'SPXU', 'TNA', 'TZA', 'FAS', 'FAZ',
        'TECL', 'TECS', 'CURE', 'RXD', 'LABU', 'LABD', 'NAIL', 'NYMT',
        # Volatility ETFs
        'UVXY', 'VXX', 'SVXY', 'VIXY', 'TVIX', 'XIV', 'VIXM', 'VXZ',
        # International ETFs
        'EEM', 'FXI', 'EWJ', 'EWZ', 'RSX', 'INDA', 'MCHI', 'ASHR', 'EWY', 'EWT',
        'EWG', 'EWU', 'EWC', 'EWA', 'EWH', 'EWS', 'EWW', 'EWI', 'EWP', 'EWQ',
        # Dividend/Income ETFs
        'MSTY', 'XDTE', 'ULTY', 'JEPI', 'JEPQ', 'SCHD', 'DIVO', 'QYLD', 'RYLD',
        'XYLD', 'NUSI', 'QQQX', 'SPYD', 'HDV', 'VYM', 'DGRO', 'NOBL', 'VIG',
        # Crypto ETFs
        'BITO', 'BTCC', 'ETHE', 'GBTC', 'COIN', 'MSTR', 'RIOT', 'MARA',
        # Real Estate ETFs
        'VNQ', 'VNQI', 'RWR', 'SCHH', 'IYR', 'XLRE', 'REM', 'MORT', 'REZ',
        # Currency ETFs
        'UUP', 'FXE', 'FXY', 'FXB', 'FXC', 'FXA', 'CYB', 'DBV', 'UDN',
        # Small/Mid Cap ETFs
        'IJH', 'IJR', 'MDY', 'SLY', 'VB', 'VO', 'VTWO', 'VTWG', 'SCHA', 'SCHM',
        # Growth/Value ETFs
        'VUG', 'VTV', 'IVW', 'IVE', 'MTUM', 'QUAL', 'USMV', 'VMOT', 'SPLG', 'SPTM'
    }
    
    if symbol in known_etfs:
        return 'ETF'
    
    # Check for ETF patterns (many ETFs end with common suffixes)
    etf_patterns = ['ETF', 'FUND', 'TR', 'TRUST']
    if any(pattern in symbol for pattern in etf_patterns):
        return 'ETF'
    
    # Use Finnhub API for enhanced categorization as fallback
    try:
        finnhub_category = categorize_ticker(symbol)
        if finnhub_category != 'Unknown':
            logger.info(f"Enhanced categorization for {symbol}: {finnhub_category}")
            return finnhub_category
    except Exception as e:
        logger.warning(f"Finnhub categorization failed for {symbol}: {e}")
    
    # Default to Stock for regular equity symbols
    return 'Stock'

def update_asset_categories():
    """Update asset categories for all existing transactions"""
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            
            # Get all transactions that need categorization
            cursor.execute("""
                SELECT rowid, Symbol, Instrument_Type, Call_or_Put, Strike_Price, Expiration_Date
                FROM transactions 
                WHERE Symbol IS NOT NULL AND Symbol != ''
            """)
            
            transactions = cursor.fetchall()
            updated_count = 0
            
            for transaction in transactions:
                trans_id, symbol, instrument_type, call_or_put, strike_price, expiration_date = transaction
                
                # Categorize the asset
                category = categorize_asset(symbol, instrument_type, call_or_put, strike_price, expiration_date)
                
                # Update the transaction with the category
                cursor.execute("""
                    UPDATE transactions 
                    SET Asset_Category = ?
                    WHERE rowid = ?
                """, (category, trans_id))
                
                updated_count += 1
            
            conn.commit()
            logger.info(f"Updated asset categories for {updated_count} transactions")
            return updated_count
            
    except Exception as e:
        log_error(e, "Error updating asset categories")
        return 0

if __name__ == '__main__':
    import webbrowser
    import threading
    import time
    
    # Check if database exists
    if not os.path.exists(DATABASE):
        print(f"Database {DATABASE} not found. Please run create_database.py first.")
        exit(1)
    
    def open_browser():
        """Open the web browser after a short delay"""
        time.sleep(1.5)  # Wait for Flask to start
        webbrowser.open('http://localhost:5000')
    
    print("Starting Transaction Management Web Application...")
    print("Access the application at: http://localhost:5000")
    
    # Only open browser on initial start, not on Flask reloader restart
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        print("Opening browser automatically...")
        # Start browser in a separate thread
        browser_thread = threading.Thread(target=open_browser)
        browser_thread.daemon = True
        browser_thread.start()
    
    # Start Flask application
    app.run(debug=True, host='0.0.0.0', port=5000)