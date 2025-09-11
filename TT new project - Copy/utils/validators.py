"""Validation utilities for transaction management system."""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import re

class ValidationError(Exception):
    """Custom exception for validation errors."""
    
    def __init__(self, message: str, field: Optional[str] = None):
        self.message = message
        self.field = field
        super().__init__(self.message)

class TransactionValidator:
    """Validator class for transaction data."""
    
    REQUIRED_FIELDS = ['Date', 'Symbol', 'Action', 'Quantity', 'Average_Price']
    VALID_ACTIONS = ['BUY', 'SELL', 'DEPOSIT', 'WITHDRAWAL']
    VALID_TYPES = ['Trade', 'Money Movement']
    
    @classmethod
    def validate_transaction(cls, transaction_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean transaction data."""
        errors = []
        cleaned_data = {}
        
        # Check required fields
        for field in cls.REQUIRED_FIELDS:
            if field not in transaction_data or transaction_data[field] is None:
                errors.append(f"Field '{field}' is required")
            else:
                cleaned_data[field] = transaction_data[field]
        
        if errors:
            raise ValidationError(f"Missing required fields: {', '.join(errors)}")
        
        # Validate date format
        try:
            datetime.strptime(cleaned_data['Date'], '%Y-%m-%d')
        except ValueError:
            errors.append("Date must be in YYYY-MM-DD format")
        
        # Validate symbol
        if not cls._is_valid_symbol(cleaned_data['Symbol']):
            errors.append("Symbol must be 1-10 alphanumeric characters")
        
        # Validate action
        if cleaned_data['Action'].upper() not in cls.VALID_ACTIONS:
            errors.append(f"Action must be one of: {', '.join(cls.VALID_ACTIONS)}")
        
        # Validate numeric fields
        numeric_fields = ['Quantity', 'Average_Price', 'Total', 'Fees', 'Commissions']
        for field in numeric_fields:
            if field in transaction_data:
                try:
                    value = float(transaction_data[field]) if transaction_data[field] else 0
                    if field in ['Quantity', 'Average_Price'] and value <= 0:
                        errors.append(f"{field} must be greater than 0")
                    cleaned_data[field] = value
                except (ValueError, TypeError):
                    errors.append(f"{field} must be a valid number")
        
        if errors:
            raise ValidationError(f"Validation errors: {'; '.join(errors)}")
        
        return cleaned_data
    
    @staticmethod
    def _is_valid_symbol(symbol: str) -> bool:
        """Check if symbol is valid format."""
        if not isinstance(symbol, str):
            return False
        return bool(re.match(r'^[A-Za-z0-9]{1,10}$', symbol.strip()))

class PortfolioValidator:
    """Validator class for portfolio-related data."""
    
    @staticmethod
    def validate_year_parameter(year: Union[str, int, None]) -> Optional[str]:
        """Validate and normalize year parameter."""
        if year is None:
            return None
        
        if isinstance(year, int):
            year = str(year)
        
        if year == 'ytd':
            return 'ytd'
        
        try:
            year_int = int(year)
            if 1900 <= year_int <= 2100:
                return str(year_int)
            else:
                raise ValidationError("Year must be between 1900 and 2100")
        except ValueError:
            raise ValidationError("Year must be a valid integer or 'ytd'")
    
    @staticmethod
    def validate_price_data(price: Any) -> float:
        """Validate and convert price data."""
        if price is None:
            raise ValidationError("Price cannot be None")
        
        try:
            price_float = float(price)
            if price_float < 0:
                raise ValidationError("Price cannot be negative")
            return price_float
        except (ValueError, TypeError):
            raise ValidationError("Price must be a valid number")
    
    @staticmethod
    def validate_quantity(quantity: Any) -> float:
        """Validate and convert quantity data."""
        if quantity is None:
            raise ValidationError("Quantity cannot be None")
        
        try:
            quantity_float = float(quantity)
            if quantity_float <= 0:
                raise ValidationError("Quantity must be greater than 0")
            return quantity_float
        except (ValueError, TypeError):
            raise ValidationError("Quantity must be a valid number")

class FileValidator:
    """Validator class for file uploads and processing."""
    
    ALLOWED_EXTENSIONS = {'.csv', '.xlsx', '.xls'}
    MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
    
    @classmethod
    def validate_file_upload(cls, file_path: str, file_size: int) -> None:
        """Validate uploaded file."""
        # Check file extension
        file_extension = file_path.lower().split('.')[-1]
        if f'.{file_extension}' not in cls.ALLOWED_EXTENSIONS:
            raise ValidationError(
                f"File type not allowed. Allowed types: {', '.join(cls.ALLOWED_EXTENSIONS)}"
            )
        
        # Check file size
        if file_size > cls.MAX_FILE_SIZE:
            raise ValidationError(
                f"File size too large. Maximum size: {cls.MAX_FILE_SIZE // (1024*1024)}MB"
            )
    
    @staticmethod
    def validate_csv_headers(headers: List[str], required_headers: List[str]) -> None:
        """Validate CSV file headers."""
        missing_headers = []
        for required_header in required_headers:
            if required_header not in headers:
                missing_headers.append(required_header)
        
        if missing_headers:
            raise ValidationError(
                f"Missing required CSV headers: {', '.join(missing_headers)}"
            )

class APIValidator:
    """Validator class for API requests and responses."""
    
    @staticmethod
    def validate_pagination_params(page: Any, per_page: Any) -> tuple:
        """Validate pagination parameters."""
        try:
            page_int = int(page) if page else 1
            per_page_int = int(per_page) if per_page else 20
            
            if page_int < 1:
                raise ValidationError("Page number must be greater than 0")
            
            if per_page_int < 1 or per_page_int > 100:
                raise ValidationError("Per page must be between 1 and 100")
            
            return page_int, per_page_int
        except ValueError:
            raise ValidationError("Page and per_page must be valid integers")
    
    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> tuple:
        """Validate date range parameters."""
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            if start_dt > end_dt:
                raise ValidationError("Start date must be before end date")
            
            return start_date, end_date
        except ValueError:
            raise ValidationError("Dates must be in YYYY-MM-DD format")