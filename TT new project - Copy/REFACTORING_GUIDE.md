# Code Refactoring Guide

This document outlines the refactoring improvements made to the Transaction Management System to enhance code organization, maintainability, and scalability.

## Overview of Changes

The codebase has been refactored to follow better software engineering practices:

1. **Service Layer Architecture**: Separated business logic into dedicated service classes
2. **Reusable Components**: Created template components to reduce code duplication
3. **Validation Framework**: Implemented comprehensive validation classes
4. **Configuration Management**: Centralized application settings
5. **Error Handling**: Improved error handling and validation

## New Architecture

### Service Layer (`/services/`)

#### DatabaseService (`services/database_service.py`)
- Handles all database operations with proper connection management
- Provides methods for common database queries
- Includes automatic error handling and connection cleanup

```python
from services.database_service import DatabaseService

db_service = DatabaseService('transactions.db')
transactions = db_service.get_recent_transactions(limit=10)
```

#### PortfolioService (`services/portfolio_service.py`)
- Manages portfolio calculations and analysis
- Breaks down complex calculations into smaller, focused methods
- Handles unrealized gains/losses calculations with FIFO matching

```python
from services.portfolio_service import PortfolioService

portfolio_service = PortfolioService(db_service)
holdings = portfolio_service.calculate_unrealized_gains_losses()
```

#### TaxService (`services/tax_service.py`)
- Handles tax-related calculations and reporting
- Processes realized gains/losses with proper FIFO accounting
- Manages dividend data and tax reporting

```python
from services.tax_service import TaxService

tax_service = TaxService(db_service)
tax_data = tax_service.calculate_realized_gains_losses(year='2024')
```

### Validation Framework (`/utils/`)

#### Validators (`utils/validators.py`)
- Comprehensive validation classes for different data types
- Custom exception handling for validation errors
- Input sanitization and data cleaning

```python
from utils.validators import TransactionValidator, ValidationError

try:
    cleaned_data = TransactionValidator.validate_transaction(raw_data)
except ValidationError as e:
    print(f"Validation failed: {e.message}")
```

### Configuration Management (`/config/`)

#### Settings (`config/settings.py`)
- Centralized configuration management
- Environment-specific settings
- Validation of configuration parameters

```python
from config.settings import config

# Access database configuration
db_path = config.database.path
api_timeout = config.api.request_timeout
```

### Template Components (`/templates/components/`)

#### Tax Summary Card (`templates/components/tax_summary_card.html`)
- Reusable component for tax summary displays
- Reduces code duplication across templates
- Consistent styling and behavior

```html
{% set card_data = {
    'icon_class': 'bi bi-graph-up text-success',
    'title': 'Realized Gains',
    'value_eur': 1500.00,
    'value_usd': 1650.00,
    'text_class': 'text-success',
    'subtitle': 'Completed transactions'
} %}
{% include 'components/tax_summary_card.html' with context %}
```

## Benefits of Refactoring

### 1. Improved Maintainability
- **Separation of Concerns**: Business logic is separated from presentation logic
- **Single Responsibility**: Each class has a focused, well-defined purpose
- **Reduced Coupling**: Services can be used independently

### 2. Enhanced Testability
- **Unit Testing**: Individual services can be tested in isolation
- **Mock Dependencies**: Database operations can be easily mocked
- **Validation Testing**: Input validation can be thoroughly tested

### 3. Better Code Reusability
- **Service Classes**: Can be reused across different parts of the application
- **Template Components**: Reduce HTML duplication and ensure consistency
- **Validation Logic**: Centralized validation rules

### 4. Improved Error Handling
- **Custom Exceptions**: Specific error types for different validation failures
- **Graceful Degradation**: Better handling of database and API errors
- **User-Friendly Messages**: Clear error messages for validation failures

### 5. Configuration Management
- **Environment Flexibility**: Easy switching between development/production settings
- **Security**: Sensitive configuration can be managed via environment variables
- **Validation**: Configuration parameters are validated on startup

## Migration Guide

### Using the New Services

#### Before (Old Code)
```python
# Direct database access in routes
@app.route('/dashboard')
def dashboard():
    conn = sqlite3.connect('transactions.db')
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM transactions")
    total_records = cursor.fetchone()[0]
    conn.close()
    # ... more database operations
```

#### After (Refactored Code)
```python
# Using service layer
from services.database_service import DatabaseService
from services.portfolio_service import PortfolioService

db_service = DatabaseService()
portfolio_service = PortfolioService(db_service)

@app.route('/dashboard')
def dashboard():
    total_records = db_service.get_total_records()
    holdings = portfolio_service.calculate_unrealized_gains_losses()
    # Clean, focused code
```

### Template Refactoring

#### Before (Duplicated HTML)
```html
<!-- Repeated card structure -->
<div class="col-md-3">
    <div class="card text-center">
        <div class="card-body">
            <!-- 20+ lines of repeated HTML -->
        </div>
    </div>
</div>
```

#### After (Reusable Component)
```html
<!-- Simple, reusable component -->
{% set card_data = {...} %}
{% include 'components/tax_summary_card.html' with context %}
```

## Best Practices

### 1. Service Usage
- Always use dependency injection for services
- Handle service exceptions appropriately
- Use type hints for better code documentation

### 2. Validation
- Validate all user inputs using the validation framework
- Handle ValidationError exceptions gracefully
- Provide meaningful error messages to users

### 3. Configuration
- Use environment variables for sensitive settings
- Validate configuration on application startup
- Document configuration options clearly

### 4. Template Components
- Keep components focused and reusable
- Use clear parameter names and documentation
- Test components with different data scenarios

## Future Improvements

1. **API Layer**: Add RESTful API endpoints using the service layer
2. **Caching**: Implement caching for expensive calculations
3. **Logging**: Add comprehensive logging throughout the application
4. **Testing**: Create comprehensive test suite for all services
5. **Documentation**: Generate API documentation from code

## Conclusion

This refactoring significantly improves the codebase structure, making it more maintainable, testable, and scalable. The new architecture follows industry best practices and provides a solid foundation for future development.