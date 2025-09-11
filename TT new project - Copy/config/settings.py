"""Configuration management for transaction management system."""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    path: str = 'transactions.db'
    timeout: int = 30
    check_same_thread: bool = False
    
@dataclass
class APIConfig:
    """API configuration settings."""
    alpha_vantage_key: Optional[str] = None
    request_timeout: int = 10
    max_retries: int = 3
    cache_timeout: int = 300  # 5 minutes
    
@dataclass
class UIConfig:
    """UI configuration settings."""
    items_per_page: int = 20
    max_items_per_page: int = 100
    date_format: str = '%Y-%m-%d'
    currency_format: str = '€{:,.2f}'
    
@dataclass
class FileConfig:
    """File handling configuration."""
    upload_folder: str = 'uploads'
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    allowed_extensions: tuple = ('.csv', '.xlsx', '.xls')
    
@dataclass
class SecurityConfig:
    """Security configuration settings."""
    secret_key: str = 'your-secret-key-change-in-production'
    session_timeout: int = 3600  # 1 hour
    max_login_attempts: int = 5
    
class AppConfig:
    """Main application configuration class."""
    
    def __init__(self, environment: str = 'development'):
        self.environment = environment
        self.database = DatabaseConfig()
        self.api = APIConfig()
        self.ui = UIConfig()
        self.file = FileConfig()
        self.security = SecurityConfig()
        
        # Load environment-specific settings
        self._load_environment_config()
        
        # Load from environment variables
        self._load_from_env()
    
    def _load_environment_config(self):
        """Load environment-specific configuration."""
        if self.environment == 'production':
            self.database.path = os.path.join('data', 'transactions.db')
            self.security.secret_key = os.environ.get('SECRET_KEY', 'change-me-in-production')
            self.api.request_timeout = 15
        elif self.environment == 'testing':
            self.database.path = ':memory:'
            self.api.cache_timeout = 0  # Disable caching in tests
            self.ui.items_per_page = 5
    
    def _load_from_env(self):
        """Load configuration from environment variables."""
        # Database settings
        if os.environ.get('DATABASE_PATH'):
            self.database.path = os.environ['DATABASE_PATH']
        
        # API settings
        if os.environ.get('ALPHA_VANTAGE_API_KEY'):
            self.api.alpha_vantage_key = os.environ['ALPHA_VANTAGE_API_KEY']
        
        if os.environ.get('API_TIMEOUT'):
            try:
                self.api.request_timeout = int(os.environ['API_TIMEOUT'])
            except ValueError:
                pass
        
        # Security settings
        if os.environ.get('SECRET_KEY'):
            self.security.secret_key = os.environ['SECRET_KEY']
        
        if os.environ.get('SESSION_TIMEOUT'):
            try:
                self.security.session_timeout = int(os.environ['SESSION_TIMEOUT'])
            except ValueError:
                pass
    
    def get_flask_config(self) -> Dict[str, Any]:
        """Get Flask-specific configuration dictionary."""
        return {
            'SECRET_KEY': self.security.secret_key,
            'PERMANENT_SESSION_LIFETIME': self.security.session_timeout,
            'MAX_CONTENT_LENGTH': self.file.max_file_size,
            'UPLOAD_FOLDER': self.file.upload_folder
        }
    
    def validate(self) -> bool:
        """Validate configuration settings."""
        errors = []
        
        # Validate database path
        if not self.database.path:
            errors.append("Database path cannot be empty")
        
        # Validate API settings
        if self.api.request_timeout <= 0:
            errors.append("API request timeout must be positive")
        
        if self.api.cache_timeout < 0:
            errors.append("Cache timeout cannot be negative")
        
        # Validate UI settings
        if self.ui.items_per_page <= 0:
            errors.append("Items per page must be positive")
        
        if self.ui.max_items_per_page < self.ui.items_per_page:
            errors.append("Max items per page must be >= items per page")
        
        # Validate file settings
        if self.file.max_file_size <= 0:
            errors.append("Max file size must be positive")
        
        # Validate security settings
        if len(self.security.secret_key) < 16:
            errors.append("Secret key should be at least 16 characters")
        
        if self.security.session_timeout <= 0:
            errors.append("Session timeout must be positive")
        
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'environment': self.environment,
            'database': {
                'path': self.database.path,
                'timeout': self.database.timeout,
                'check_same_thread': self.database.check_same_thread
            },
            'api': {
                'alpha_vantage_key': '***' if self.api.alpha_vantage_key else None,
                'request_timeout': self.api.request_timeout,
                'max_retries': self.api.max_retries,
                'cache_timeout': self.api.cache_timeout
            },
            'ui': {
                'items_per_page': self.ui.items_per_page,
                'max_items_per_page': self.ui.max_items_per_page,
                'date_format': self.ui.date_format,
                'currency_format': self.ui.currency_format
            },
            'file': {
                'upload_folder': self.file.upload_folder,
                'max_file_size': self.file.max_file_size,
                'allowed_extensions': self.file.allowed_extensions
            },
            'security': {
                'secret_key': '***',
                'session_timeout': self.security.session_timeout,
                'max_login_attempts': self.security.max_login_attempts
            }
        }

# Global configuration instance
config = AppConfig(environment=os.environ.get('FLASK_ENV', 'development'))

# Validate configuration on import
try:
    config.validate()
except ValueError as e:
    print(f"Configuration warning: {e}")