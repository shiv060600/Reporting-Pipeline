"""
Shared fixtures for all tests.
This file is special - pytest automatically loads it.
"""
import pytest
import pandas as pd
import polars as pl
import sys
import os
from unittest.mock import Mock
import sqlalchemy

# Add src to path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

@pytest.fixture
def sample_ingram_data():
    """Sample Ingram data for testing - runs before each test that uses it"""
    return pd.DataFrame({
        'ISBN': ['9781234567890', '9780987654321', '9781111111111'],
        'NAMECUST': ['AMAZON.COM', 'BARNES & NOBLE', 'TARGET CORP'], 
        'NETUNITS': [10, 25, 5],
        'NETAMT': [100.50, 250.75, 50.25],
        'YEAR': [2024, 2024, 2024],
        'MONTH': [10, 10, 10],
    })

@pytest.fixture
def sample_sage_data():
    """Sample Sage data for testing"""
    return pd.DataFrame({
        'ISBN': ['9781234567890', '9780555555555', '9781111111111'],
        'NEWBILLTO': ['Amazon', 'Independent Books', 'Target'],
        'NETUNITS': [15, 8, 12], 
        'NETAMT': [150.75, 80.40, 120.60],
        'YEAR': [2024, 2024, 2024],
        'MONTH': [10, 10, 10],
    })

@pytest.fixture  
def tolerance():
    """Standard tolerance for financial calculations"""
    return 0.05  # 5% as you mentioned

@pytest.fixture
def mock_database_engine():
    """Mock database engine for testing without real DB connection"""
    engine = Mock(spec=sqlalchemy.engine.Engine)
    
    # Mock connection context manager
    mock_connection = Mock()
    engine.connect.return_value.__enter__.return_value = mock_connection
    engine.connect.return_value.__exit__.return_value = None
    
    # Mock successful execution
    mock_connection.execute.return_value = Mock()
    
    return engine

@pytest.fixture
def real_database_engine():
    """Real database connection for integration tests"""
    try:
        # Import your actual connection string
        from helpers.paths import PATHS
        conn_string = PATHS["SSMS_CONN_STRING"]
        engine = sqlalchemy.create_engine(conn_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("SELECT 1"))
        
        yield engine
        engine.dispose()
        
    except Exception as e:
        pytest.skip(f"Database connection failed: {e}")

@pytest.fixture
def polars_sample_ingram():
    """Polars version of sample data for your pipeline functions"""
    return pl.DataFrame({
        'ISBN': ['9781234567890', '9780987654321', '9781111111111'],
        'NAMECUST': ['AMAZON.COM', 'BARNES & NOBLE', 'TARGET CORP'], 
        'NETUNITS': [10, 25, 5],
        'NETAMT': [100.50, 250.75, 50.25],
        'YEAR': [2024, 2024, 2024],
        'MONTH': [10, 10, 10],
    })

@pytest.fixture
def polars_sample_sage():
    """Polars version of sage data"""
    return pl.DataFrame({
        'ISBN': ['9781234567890', '9780555555555', '9781111111111'],
        'NEWBILLTO': ['Amazon', 'Independent Books', 'Target'],
        'NETUNITS': [15, 8, 12], 
        'NETAMT': [150.75, 80.40, 120.60],
        'YEAR': [2024, 2024, 2024],
        'MONTH': [10, 10, 10],
    })
