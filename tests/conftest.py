import os
import sys
from pathlib import Path
import tempfile
import pytest
from unittest.mock import Mock, MagicMock, patch
import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
from typing import Optional

# Add the project root to sys.path so imports work correctly
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables for testing
env_file = project_root / "config" / ".env"
if env_file.exists():
    load_dotenv(env_file)

from database.base import Base
from fastapi.testclient import TestClient
from main import create_app


@pytest.fixture
def test_db():
    """Create a database for testing.

    By default we use an in-memory SQLite database, but if the
    environment variable `TEST_MYSQL_URL` or the settings object
    contains a MySQL connection URL we will connect to that server.
    This allows integration tests to run against the same engine used
    in production while keeping the default fast and isolated.
    """
    mysql_url = os.getenv("TEST_MYSQL_URL")
    if not mysql_url:
        try:
            from config.settings import settings
            mysql_url = getattr(settings, "TEST_MYSQL_URL", None)
        except Exception:
            mysql_url = None

    if mysql_url:
        # use provided MySQL server (user must have created a test database)
        engine = create_engine(mysql_url)
    else:
        # fall back to SQLite in-memory for unit tests
        engine = create_engine("sqlite:///:memory:")

    # create all tables so repository methods work
    Base.metadata.create_all(engine)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    yield db

    db.close()
    engine.dispose()


@pytest.fixture
def mock_db():
    """Create a mocked database session"""
    db = MagicMock(spec=Session)
    return db


@pytest.fixture
def client():
    """Create a TestClient for testing API endpoints"""
    app = create_app()
    return TestClient(app)


@pytest.fixture
def temp_excel_file():
    """Create a temporary Excel file for testing"""
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp_file:
        # Create sample data
        df = pl.DataFrame({
            "Área abastec.prod.": ["Area1", "Area2"],
            "Depósito": ["LB01", "LB01"],
            "Responsável": ["John", "Jane"],
            "Ponto de descarga": ["P1", "P2"],
            "Denominação SupM": ["T001 Item A", "T002 Item B"],
        })
        
        # Write to Excel
        df.write_excel(tmp_file.name)
        tmp_file.flush()
        
        yield tmp_file.name
        
        # Cleanup
        if os.path.exists(tmp_file.name):
            os.unlink(tmp_file.name)


@pytest.fixture
def sample_polars_df():
    """Create a sample Polars DataFrame for testing"""
    return pl.DataFrame({
        "supply_area": ["Area1", "Area2", "Area3"],
        "deposit": ["LB01", "LB01", "LB02"],
        "responsible": ["John", "Jane", "Bob"],
        "discharge_point": ["P1", "P2", "P3"],
        "description": ["T001 Item A", "T002 Item B", "T003 Item C"],
        "takt": ["T001", "T002", "T003"],
    })


@pytest.fixture
def sample_cleaned_df():
    """Create a sample cleaned DataFrame with proper structure"""
    return pl.DataFrame({
        "supply_area": ["Area1", "Area2"],
        "deposit": ["LB01", "LB01"],
        "responsible": ["John", "Jane"],
        "discharge_point": ["P1", "P2"],
        "description": ["T001 Item A", "T002 Item B"],
        "takt": ["T001", "T002"],
    })
