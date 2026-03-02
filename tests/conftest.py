import os
import sys
import tempfile
import pytest
from pathlib import Path
from unittest.mock import MagicMock
import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

# -------------------------------------------------------------------------------------
# Ensure project root in PYTHONPATH
# -------------------------------------------------------------------------------------

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

env_file = project_root / "config" / ".env"
if env_file.exists():
    load_dotenv(env_file)

from database.base import Base
from fastapi.testclient import TestClient
from main import create_app


# =====================================================================================
# DATABASE FIXTURES
# =====================================================================================

@pytest.fixture
def test_db():
    """Create a real temporary database (SQLite in-memory by default)."""
    mysql_url = os.getenv("TEST_MYSQL_URL")

    if not mysql_url:
        try:
            from config.settings import settings
            mysql_url = getattr(settings, "TEST_MYSQL_URL", None)
        except Exception:
            mysql_url = None

    # If MySQL test server available, use it — otherwise SQLite memory DB.
    engine = create_engine(mysql_url or "sqlite:///:memory:")

    # drop any pre-existing tables to guarantee a fresh state; this is important
    # when pointing at a real MySQL test database that may retain rows between
    # pytest invocations.
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    yield db

    db.close()
    engine.dispose()


@pytest.fixture
def mock_db():
    """Provide a mocked database session for unit tests."""
    return MagicMock(spec=Session)


# =====================================================================================
# FASTAPI APP + CLIENT
# =====================================================================================

@pytest.fixture(scope="session")
def app():
    """FastAPI application used across all API tests."""
    return create_app()


@pytest.fixture
def client(app):
    """Returns a TestClient wrapping the FastAPI app."""
    return TestClient(app)


# =====================================================================================
# EXCEL FILE FIXTURE (PK05 / PKMC PIPELINES)
# =====================================================================================

@pytest.fixture
def temp_excel_file():
    """
    Creates a temporary Excel file with all expected raw columns normally found
    in PK05/PKMC SAP exports.

    Uses mkstemp() to avoid Windows file locking issues.
    """
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)  # prevents PermissionError on Windows

    df = pl.DataFrame({
        # PK05 / PKMC expected raw SAP columns
        "Material": ["PN-123"],
        "Área abastec.prod.": ["Area1"],
        "Nº circ.regul.": ["CIRC-001"],
        "Depósito": ["LB01"],
        "Responsável": ["João"],
        "Ponto de descarga": ["P1"],
        # PK05-specific column needed by cleaner
        "Denominação SupM": ["T001 Item A"],

        # Additional fields PKMC cleaner expects
        "Tipo de depósito": ["Type1"],
        "Posição no depósito": ["Pos1"],
        "Container": ["Box"],
        "Texto breve de material": ["Item Teste"],
        "Norma de embalagem": ["STD"],
        "Quantidade Kanban": [10.0],
        "Posição de armazenamento": [50.0],

        # Shared column PK05/PKMC use (some overlap with Denominação SupM)
        "Descrição": ["Item Teste"],
        "Takt": ["T001"],
    })

    df.write_excel(path)
    yield path

    if os.path.exists(path):
        os.unlink(path)


# =====================================================================================
# SAMPLE DATAFRAMES
# =====================================================================================

@pytest.fixture
def sample_polars_df():
    """Sample transformed dataframe."""
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
    """Same as the DataFrame returned after pipeline cleaning."""
    return pl.DataFrame({
        "supply_area": ["Area1", "Area2"],
        "deposit": ["LB01", "LB01"],
        "responsible": ["John", "Jane"],
        "discharge_point": ["P1", "P2"],
        "description": ["T001 Item A", "T002 Item B"],
        "takt": ["T001", "T002"],
    })