import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.orm import Session
from sqlalchemy import text
import polars as pl
from modules.pk05.infrastructure.repository import PK05Repository
from database.models.pk05 import PK05


class TestPK05Repository:
    """Test suite for PK05Repository"""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mocked database session"""
        return MagicMock(spec=Session)
    
    @pytest.fixture
    def repository(self, mock_session):
        """Create a repository instance with mocked session"""
        return PK05Repository(mock_session)
    
    @pytest.fixture
    def sample_records(self):
        """Create sample records for testing"""
        return [
            {
                "supply_area": "Area1",
                "deposit": "LB01",
                "responsible": "John",
                "discharge_point": "P1",
                "description": "Item 1",
                "takt": "T001",
                "created_at": "2024-01-01 00:00:00"
            },
            {
                "supply_area": "Area2",
                "deposit": "LB01",
                "responsible": "Jane",
                "discharge_point": "P2",
                "description": "Item 2",
                "takt": "T002",
                "created_at": "2024-01-01 00:00:00"
            },
        ]
    
    def test_repository_initialization(self, mock_session):
        """Test repository initialization"""
        repo = PK05Repository(mock_session)
        
        assert repo.db == mock_session
        assert repo.model == PK05
    
    def test_fetch_all_success(self, repository, mock_session, sample_records):
        mock_result = Mock()
        mock_mappings = Mock()
        mock_result.mappings.return_value = mock_mappings
        mock_mappings.all.return_value = [dict(rec) for rec in sample_records]

        mock_session.execute.return_value = mock_result

        result = repository.fetch_all()

        assert len(result) == len(sample_records)
        assert mock_session.execute.called
    
    def test_fetch_all_with_limit(self, repository, mock_session, sample_records):
        """Test fetch_all with limit parameter"""
        # Mock the database execute and mappings
        mock_result = Mock()
        mock_mappings = Mock()
        mock_result.mappings.return_value = mock_mappings
        mock_mappings.all.return_value = [dict(rec) for rec in sample_records]
        
        mock_session.execute.return_value = mock_result
        
        result = repository.fetch_all(limit=1)
        
        # Verify limit was used
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0][0]
        assert "LIMIT 1" in str(call_args)
    
    def test_fetch_all_exception_handling(self, repository, mock_session):
        """Test exception handling in fetch_all"""
        # Mock an exception
        mock_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception) as exc_info:
            repository.fetch_all()
        
        assert "Database error" in str(exc_info.value)
    
    def test_bulk_upsert_success(self, repository, mock_session):
        """Test successful bulk_upsert operation"""
        df = pl.DataFrame({
            "supply_area": ["Area1", "Area2"],
            "deposit": ["LB01", "LB01"],
            "responsible": ["John", "Jane"],
            "discharge_point": ["P1", "P2"],
            "description": ["Item 1", "Item 2"],
            "takt": ["T001", "T002"],
        })
        
        # Mock commit
        mock_session.commit = Mock()
        
        # Mock execute to simulate successful insertion
        mock_session.execute = Mock()
        
        # Call bulk_upsert
        result = repository.bulk_upsert(df, batch_size=10)
        
        # Verify results
        assert result == 2
        assert mock_session.commit.called
    
    def test_bulk_upsert_with_custom_batch_size(self, repository, mock_session):
        """Test bulk_upsert with custom batch size"""
        # Create a larger dataframe to trigger multiple batches
        data = {
            "supply_area": [f"Area{i}" for i in range(15)],
            "deposit": ["LB01"] * 15,
            "responsible": [f"User{i}" for i in range(15)],
            "discharge_point": [f"P{i}" for i in range(15)],
            "description": [f"Item {i}" for i in range(15)],
            "takt": [f"T{i:03d}" for i in range(15)],
        }
        df = pl.DataFrame(data)
        
        # Mock execute and commit
        mock_session.execute = Mock()
        mock_session.commit = Mock()
        
        # Call bulk_upsert with batch size of 5
        result = repository.bulk_upsert(df, batch_size=5)
        
        # Verify results
        assert result == 15
        # Should be called 3 times for 3 batches of 5 records each
        assert mock_session.execute.call_count == 3
    
    def test_bulk_upsert_rollback_on_error(self, repository, mock_session):
        """Test rollback on error during bulk_upsert"""
        df = pl.DataFrame({
            "supply_area": ["Area1"],
            "deposit": ["LB01"],
            "responsible": ["John"],
            "discharge_point": ["P1"],
            "description": ["Item 1"],
            "takt": ["T001"],
        })
        
        # Mock execute to raise exception
        mock_session.execute.side_effect = Exception("Insert failed")
        mock_session.rollback = Mock()
        
        with pytest.raises(Exception) as exc_info:
            repository.bulk_upsert(df)
        
        # Verify rollback was called
        assert mock_session.rollback.called
        assert "Insert failed" in str(exc_info.value)
    
    def test_bulk_upsert_empty_dataframe(self, repository, mock_session):
        """Test bulk_upsert with empty dataframe"""
        df = pl.DataFrame({
            "supply_area": [],
            "deposit": [],
            "responsible": [],
            "discharge_point": [],
            "description": [],
            "takt": [],
        })
        
        mock_session.commit = Mock()
        mock_session.execute = Mock()
        
        result = repository.bulk_upsert(df)
        
        # Empty dataframe should return 0
        assert result == 0
        assert mock_session.commit.called


class TestBaseRepository:
    """Test suite for BaseRepository functionality"""
    
    def test_to_dict_method(self, mock_db):
        """Test _to_dict method functionality"""
        repo = PK05Repository(mock_db)
        
        # Create a mock model instance
        mock_obj = Mock()
        mock_obj.__dict__ = {
            "supply_area": "Area1",
            "deposit": "LB01",
            "_sa_instance_state": Mock(),  # SQLAlchemy internal attribute
        }
        
        result = repo._to_dict(mock_obj)
        
        # Should not include private attributes
        assert "supply_area" in result
        assert "deposit" in result
        assert "_sa_instance_state" not in result


class TestRepositoryIntegration:
    """Integration tests for repository with real database"""
    
    def test_repository_with_real_db(self, test_db):
        """Test repository with real SQLite database"""
        repo = PK05Repository(test_db)
        
        # Test fetch_all on empty database
        result = repo.fetch_all()
        
        assert isinstance(result, list)
        assert len(result) == 0
    
    def test_bulk_upsert_with_real_db(self, test_db):
        """Test bulk_upsert with real database"""
        repo = PK05Repository(test_db)
        
        df = pl.DataFrame({
            "supply_area": ["Area1", "Area2"],
            "deposit": ["LB01", "LB01"],
            "responsible": ["John", "Jane"],
            "discharge_point": ["P1", "P2"],
            "description": ["Item 1", "Item 2"],
            "takt": ["T001", "T002"],
        })
        
        # Test upsert
        result = repo.bulk_upsert(df)
        
        assert result == 2
        
        # Verify data was inserted
        records = repo.fetch_all()
        assert len(records) == 2
