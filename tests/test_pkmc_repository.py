import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.orm import Session
from sqlalchemy import text
import polars as pl
from modules.pkmc.infrastructure.repository import PKMCRepository
from database.models.pkmc import PKMC


@pytest.fixture
def mock_session():
    mock = Mock()
    mock.commit = Mock()
    mock.execute = Mock()
    mock.rollback = Mock()
    return mock

@pytest.fixture
def repository(mock_session):
    return PKMCRepository(mock_session)


class TestPKMCRepository:
    @pytest.fixture
    def sample_records(self):
        """Create sample PKMC records for testing"""
        return [
            {
                "partnumber": "PN-1234",
                "supply_area": "P01A",
                "num_reg_circ": "CIRC-001",
                "deposit_type": "B01",
                "deposit_position": "Pos1",
                "container": "Container1",
                "description": "Item 1",
                "pack_standard": "Standard1",
                "qty_per_box": 10.0,
                "qty_max_box": 50.0,
                "total_theoretical_qty": 500.0,
                "qty_for_restock": 490.0,
                "rack": "Rack1",
                "lb_balance": 2000.0,
                "lb_balance_box": 200.0,
                "created_at": "2024-01-01 00:00:00"
            },
            {
                "partnumber": "PN-5678",
                "supply_area": "P02B",
                "num_reg_circ": "CIRC-002",
                "deposit_type": "B01",
                "deposit_position": "Pos2",
                "container": "Container2",
                "description": "Item 2",
                "pack_standard": "Standard2",
                "qty_per_box": 15.0,
                "qty_max_box": 60.0,
                "total_theoretical_qty": 900.0,
                "qty_for_restock": 885.0,
                "rack": "Rack2",
                "lb_balance": 2000.0,
                "lb_balance_box": 133.33,
                "created_at": "2024-01-01 00:00:00"
            },
        ]
    
    def test_repository_initialization(self, mock_session):
        """Test repository initialization"""
        repo = PKMCRepository(mock_session)
        
        assert repo.db == mock_session
        assert repo.model == PKMC
    
    def test_fetch_all_success(self, repository, mock_session, sample_records):
        mock_result = Mock()
        mock_mappings = Mock()
        mock_result.mappings.return_value = mock_mappings

        mock_mappings.all.return_value = sample_records  

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
        mock_mappings.all.return_value = sample_records
        
        mock_session.execute.return_value = mock_result
        
        # Call fetch_all with limit
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
            "partnumber": ["PN-1234", "PN-5678"],
            "supply_area": ["P01A", "P02B"],
            "num_reg_circ": ["CIRC-001", "CIRC-002"],
            "deposit_type": ["B01", "B01"],
            "deposit_position": ["Pos1", "Pos2"],
            "container": ["Container1", "Container2"],
            "description": ["Item 1", "Item 2"],
            "pack_standard": ["Standard1", "Standard2"],
            "qty_per_box": [10.0, 15.0],
            "qty_max_box": [50.0, 60.0],
            "total_theoretical_qty": [500.0, 900.0],
            "qty_for_restock": [490.0, 885.0],
            "rack": ["Rack1", "Rack2"],
            "lb_balance": [2000.0, 2000.0],
            "lb_balance_box": [200.0, 133.33],
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
            "partnumber": [f"PN-{i}" for i in range(15)],
            "supply_area": [f"P{i:02d}A" for i in range(15)],
            "num_reg_circ": [f"CIRC-{i}" for i in range(15)],
            "deposit_type": ["B01"] * 15,
            "deposit_position": [f"Pos{i}" for i in range(15)],
            "container": [f"Container{i}" for i in range(15)],
            "description": [f"Item {i}" for i in range(15)],
            "pack_standard": [f"Standard{i}" for i in range(15)],
            "qty_per_box": [10.0 + i for i in range(15)],
            "qty_max_box": [50.0 + i for i in range(15)],
            "total_theoretical_qty": [500.0 + (i * 10) for i in range(15)],
            "qty_for_restock": [490.0 + (i * 10) for i in range(15)],
            "rack": [f"Rack{i}" for i in range(15)],
            "lb_balance": [2000.0] * 15,
            "lb_balance_box": [200.0 + i for i in range(15)],
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
            "partnumber": ["PN-1234"],
            "supply_area": ["P01A"],
            "num_reg_circ": ["CIRC-001"],
            "deposit_type": ["B01"],
            "deposit_position": ["Pos1"],
            "container": ["Container1"],
            "description": ["Item 1"],
            "pack_standard": ["Standard1"],
            "qty_per_box": [10.0],
            "qty_max_box": [50.0],
            "total_theoretical_qty": [500.0],
            "qty_for_restock": [490.0],
            "rack": ["Rack1"],
            "lb_balance": [2000.0],
            "lb_balance_box": [200.0],
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
            "partnumber": [],
            "supply_area": [],
            "num_reg_circ": [],
            "deposit_type": [],
            "deposit_position": [],
            "container": [],
            "description": [],
            "pack_standard": [],
            "qty_per_box": [],
            "qty_max_box": [],
            "total_theoretical_qty": [],
            "qty_for_restock": [],
            "rack": [],
            "lb_balance": [],
            "lb_balance_box": [],
        })
        
        mock_session.commit = Mock()
        mock_session.execute = Mock()
        
        result = repository.bulk_upsert(df)
        
        # Empty dataframe should return 0
        assert result == 0
        assert mock_session.commit.called


class TestPKMCRepositoryIntegration:
    """Integration tests for repository with real database"""
    
    def test_repository_with_real_db(self, test_db):
        """Test repository with real SQLite database"""
        repo = PKMCRepository(test_db)
        
        # Test fetch_all on empty database
        result = repo.fetch_all()
        
        assert isinstance(result, list)
        assert len(result) == 0
    
    def test_bulk_upsert_with_real_db(self, test_db):
        """Test bulk_upsert with real database"""
        repo = PKMCRepository(test_db)
        
        df = pl.DataFrame({
            "partnumber": ["PN-1234", "PN-5678"],
            "supply_area": ["P01A", "P02B"],
            "num_reg_circ": ["CIRC-001", "CIRC-002"],
            "deposit_type": ["B01", "B01"],
            "deposit_position": ["Pos1", "Pos2"],
            "container": ["Container1", "Container2"],
            "description": ["Item 1", "Item 2"],
            "pack_standard": ["Standard1", "Standard2"],
            "qty_per_box": [10.0, 15.0],
            "qty_max_box": [50.0, 60.0],
            "total_theoretical_qty": [500.0, 900.0],
            "qty_for_restock": [490.0, 885.0],
            "rack": ["Rack1", "Rack2"],
            "lb_balance": [2000.0, 2000.0],
            "lb_balance_box": [200.0, 133.33],
        })
        
        # Test upsert
        result = repo.bulk_upsert(df)
        
        assert result == 2
        
        # Verify data was inserted
        records = repo.fetch_all()
        assert len(records) == 2