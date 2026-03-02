import pytest
from unittest.mock import Mock, patch, MagicMock
from fastapi.testclient import TestClient
from main import create_app
import polars as pl


@pytest.fixture
def app():
    """Create FastAPI app instance"""
    return create_app()


@pytest.fixture
def client(app):
    """Create test client"""
    return TestClient(app)


class TestPKMCRoutes:
    """Test suite for PKMC API routes"""
    
    @patch('modules.pkmc.api.routes.PKMCPipeline')
    def test_get_response_success(self, mock_pipeline, client):
        """Test GET /pkmc/response endpoint"""
        # Mock the pipeline to return sample data
        mock_instance = Mock()
        mock_df = pl.DataFrame({
            "partnumber": ["PN-1234"],
            "supply_area": ["Area1"],
            "deposit_type": ["Type1"],
            "deposit_position": ["Pos1"],
            "description": ["Item 1"],
            "qty_per_box": [10.0],
            "qty_max_box": [50.0],
            "total_theoretical_qty": [100.0],
            "qty_for_restock": [20.0],
            "rack": ["Rack1"],
            "lb_balance": [25.0],
            "lb_balance_box": [2.5],
        })
        
        mock_instance.run.return_value.head.return_value.collect.return_value = mock_df
        mock_pipeline.return_value = mock_instance
        
        # Make request
        response = client.get("/pkmc/response", params={"limit": 50})
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "data" in data
        assert len(data["data"]) == 1
    
    @patch('modules.pkmc.api.routes.PKMCPipeline')
    def test_get_response_with_limit(self, mock_pipeline, client):
        """Test GET /pkmc/response with limit parameter"""
        mock_instance = Mock()
        mock_df = pl.DataFrame({
            "partnumber": ["PN-1", "PN-2"],
            "supply_area": ["Area1", "Area2"],
            "deposit_type": ["Type1", "Type2"],
            "deposit_position": ["Pos1", "Pos2"],
            "description": ["Item 1", "Item 2"],
            "qty_per_box": [10.0, 15.0],
            "qty_max_box": [50.0, 60.0],
            "total_theoretical_qty": [100.0, 150.0],
            "qty_for_restock": [20.0, 30.0],
            "rack": ["Rack1", "Rack2"],
            "lb_balance": [25.0, 35.0],
            "lb_balance_box": [2.5, 3.5],
        })
        
        mock_instance.run.return_value.head.return_value.collect.return_value = mock_df
        mock_pipeline.return_value = mock_instance
        
        # Make request with limit
        response = client.get("/pkmc/response", params={"limit": 10})
        
        assert response.status_code == 200
        assert mock_instance.run.return_value.head.called
    
    @patch('modules.pkmc.api.routes.PKMCPipeline')
    def test_get_response_invalid_limit(self, mock_pipeline, client):
        """Test GET /pkmc/response with invalid limit"""
        response = client.get("/pkmc/response", params={"limit": 0})
        
        # Should fail validation
        assert response.status_code == 422
    
    @patch('modules.pkmc.api.routes.PKMCPipeline')
    def test_get_response_error(self, mock_pipeline, client):
        """Test GET /pkmc/response with pipeline error"""
        mock_instance = Mock()
        mock_instance.run.side_effect = Exception("Pipeline error")
        mock_pipeline.return_value = mock_instance
        
        response = client.get("/pkmc/response", params={"limit": 50})
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
    
    @patch('modules.pkmc.api.routes.PKMCRepository')
    def test_get_from_db_success(self, mock_repo_class, client):
        """Test GET /pkmc/response/db endpoint"""
        mock_instance = Mock()
        mock_instance.fetch_all.return_value = [
            {
                "partnumber": "PN-1234",
                "supply_area": "Area1",
                "description": "Item 1",
                "qty_per_box": 10.0,
            }
        ]
        mock_repo_class.return_value = mock_instance
        
        response = client.get("/pkmc/response/db")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["data"]) == 1
    
    @patch('modules.pkmc.api.routes.PKMCRepository')
    def test_get_from_db_with_limit(self, mock_repo_class, client):
        """Test GET /pkmc/response/db with limit parameter"""
        mock_instance = Mock()
        mock_instance.fetch_all.return_value = []
        mock_repo_class.return_value = mock_instance
        
        response = client.get("/pkmc/response/db", params={"limit": 100})
        
        # Verify limit was passed
        mock_instance.fetch_all.assert_called_with(100)
        assert response.status_code == 200
    
    @patch('modules.pkmc.api.routes.PKMCRepository')
    def test_get_from_db_error(self, mock_repo_class, client):
        """Test GET /pkmc/response/db with database error"""
        mock_instance = Mock()
        mock_instance.fetch_all.side_effect = Exception("Database error")
        mock_repo_class.return_value = mock_instance
        
        response = client.get("/pkmc/response/db")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
    
    @patch('modules.pkmc.api.routes.PKMCPipeline')
    @patch('modules.pkmc.api.routes.PKMCRepository')
    def test_upsert_success(self, mock_repo_class, mock_pipeline, client):
        """Test POST /pkmc/upsert endpoint"""
        # Mock pipeline
        mock_pipe_instance = Mock()
        mock_df = pl.DataFrame({
            "partnumber": ["PN-1234"],
            "supply_area": ["Area1"],
            "deposit_type": ["Type1"],
            "deposit_position": ["Pos1"],
            "description": ["Item 1"],
            "qty_per_box": [10.0],
            "qty_max_box": [50.0],
            "total_theoretical_qty": [100.0],
            "qty_for_restock": [20.0],
            "rack": ["Rack1"],
            "lb_balance": [25.0],
            "lb_balance_box": [2.5],
        })
        
        mock_pipe_instance.run.return_value.collect.return_value = mock_df
        mock_pipeline.return_value = mock_pipe_instance
        
        # Mock repository
        mock_repo_instance = Mock()
        mock_repo_instance.bulk_upsert.return_value = 1
        mock_repo_class.return_value = mock_repo_instance
        
        response = client.post("/pkmc/upsert", params={"batch_size": 10000})
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["rows"] == 1
        assert data["data"]["table"] == "pkmc"
    
    @patch('modules.pkmc.api.routes.PKMCPipeline')
    @patch('modules.pkmc.api.routes.PKMCRepository')
    def test_upsert_with_custom_batch_size(self, mock_repo_class, mock_pipeline, client):
        """Test POST /pkmc/upsert with custom batch size"""
        mock_pipe_instance = Mock()
        mock_df = pl.DataFrame({
            "partnumber": ["PN-1234"],
            "supply_area": ["Area1"],
            "deposit_type": ["Type1"],
            "deposit_position": ["Pos1"],
            "description": ["Item 1"],
            "qty_per_box": [10.0],
            "qty_max_box": [50.0],
            "total_theoretical_qty": [100.0],
            "qty_for_restock": [20.0],
            "rack": ["Rack1"],
            "lb_balance": [25.0],
            "lb_balance_box": [2.5],
        })
        
        mock_pipe_instance.run.return_value.collect.return_value = mock_df
        mock_pipeline.return_value = mock_pipe_instance
        
        mock_repo_instance = Mock()
        mock_repo_instance.bulk_upsert.return_value = 1
        mock_repo_class.return_value = mock_repo_instance
        
        response = client.post("/pkmc/upsert", params={"batch_size": 5000})
        
        # Verify batch size was passed
        assert mock_repo_instance.bulk_upsert.called
        call_args = mock_repo_instance.bulk_upsert.call_args
        assert call_args.args[1] == 5000
    
    @patch('modules.pkmc.api.routes.PKMCPipeline')
    @patch('modules.pkmc.api.routes.PKMCRepository')
    def test_upsert_error(self, mock_repo_class, mock_pipeline, client):
        """Test POST /pkmc/upsert with error"""
        mock_pipe_instance = Mock()
        mock_pipe_instance.run.side_effect = Exception("Pipeline failed")
        mock_pipeline.return_value = mock_pipe_instance
        
        response = client.post("/pkmc/upsert")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False


class TestPKMCResponseFormat:
    """Test suite for PKMC response format consistency"""
    
    def test_success_response_structure(self, client):
        """Test that success responses have correct structure"""
        with patch('modules.pkmc.api.routes.PKMCPipeline') as mock_pipeline:
            mock_instance = Mock()
            mock_df = pl.DataFrame({
                "partnumber": ["PN-1234"],
                "supply_area": ["Area1"],
                "deposit_type": ["Type1"],
                "deposit_position": ["Pos1"],
                "description": ["Item 1"],
                "qty_per_box": [10.0],
                "qty_max_box": [50.0],
                "total_theoretical_qty": [100.0],
                "qty_for_restock": [20.0],
                "rack": ["Rack1"],
                "lb_balance": [25.0],
                "lb_balance_box": [2.5],
            })
            mock_instance.run.return_value.head.return_value.collect.return_value = mock_df
            mock_pipeline.return_value = mock_instance
            
            response = client.get("/pkmc/response")
            data = response.json()
            
            # Verify response structure
            assert "success" in data
            assert "message" in data
            assert "data" in data
            assert "timestamp" in data
    
    def test_error_response_structure(self, client):
        """Test that error responses have correct structure"""
        with patch('modules.pkmc.api.routes.PKMCPipeline') as mock_pipeline:
            mock_instance = Mock()
            mock_instance.run.side_effect = Exception("Test error")
            mock_pipeline.return_value = mock_instance
            
            response = client.get("/pkmc/response")
            data = response.json()
            
            # Verify error response structure
            assert "success" in data
            assert data["success"] is False
            assert "message" in data
            assert "error" in data
            assert "timestamp" in data
