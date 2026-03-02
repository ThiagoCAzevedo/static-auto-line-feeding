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


class TestPK05Routes:
    """Test suite for PK05 API routes"""
    
    @patch('modules.pk05.api.routes.PK05Pipeline')
    def test_get_response_success(self, mock_pipeline, client):
        """Test GET /pk05/response endpoint"""
        # Mock the pipeline to return sample data
        mock_instance = Mock()
        mock_df = pl.DataFrame({
            "supply_area": ["Area1"],
            "deposit": ["LB01"],
            "responsible": ["John"],
            "discharge_point": ["P1"],
            "description": ["Item 1"],
            "takt": ["T001"],
        })
        
        mock_instance.run.return_value.head.return_value.collect.return_value = mock_df
        mock_pipeline.return_value = mock_instance
        
        # Make request
        response = client.get("/pk05/response", params={"limit": 50})
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "data" in data
        assert len(data["data"]) == 1
    
    @patch('modules.pk05.api.routes.PK05Pipeline')
    def test_get_response_with_limit(self, mock_pipeline, client):
        """Test GET /pk05/response with limit parameter"""
        mock_instance = Mock()
        mock_df = pl.DataFrame({
            "supply_area": ["Area1", "Area2"],
            "deposit": ["LB01", "LB01"],
            "responsible": ["John", "Jane"],
            "discharge_point": ["P1", "P2"],
            "description": ["Item 1", "Item 2"],
            "takt": ["T001", "T002"],
        })
        
        mock_instance.run.return_value.head.return_value.collect.return_value = mock_df
        mock_pipeline.return_value = mock_instance
        
        # Make request with limit
        response = client.get("/pk05/response", params={"limit": 10})
        
        assert response.status_code == 200
        assert mock_instance.run.return_value.head.called
    
    @patch('modules.pk05.api.routes.PK05Pipeline')
    def test_get_response_invalid_limit(self, mock_pipeline, client):
        """Test GET /pk05/response with invalid limit"""
        response = client.get("/pk05/response", params={"limit": 0})
        
        # Should fail validation
        assert response.status_code == 422
    
    @patch('modules.pk05.api.routes.PK05Pipeline')
    def test_get_response_limit_too_high(self, mock_pipeline, client):
        """Test GET /pk05/response with limit exceeding maximum"""
        response = client.get("/pk05/response", params={"limit": 2000})
        
        # Should fail validation (max is 1000)
        assert response.status_code == 422
    
    @patch('modules.pk05.api.routes.PK05Pipeline')
    def test_get_response_error(self, mock_pipeline, client):
        """Test GET /pk05/response with pipeline error"""
        mock_instance = Mock()
        mock_instance.run.side_effect = Exception("Pipeline error")
        mock_pipeline.return_value = mock_instance
        
        response = client.get("/pk05/response", params={"limit": 50})
        
        assert response.status_code == 200  # Error handler returns 200
        data = response.json()
        assert data["success"] is False
    
    @patch('modules.pk05.api.routes.PK05Repository')
    def test_get_from_db_success(self, mock_repo_class, client):
        """Test GET /pk05/response/db endpoint"""
        # Mock the repository
        mock_instance = Mock()
        mock_instance.fetch_all.return_value = [
            {
                "supply_area": "Area1",
                "deposit": "LB01",
                "description": "Item 1",
                "takt": "T001",
            }
        ]
        mock_repo_class.return_value = mock_instance
        
        response = client.get("/pk05/response/db")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["data"]) == 1
    
    @patch('modules.pk05.api.routes.PK05Repository')
    def test_get_from_db_with_limit(self, mock_repo_class, client):
        """Test GET /pk05/response/db with limit parameter"""
        mock_instance = Mock()
        mock_instance.fetch_all.return_value = []
        mock_repo_class.return_value = mock_instance
        
        response = client.get("/pk05/response/db", params={"limit": 100})
        
        # Verify limit was passed to fetch_all
        mock_instance.fetch_all.assert_called_with(100)
        assert response.status_code == 200
    
    @patch('modules.pk05.api.routes.PK05Repository')
    def test_get_from_db_error(self, mock_repo_class, client):
        """Test GET /pk05/response/db with database error"""
        mock_instance = Mock()
        mock_instance.fetch_all.side_effect = Exception("Database error")
        mock_repo_class.return_value = mock_instance
        
        response = client.get("/pk05/response/db")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
    
    @patch('modules.pk05.api.routes.PK05Pipeline')
    @patch('modules.pk05.api.routes.PK05Repository')
    def test_upsert_success(self, mock_repo_class, mock_pipeline, client):
        """Test POST /pk05/upsert endpoint"""
        # Mock pipeline
        mock_pipe_instance = Mock()
        mock_df = pl.DataFrame({
            "supply_area": ["Area1"],
            "deposit": ["LB01"],
            "responsible": ["John"],
            "discharge_point": ["P1"],
            "description": ["Item 1"],
            "takt": ["T001"],
        })
        mock_pipe_instance.run.return_value.collect.return_value = mock_df
        mock_pipeline.return_value = mock_pipe_instance
        
        # Mock repository
        mock_repo_instance = Mock()
        mock_repo_instance.bulk_upsert.return_value = 1
        mock_repo_class.return_value = mock_repo_instance
        
        response = client.post("/pk05/upsert", params={"batch_size": 10000})
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["rows"] == 1
        assert data["data"]["table"] == "pk05"
    
    @patch('modules.pk05.api.routes.PK05Pipeline')
    @patch('modules.pk05.api.routes.PK05Repository')
    def test_upsert_with_custom_batch_size(self, mock_repo_class, mock_pipeline, client):
        """Test POST /pk05/upsert with custom batch size"""
        mock_pipe_instance = Mock()
        mock_df = pl.DataFrame({
            "supply_area": ["Area1"],
            "deposit": ["LB01"],
            "responsible": ["John"],
            "discharge_point": ["P1"],
            "description": ["Item 1"],
            "takt": ["T001"],
        })
        mock_pipe_instance.run.return_value.collect.return_value = mock_df
        mock_pipeline.return_value = mock_pipe_instance
        
        mock_repo_instance = Mock()
        mock_repo_instance.bulk_upsert.return_value = 1
        mock_repo_class.return_value = mock_repo_instance
        
        response = client.post("/pk05/upsert", params={"batch_size": 5000})
        
        # Verify batch size was passed
        assert mock_repo_instance.bulk_upsert.called
        call_args = mock_repo_instance.bulk_upsert.call_args
        assert call_args.kwargs["batch_size"] == 5000 or call_args[0][1] == 5000
    
    @patch('modules.pk05.api.routes.PK05Pipeline')
    @patch('modules.pk05.api.routes.PK05Repository')
    def test_upsert_error(self, mock_repo_class, mock_pipeline, client):
        """Test POST /pk05/upsert with error"""
        mock_pipe_instance = Mock()
        mock_pipe_instance.run.side_effect = Exception("Pipeline failed")
        mock_pipeline.return_value = mock_pipe_instance
        
        response = client.post("/pk05/upsert")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False


class TestResponseFormat:
    """Test suite for response format consistency"""
    
    def test_success_response_structure(self, client):
        """Test that success responses have correct structure"""
        with patch('modules.pk05.api.routes.PK05Pipeline') as mock_pipeline:
            mock_instance = Mock()
            mock_df = pl.DataFrame({
                "supply_area": ["Area1"],
                "deposit": ["LB01"],
                "responsible": ["John"],
                "discharge_point": ["P1"],
                "description": ["Item 1"],
                "takt": ["T001"],
            })
            mock_instance.run.return_value.head.return_value.collect.return_value = mock_df
            mock_pipeline.return_value = mock_instance
            
            response = client.get("/pk05/response")
            data = response.json()
            
            # Verify response structure
            assert "success" in data
            assert "message" in data
            assert "data" in data
            assert "timestamp" in data
    
    def test_error_response_structure(self, client):
        """Test that error responses have correct structure"""
        with patch('modules.pk05.api.routes.PK05Pipeline') as mock_pipeline:
            mock_instance = Mock()
            mock_instance.run.side_effect = Exception("Test error")
            mock_pipeline.return_value = mock_instance
            
            response = client.get("/pk05/response")
            data = response.json()
            
            # Verify error response structure
            assert "success" in data
            assert data["success"] is False
            assert "message" in data
            assert "error" in data
            assert "timestamp" in data
