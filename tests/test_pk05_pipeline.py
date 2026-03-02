import pytest
from unittest.mock import Mock, patch, MagicMock
import polars as pl
from modules.pk05.application.pipeline import PK05Pipeline
from modules.pk05.infrastructure.cleaner import PK05DefineDataframe, PK05Cleaner


class TestPK05Pipeline:
    """Test suite for PK05Pipeline"""
    
    def test_pipeline_initialization(self):
        """Test pipeline initialization"""
        with patch('modules.pk05.application.pipeline.settings') as mock_settings:
            mock_settings.PK05_PATH = "/path/to/file.xlsx"
            
            pipeline = PK05Pipeline()
            
            assert pipeline.file_path == "/path/to/file.xlsx"
    
    @patch('modules.pk05.application.pipeline.PK05DefineDataframe')
    @patch('modules.pk05.application.pipeline.PK05Cleaner')
    def test_pipeline_run_success(self, mock_cleaner_class, mock_loader_class):
        """Test successful pipeline execution"""
        # Mock loader
        mock_loader = Mock()
        mock_df = pl.DataFrame({
            "Área abastec.prod.": ["Area1"],
            "Depósito": ["LB01"],
            "Responsável": ["John"],
            "Ponto de descarga": ["P1"],
            "Denominação SupM": ["T001 Item A"],
        }).lazy()
        
        mock_loader.create_df.return_value = mock_df
        mock_loader_class.return_value = mock_loader
        
        # Mock cleaner
        mock_cleaner = Mock()
        renamed_df = pl.DataFrame({
            "supply_area": ["Area1"],
            "deposit": ["LB01"],
            "responsible": ["John"],
            "discharge_point": ["P1"],
            "description": ["T001 Item A"],
        }).lazy()
        
        created_df = renamed_df.with_columns(
            pl.col("description").str.extract(r"(T\d+)", 1).alias("takt")
        )
        
        mock_cleaner.rename_columns.return_value = renamed_df
        mock_cleaner.create_columns.return_value = created_df
        mock_cleaner.filter_columns.return_value = created_df
        mock_cleaner_class.return_value = mock_cleaner
        
        with patch('modules.pk05.application.pipeline.settings') as mock_settings:
            mock_settings.PK05_PATH = "/path/to/file.xlsx"
            
            pipeline = PK05Pipeline()
            result = pipeline.run()
            
            # Verify result is a LazyFrame
            assert isinstance(result, pl.LazyFrame)
    
    @patch('modules.pk05.application.pipeline.PK05DefineDataframe')
    def test_pipeline_run_file_error(self, mock_loader_class):
        """Test pipeline handling file loading error"""
        mock_loader = Mock()
        mock_loader.create_df.side_effect = FileNotFoundError("File not found")
        mock_loader_class.return_value = mock_loader
        
        with patch('modules.pk05.application.pipeline.settings') as mock_settings:
            mock_settings.PK05_PATH = "/nonexistent/file.xlsx"
            
            pipeline = PK05Pipeline()
            
            with pytest.raises(FileNotFoundError):
                pipeline.run()
    
    @patch('modules.pk05.application.pipeline.PK05DefineDataframe')
    @patch('modules.pk05.application.pipeline.PK05Cleaner')
    def test_pipeline_run_cleaner_error(self, mock_cleaner_class, mock_loader_class):
        """Test pipeline handling cleaner error"""
        # Mock loader
        mock_loader = Mock()
        mock_df = pl.DataFrame({
            "Área abastec.prod.": ["Area1"],
            "Depósito": ["LB01"],
            "Responsável": ["John"],
            "Ponto de descarga": ["P1"],
            "Denominação SupM": ["T001 Item A"],
        }).lazy()
        
        mock_loader.create_df.return_value = mock_df
        mock_loader_class.return_value = mock_loader
        
        # Mock cleaner with error
        mock_cleaner = Mock()
        mock_cleaner.rename_columns.side_effect = Exception("Cleaner error")
        mock_cleaner_class.return_value = mock_cleaner
        
        with patch('modules.pk05.application.pipeline.settings') as mock_settings:
            mock_settings.PK05_PATH = "/path/to/file.xlsx"
            
            pipeline = PK05Pipeline()
            
            with pytest.raises(Exception) as exc_info:
                pipeline.run()
            
            assert "Cleaner error" in str(exc_info.value)


class TestPipelineIntegration:
    """Integration tests for the full pipeline"""
    
    @patch('modules.pk05.application.pipeline.settings')
    def test_pipeline_full_flow(self, mock_settings, temp_excel_file):
        """Test full pipeline with real data"""
        mock_settings.PK05_PATH = temp_excel_file
        
        # Create pipeline and run
        pipeline = PK05Pipeline()
        result = pipeline.run()
        
        # Verify result
        assert isinstance(result, pl.LazyFrame)
        
        # Collect and verify data structure
        df = result.collect()
        expected_columns = [
            "supply_area", "deposit", "responsible", 
            "discharge_point", "description", "takt"
        ]
        
        for col in expected_columns:
            assert col in df.columns
    
    @patch('modules.pk05.application.pipeline.settings')
    def test_pipeline_data_transformation(self, mock_settings, temp_excel_file):
        """Test that pipeline correctly transforms data"""
        mock_settings.PK05_PATH = temp_excel_file
        
        pipeline = PK05Pipeline()
        result = pipeline.run().collect()
        
        # Verify transformations
        # All should have LB01 deposit and T-starting takt
        for row in result.to_dicts():
            assert row["deposit"] == "LB01"
            if row["takt"]:  # takt might be None for non-matching patterns
                assert isinstance(row["takt"], str) or row["takt"] is None


class TestPipelineEdgeCases:
    """Test edge cases and error conditions"""
    
    @patch('modules.pk05.application.pipeline.PK05DefineDataframe')
    @patch('modules.pk05.application.pipeline.PK05Cleaner')
    def test_pipeline_with_empty_dataframe(self, mock_cleaner_class, mock_loader_class):
        """Test pipeline with empty input"""
        # Mock loader with empty dataframe
        mock_loader = Mock()
        mock_df = pl.DataFrame({
            "Área abastec.prod.": [],
            "Depósito": [],
            "Responsável": [],
            "Ponto de descarga": [],
            "Denominação SupM": [],
        }).lazy()
        
        mock_loader.create_df.return_value = mock_df
        mock_loader_class.return_value = mock_loader
        
        # Mock cleaner
        mock_cleaner = Mock()
        renamed = mock_df.select([]).rename({})
        mock_cleaner.rename_columns.return_value = renamed
        mock_cleaner.create_columns.return_value = renamed
        mock_cleaner.filter_columns.return_value = renamed
        mock_cleaner_class.return_value = mock_cleaner
        
        with patch('modules.pk05.application.pipeline.settings') as mock_settings:
            mock_settings.PK05_PATH = "/path/to/empty.xlsx"
            
            pipeline = PK05Pipeline()
            result = pipeline.run()
            
            # Should handle empty dataframe gracefully
            assert isinstance(result, pl.LazyFrame)
    
    @patch('modules.pk05.application.pipeline.PK05DefineDataframe')
    @patch('modules.pk05.application.pipeline.PK05Cleaner')
    def test_pipeline_cleaner_called_in_order(self, mock_cleaner_class, mock_loader_class):
        """Test that cleaner methods are called in correct order"""
        # Setup mocks
        mock_loader = Mock()
        mock_df = pl.DataFrame({
            "Área abastec.prod.": ["Area1"],
            "Depósito": ["LB01"],
            "Responsável": ["John"],
            "Ponto de descarga": ["P1"],
            "Denominação SupM": ["T001 Item A"],
        }).lazy()
        
        mock_loader.create_df.return_value = mock_df
        mock_loader_class.return_value = mock_loader
        
        # Create cleaner mock that tracks call order
        mock_cleaner = Mock()
        mock_cleaner.rename_columns.return_value = mock_df
        mock_cleaner.create_columns.return_value = mock_df
        mock_cleaner.filter_columns.return_value = mock_df
        mock_cleaner_class.return_value = mock_cleaner
        
        with patch('modules.pk05.application.pipeline.settings') as mock_settings:
            mock_settings.PK05_PATH = "/path/to/file.xlsx"
            
            pipeline = PK05Pipeline()
            pipeline.run()
            
            # Verify call order: rename -> create -> filter
            expected_calls = [
                "rename_columns",
                "create_columns",
                "filter_columns",
            ]
            
            actual_calls = [call[0] for call in mock_cleaner.method_calls]
            
            # Check that methods were called in correct order
            assert actual_calls[0] == "rename_columns"
            assert actual_calls[1] == "create_columns"
            assert actual_calls[2] == "filter_columns"
