import pytest
from unittest.mock import Mock, patch, MagicMock
import polars as pl
from modules.pkmc.application.pipeline import PKMCPipeline
from modules.pkmc.infrastructure.loader import PKMCLoader
from modules.pkmc.infrastructure.cleaner import PKMCCleaner


class TestPKMCPipeline:
    """Test suite for PKMCPipeline"""
    
    def test_pipeline_initialization(self):
        """Test pipeline initialization"""
        with patch('modules.pkmc.application.pipeline.settings') as mock_settings:
            mock_settings.PKMC_PATH = "/path/to/file.xlsx"
            
            pipeline = PKMCPipeline()
            
            assert pipeline.file_path == "/path/to/file.xlsx"
    
    @patch('modules.pkmc.application.pipeline.PKMCLoader')
    @patch('modules.pkmc.application.pipeline.PKMCCleaner')
    def test_pipeline_run_success(self, mock_cleaner_class, mock_loader_class):
        """Test successful pipeline execution"""
        # Mock loader
        mock_loader = Mock()
        mock_df = pl.DataFrame({
            "Material": ["PN001"],
            "Área abastec.prod.": ["P01A"],
            "Nº circ.regul.": ["CIRC001"],
            "Tipo de depósito": ["B01"],
            "Posição no depósito": ["Pos1"],
            "Container": ["Container1"],
            "Texto breve de material": ["Item A"],
            "Norma de embalagem": ["Standard1"],
            "Quantidade Kanban": [10.0],
            "Posição de armazenamento": ["MAX 50"],
        }).lazy()
        
        mock_loader.create_df.return_value = mock_df
        mock_loader_class.return_value = mock_loader
        
        # Mock cleaner
        mock_cleaner = Mock()
        mock_cleaner.rename_columns.return_value = mock_df
        mock_cleaner.filter_columns.return_value = mock_df
        mock_cleaner.clean_columns.return_value = mock_df
        mock_cleaner.create_columns.return_value = mock_df
        mock_cleaner_class.return_value = mock_cleaner
        
        with patch('modules.pkmc.application.pipeline.settings') as mock_settings:
            mock_settings.PKMC_PATH = "/path/to/file.xlsx"
            
            pipeline = PKMCPipeline()
            result = pipeline.run()
            
            # Verify result is a LazyFrame
            assert isinstance(result, pl.LazyFrame)
    
    @patch('modules.pkmc.application.pipeline.PKMCLoader')
    def test_pipeline_run_file_error(self, mock_loader_class):
        """Test pipeline handling file loading error"""
        mock_loader = Mock()
        mock_loader.create_df.side_effect = FileNotFoundError("File not found")
        mock_loader_class.return_value = mock_loader
        
        with patch('modules.pkmc.application.pipeline.settings') as mock_settings:
            mock_settings.PKMC_PATH = "/nonexistent/file.xlsx"
            
            pipeline = PKMCPipeline()
            
            with pytest.raises(FileNotFoundError):
                pipeline.run()
    
    @patch('modules.pkmc.application.pipeline.PKMCLoader')
    @patch('modules.pkmc.application.pipeline.PKMCCleaner')
    def test_pipeline_run_cleaner_error(self, mock_cleaner_class, mock_loader_class):
        """Test pipeline handling cleaner error"""
        # Mock loader
        mock_loader = Mock()
        mock_df = pl.DataFrame({
            "Material": ["PN001"],
            "Área abastec.prod.": ["P01A"],
            "Nº circ.regul.": ["CIRC001"],
            "Tipo de depósito": ["B01"],
            "Posição no depósito": ["Pos1"],
            "Container": ["Container1"],
            "Texto breve de material": ["Item A"],
            "Norma de embalagem": ["Standard1"],
            "Quantidade Kanban": [10.0],
            "Posição de armazenamento": ["MAX 50"],
        }).lazy()
        
        mock_loader.create_df.return_value = mock_df
        mock_loader_class.return_value = mock_loader
        
        # Mock cleaner with error
        mock_cleaner = Mock()
        mock_cleaner.rename_columns.side_effect = Exception("Cleaner error")
        mock_cleaner_class.return_value = mock_cleaner
        
        with patch('modules.pkmc.application.pipeline.settings') as mock_settings:
            mock_settings.PKMC_PATH = "/path/to/file.xlsx"
            
            pipeline = PKMCPipeline()
            
            with pytest.raises(Exception) as exc_info:
                pipeline.run()
            
            assert "Cleaner error" in str(exc_info.value)


class TestPKMCPipelineIntegration:
    """Integration tests for the full PKMC pipeline"""
    
    @patch('modules.pkmc.application.pipeline.settings')
    def test_pipeline_full_flow(self, mock_settings, temp_excel_file):
        """Test full pipeline with real data"""
        mock_settings.PKMC_PATH = temp_excel_file
        
        # Create pipeline and run
        pipeline = PKMCPipeline()
        result = pipeline.run()
        
        # Verify result
        assert isinstance(result, pl.LazyFrame)
        
        # Collect and verify data structure
        df = result.collect()
        expected_columns = [
            "partnumber", "supply_area", "num_reg_circ", "deposit_type",
            "deposit_position", "container", "description", "pack_standard",
            "qty_per_box", "qty_max_box"
        ]
        
        for col in expected_columns:
            assert col in df.columns
    
    @patch('modules.pkmc.application.pipeline.settings')
    def test_pipeline_data_transformation(self, mock_settings, temp_excel_file):
        """Test that pipeline correctly transforms data"""
        mock_settings.PKMC_PATH = temp_excel_file
        
        pipeline = PKMCPipeline()
        result = pipeline.run().collect()
        
        # Verify transformations
        # All should have B01 deposit_type after filtering
        assert all(deposit_type == "B01" or len(result) == 0 for deposit_type in result["deposit_type"])


class TestPKMCPipelineEdgeCases:
    """Test edge cases and error conditions"""
    
    @patch('modules.pkmc.application.pipeline.PKMCLoader')
    @patch('modules.pkmc.application.pipeline.PKMCCleaner')
    def test_pipeline_with_empty_dataframe(self, mock_cleaner_class, mock_loader_class):
        """Test pipeline with empty input"""
        # Mock loader with empty dataframe
        mock_loader = Mock()
        mock_df = pl.DataFrame({
            "Material": [],
            "Área abastec.prod.": [],
            "Nº circ.regul.": [],
            "Tipo de depósito": [],
            "Posição no depósito": [],
            "Container": [],
            "Texto breve de material": [],
            "Norma de embalagem": [],
            "Quantidade Kanban": [],
            "Posição de armazenamento": [],
        }).lazy()
        
        mock_loader.create_df.return_value = mock_df
        mock_loader_class.return_value = mock_loader
        
        # Mock cleaner
        mock_cleaner = Mock()
        empty_renamed = mock_df.select([])
        mock_cleaner.rename_columns.return_value = empty_renamed
        mock_cleaner.filter_columns.return_value = empty_renamed
        mock_cleaner.clean_columns.return_value = empty_renamed
        mock_cleaner.create_columns.return_value = empty_renamed
        mock_cleaner_class.return_value = mock_cleaner
        
        with patch('modules.pkmc.application.pipeline.settings') as mock_settings:
            mock_settings.PKMC_PATH = "/path/to/empty.xlsx"
            
            pipeline = PKMCPipeline()
            result = pipeline.run()
            
            # Should handle empty dataframe gracefully
            assert isinstance(result, pl.LazyFrame)
    
    @patch('modules.pkmc.application.pipeline.PKMCLoader')
    @patch('modules.pkmc.application.pipeline.PKMCCleaner')
    def test_pipeline_cleaner_called_in_order(self, mock_cleaner_class, mock_loader_class):
        """Test that cleaner methods are called in correct order"""
        # Setup mocks
        mock_loader = Mock()
        mock_df = pl.DataFrame({
            "Material": ["PN001"],
            "Área abastec.prod.": ["P01A"],
            "Nº circ.regul.": ["CIRC001"],
            "Tipo de depósito": ["B01"],
            "Posição no depósito": ["Pos1"],
            "Container": ["Container1"],
            "Texto breve de material": ["Item A"],
            "Norma de embalagem": ["Standard1"],
            "Quantidade Kanban": [10.0],
            "Posição de armazenamento": ["MAX 50"],
        }).lazy()
        
        mock_loader.create_df.return_value = mock_df
        mock_loader_class.return_value = mock_loader
        
        # Create cleaner mock that tracks call order
        mock_cleaner = Mock()
        mock_cleaner.rename_columns.return_value = mock_df
        mock_cleaner.filter_columns.return_value = mock_df
        mock_cleaner.clean_columns.return_value = mock_df
        mock_cleaner.create_columns.return_value = mock_df
        mock_cleaner_class.return_value = mock_cleaner
        
        with patch('modules.pkmc.application.pipeline.settings') as mock_settings:
            mock_settings.PKMC_PATH = "/path/to/file.xlsx"
            
            pipeline = PKMCPipeline()
            pipeline.run()
            
            # Verify call order: rename -> filter -> clean -> create
            expected_calls = [
                "rename_columns",
                "filter_columns",
                "clean_columns",
                "create_columns",
            ]
            
            actual_calls = [call[0] for call in mock_cleaner.method_calls]
            
            # Check that methods were called in correct order
            assert actual_calls[0] == "rename_columns"
            assert actual_calls[1] == "filter_columns"
            assert actual_calls[2] == "clean_columns"
            assert actual_calls[3] == "create_columns"


class TestPKMCPipelineSteps:
    """Test individual pipeline steps and their interactions"""
    
    @patch('modules.pkmc.application.pipeline.PKMCLoader')
    @patch('modules.pkmc.application.pipeline.PKMCCleaner')
    def test_pipeline_step_rename(self, mock_cleaner_class, mock_loader_class):
        """Test rename step is applied correctly"""
        mock_loader = Mock()
        mock_df = pl.DataFrame({
            "Material": ["PN001"],
            "Área abastec.prod.": ["P01A"],
            "Nº circ.regul.": ["CIRC001"],
            "Tipo de depósito": ["B01"],
            "Posição no depósito": ["Pos1"],
            "Container": ["Container1"],
            "Texto breve de material": ["Item A"],
            "Norma de embalagem": ["Standard1"],
            "Quantidade Kanban": [10.0],
            "Posição de armazenamento": ["MAX 50"],
        }).lazy()
        
        mock_loader.create_df.return_value = mock_df
        mock_loader_class.return_value = mock_loader
        
        mock_cleaner = Mock()
        mock_cleaner.rename_columns.return_value = mock_df
        mock_cleaner.filter_columns.return_value = mock_df
        mock_cleaner.clean_columns.return_value = mock_df
        mock_cleaner.create_columns.return_value = mock_df
        mock_cleaner_class.return_value = mock_cleaner
        
        with patch('modules.pkmc.application.pipeline.settings') as mock_settings:
            mock_settings.PKMC_PATH = "/path/to/file.xlsx"
            
            pipeline = PKMCPipeline()
            pipeline.run()
            
            # Verify rename_columns was called first
            assert mock_cleaner.rename_columns.called
            assert mock_cleaner.rename_columns.call_count == 1
    
    @patch('modules.pkmc.application.pipeline.PKMCLoader')
    @patch('modules.pkmc.application.pipeline.PKMCCleaner')
    def test_pipeline_step_filter(self, mock_cleaner_class, mock_loader_class):
        """Test filter step is applied correctly"""
        mock_loader = Mock()
        mock_df = pl.DataFrame({
            "Material": ["PN001"],
            "deposit_type": ["B01"],
        }).lazy()
        
        mock_loader.create_df.return_value = mock_df
        mock_loader_class.return_value = mock_loader
        
        mock_cleaner = Mock()
        mock_cleaner.rename_columns.return_value = mock_df
        mock_cleaner.filter_columns.return_value = mock_df
        mock_cleaner.clean_columns.return_value = mock_df
        mock_cleaner.create_columns.return_value = mock_df
        mock_cleaner_class.return_value = mock_cleaner
        
        with patch('modules.pkmc.application.pipeline.settings') as mock_settings:
            mock_settings.PKMC_PATH = "/path/to/file.xlsx"
            
            pipeline = PKMCPipeline()
            pipeline.run()
            
            # Verify filter_columns was called after rename
            assert mock_cleaner.filter_columns.called
            assert mock_cleaner.filter_columns.call_count == 1
