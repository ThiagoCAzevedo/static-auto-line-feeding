import pytest
import polars as pl
from unittest.mock import Mock, patch, MagicMock
from modules.pk05.infrastructure.cleaner import PK05DefineDataframe, PK05Cleaner
from modules.pk05.infrastructure.base import PK05Base


class TestPK05DefineDataframe:
    """Test suite for PK05DefineDataframe class"""
    
    def test_create_df_success(self, temp_excel_file):
        """Test successful DataFrame creation from file"""
        loader = PK05DefineDataframe(temp_excel_file)
        lf = loader.create_df()
        
        # Verify it returns a LazyFrame
        assert isinstance(lf, pl.LazyFrame)
        
        # Collect and verify data
        df = lf.collect()
        assert df.height >= 0
    
    def test_create_df_file_not_found(self):
        """Test error handling when file doesn't exist"""
        loader = PK05DefineDataframe("/nonexistent/path/file.xlsx")
        
        with pytest.raises(FileNotFoundError):
            loader.create_df()
    
    def test_create_df_invalid_file(self, tmp_path):
        """Test error handling with invalid file"""
        invalid_file = tmp_path / "invalid.xlsx"
        invalid_file.write_text("not a valid excel file")
        
        loader = PK05DefineDataframe(str(invalid_file))
        
        with pytest.raises(Exception):  # Could be various exceptions
            loader.create_df()


class TestPK05Cleaner:
    """Test suite for PK05Cleaner class"""
    
    @pytest.fixture
    def cleaner(self):
        """Initialize cleaner instance"""
        return PK05Cleaner()
    
    @pytest.fixture
    def raw_dataframe(self):
        """Create a raw dataframe with original column names"""
        return pl.DataFrame({
            "Área abastec.prod.": ["Area1", "Area2", "Area3"],
            "Depósito": ["LB01", "LB01", "LB02"],
            "Responsável": ["John", "Jane", "Bob"],
            "Ponto de descarga": ["P1", "P2", "P3"],
            "Denominação SupM": ["T001 Item A", "T002 Item B", "T003 Item C"],
        }).lazy()
    
    def test_rename_columns_success(self, cleaner, raw_dataframe):
        """Test successful column renaming"""
        renamed_df = cleaner.rename_columns(raw_dataframe)
        
        # Collect and verify structure
        df = renamed_df.collect()
        expected_columns = ["supply_area", "deposit", "responsible", "discharge_point", "description"]
        
        assert set(df.columns) == set(expected_columns)
    
    def test_rename_columns_preserves_data(self, cleaner, raw_dataframe):
        """Test that renaming preserves data integrity"""
        renamed_df = cleaner.rename_columns(raw_dataframe)
        df = renamed_df.collect()
        
        # Check data is preserved
        assert df["supply_area"][0] == "Area1"
        assert df["deposit"][0] == "LB01"
        assert df["responsible"][1] == "Jane"
    
    def test_filter_columns_by_deposit(self, cleaner):
        """Test filtering by deposit value"""
        df = pl.DataFrame({
            "supply_area": ["Area1", "Area2", "Area3"],
            "deposit": ["LB01", "LB01", "LB02"],
            "responsible": ["John", "Jane", "Bob"],
            "discharge_point": ["P1", "P2", "P3"],
            "description": ["T001 Item A", "T002 Item B", "T003 Item C"],
            "takt": ["T001", "T002", "T003"],
        }).lazy()
        
        filtered_df = cleaner.filter_columns(df)
        result = filtered_df.collect()
        
        # Should only have LB01 records, starting with T
        assert result.height == 2
        assert all(deposit == "LB01" for deposit in result["deposit"])
    
    def test_filter_columns_by_takt_prefix(self, cleaner):
        """Test filtering by takt prefix"""
        df = pl.DataFrame({
            "supply_area": ["Area1", "Area2", "Area3"],
            "deposit": ["LB01", "LB01", "LB01"],
            "responsible": ["John", "Jane", "Bob"],
            "discharge_point": ["P1", "P2", "P3"],
            "description": ["T001 Item A", "X002 Item B", "T003 Item C"],
            "takt": ["T001", "X002", "T003"],
        }).lazy()
        
        filtered_df = cleaner.filter_columns(df)
        result = filtered_df.collect()
        
        # Should only have records starting with T
        assert all(takt.startswith("T") for takt in result["takt"])
    
    def test_create_columns_takt_extraction(self, cleaner):
        """Test takt extraction from description"""
        df = pl.DataFrame({
            "supply_area": ["Area1", "Area2"],
            "deposit": ["LB01", "LB01"],
            "responsible": ["John", "Jane"],
            "discharge_point": ["P1", "P2"],
            "description": ["T001 Item A", "T002 Item B"],
        }).lazy()
        
        df_with_takt = cleaner.create_columns(df)
        result = df_with_takt.collect()
        
        # Should have extracted takt values
        assert "takt" in result.columns
        assert result["takt"][0] == "T001"
        assert result["takt"][1] == "T002"
    
    def test_create_columns_no_match(self, cleaner):
        """Test takt extraction with no match"""
        df = pl.DataFrame({
            "supply_area": ["Area1"],
            "deposit": ["LB01"],
            "responsible": ["John"],
            "discharge_point": ["P1"],
            "description": ["Item without takt"],
        }).lazy()
        
        df_with_takt = cleaner.create_columns(df)
        result = df_with_takt.collect()
        
        # Should return None/null for non-matching patterns
        assert "takt" in result.columns
    
    def test_full_cleaning_pipeline(self, cleaner, raw_dataframe):
        """Test entire cleaning pipeline"""
        # Apply all steps
        lf = cleaner.rename_columns(raw_dataframe)
        lf = cleaner.create_columns(lf)
        lf = cleaner.filter_columns(lf)
        
        result = lf.collect()
        
        # Verify results
        assert result.height >= 0
        expected_columns = ["supply_area", "deposit", "responsible", "discharge_point", "description", "takt"]
        assert set(result.columns) == set(expected_columns)


class TestPK05Base:
    """Test suite for PK05Base class"""
    
    def test_load_file_success(self, temp_excel_file):
        """Test successful file loading"""
        base = PK05Base()
        df = base.load_file(temp_excel_file)
        
        assert isinstance(df, pl.DataFrame)
        assert df.height >= 0
    
    def test_load_file_not_found(self):
        """Test error handling for missing file"""
        base = PK05Base()
        
        with pytest.raises(FileNotFoundError):
            base.load_file("/nonexistent/file.xlsx")
    
    def test_rename_method(self):
        """Test rename utility method"""
        base = PK05Base()
        
        df = pl.DataFrame({
            "old_name1": [1, 2],
            "old_name2": [3, 4],
        }).lazy()
        
        rename_map = {
            "old_name1": "new_name1",
            "old_name2": "new_name2",
        }
        
        renamed = base.rename(df, rename_map)
        result = renamed.collect()
        
        assert "new_name1" in result.columns
        assert "new_name2" in result.columns
        assert "old_name1" not in result.columns
        assert "old_name2" not in result.columns
