import pytest
import polars as pl
from unittest.mock import Mock, patch, MagicMock
from modules.pkmc.infrastructure.loader import PKMCLoader
from modules.pkmc.infrastructure.cleaner import PKMCCleaner
from modules.pkmc.infrastructure.base import PKMCBase


class TestPKMCLoader:
    """Test suite for PKMCLoader class"""
    
    def test_create_df_success(self, temp_excel_file):
        """Test successful DataFrame creation from file"""
        loader = PKMCLoader(temp_excel_file)
        lf = loader.create_df()
        
        # Verify it returns a LazyFrame
        assert isinstance(lf, pl.LazyFrame)
        
        # Collect and verify data
        df = lf.collect()
        assert df.height >= 0
    
    def test_create_df_file_not_found(self):
        """Test error handling when file doesn't exist"""
        loader = PKMCLoader("/nonexistent/path/file.xlsx")
        
        with pytest.raises(FileNotFoundError):
            loader.create_df()
    
    def test_create_df_invalid_file(self, tmp_path):
        """Test error handling with invalid file"""
        invalid_file = tmp_path / "invalid.xlsx"
        invalid_file.write_text("not a valid excel file")
        
        loader = PKMCLoader(str(invalid_file))
        
        with pytest.raises(Exception):
            loader.create_df()


class TestPKMCCleaner:
    """Test suite for PKMCCleaner class"""
    
    @pytest.fixture
    def cleaner(self):
        """Initialize cleaner instance"""
        return PKMCCleaner()
    
    @pytest.fixture
    def raw_dataframe(self):
        """Create a raw dataframe with original PKMC column names"""
        return pl.DataFrame({
            "Material": ["PN001", "PN002", "PN003"],
            "Área abastec.prod.": ["P01A", "P02B", "P03C"],
            "Nº circ.regul.": ["CIRC001", "CIRC002", "CIRC003"],
            "Tipo de depósito": ["B01", "B01", "B02"],
            "Posição no depósito": ["Pos1", "Pos2", "Pos3"],
            "Container": ["Container1", "Container2", "Container3"],
            "Texto breve de material": ["Item A", "Item B", "Item C"],
            "Norma de embalagem": ["Standard1", "Standard2", "Standard3"],
            "Quantidade Kanban": [10.0, 15.0, 20.0],
            "Posição de armazenamento": ["MAX 50", "MAX 60", "MAX 70"],
        }).lazy()
    
    def test_rename_columns_success(self, cleaner, raw_dataframe):
        """Test successful column renaming"""
        renamed_df = cleaner.rename_columns(raw_dataframe)
        
        # Collect and verify structure
        df = renamed_df.collect()
        expected_columns = [
            "partnumber", "supply_area", "num_reg_circ", "deposit_type",
            "deposit_position", "container", "description", "pack_standard",
            "qty_per_box", "qty_max_box"
        ]
        
        assert set(df.columns) == set(expected_columns)
    
    def test_rename_columns_preserves_data(self, cleaner, raw_dataframe):
        """Test that renaming preserves data integrity"""
        renamed_df = cleaner.rename_columns(raw_dataframe)
        df = renamed_df.collect()
        
        # Check data is preserved
        assert df["partnumber"][0] == "PN001"
        assert df["supply_area"][0] == "P01A"
        assert df["deposit_type"][1] == "B01"
    
    def test_filter_columns_by_deposit_type(self, cleaner):
        """Test filtering by deposit_type value"""
        df = pl.DataFrame({
            "partnumber": ["PN001", "PN002", "PN003"],
            "supply_area": ["P01", "P02", "P03"],
            "num_reg_circ": ["C1", "C2", "C3"],
            "deposit_type": ["B01", "B01", "B02"],
            "deposit_position": ["Pos1", "Pos2", "Pos3"],
            "container": ["C1", "C2", "C3"],
            "description": ["Item A", "Item B", "Item C"],
            "pack_standard": ["S1", "S2", "S3"],
            "qty_per_box": [10.0, 15.0, 20.0],
            "qty_max_box": [50, 60, 70],
        }).lazy()
        
        filtered_df = cleaner.filter_columns(df)
        result = filtered_df.collect()
        
        # Should only have B01 records
        assert result.height == 2
        assert all(deposit_type == "B01" for deposit_type in result["deposit_type"])
    
    def test_clean_columns_qty_max_box(self, cleaner):
        """Test qty_max_box cleaning and conversion"""
        df = pl.DataFrame({
            "partnumber": ["PN-001", "PN-002"],
            "supply_area": ["P01", "P02"],
            "num_reg_circ": ["C1", "C2"],
            "deposit_type": ["B01", "B01"],
            "deposit_position": ["Pos1", "Pos2"],
            "container": ["C1", "C2"],
            "description": ["Item A", "Item B"],
            "pack_standard": ["S1", "S2"],
            "qty_per_box": [10.0, 15.0],
            "qty_max_box": ["MAX 50", "MAX 60"],
        }).lazy()
        
        cleaned_df = cleaner.clean_columns(df)
        result = cleaned_df.collect()
        
        # Verify qty_max_box was cleaned
        assert result["qty_max_box"][0] == 50
        assert result["qty_max_box"][1] == 60
    
    def test_clean_columns_partnumber(self, cleaner):
        """Test partnumber cleaning (whitespace and special chars)"""
        df = pl.DataFrame({
            "partnumber": ["PN  001", "PN.002"],
            "supply_area": ["P01", "P02"],
            "num_reg_circ": ["C1", "C2"],
            "deposit_type": ["B01", "B01"],
            "deposit_position": ["Pos1", "Pos2"],
            "container": ["C1", "C2"],
            "description": ["Item A", "Item B"],
            "pack_standard": ["S1", "S2"],
            "qty_per_box": [10.0, 15.0],
            "qty_max_box": [50, 60],
        }).lazy()
        
        cleaned_df = cleaner.clean_columns(df)
        result = cleaned_df.collect()
        
        # Verify partnumber was cleaned (spaces removed, uppercase)
        assert result["partnumber"][0] == "PN001"
        assert result["partnumber"][1] == "PN002"
    
    def test_create_columns_calculations(self, cleaner):
        """Test column creation with calculations"""
        df = pl.DataFrame({
            "partnumber": ["PN001", "PN002"],
            "supply_area": ["P01A", "P02B"],
            "num_reg_circ": ["C1", "C2"],
            "deposit_type": ["B01", "B01"],
            "deposit_position": ["Pos1", "Pos2"],
            "container": ["C1", "C2"],
            "description": ["Item A", "Item B"],
            "pack_standard": ["S1", "S2"],
            "qty_per_box": [10.0, 20.0],
            "qty_max_box": [50, 60],
        }).lazy()
        
        df_with_cols = cleaner.create_columns(df)
        result = df_with_cols.collect()
        
        # Verify calculated columns exist and values are correct
        assert "total_theoretical_qty" in result.columns
        assert "qty_for_restock" in result.columns
        assert "lb_balance" in result.columns
        assert "rack" in result.columns
        assert "lb_balance_box" in result.columns
        
        # Verify calculations
        assert result["total_theoretical_qty"][0] == 500.0  # 10 * 50
        assert result["total_theoretical_qty"][1] == 1200.0  # 20 * 60
    
    def test_create_columns_rack_extraction(self, cleaner):
        """Test rack extraction from supply_area"""
        df = pl.DataFrame({
            "partnumber": ["PN001"],
            "supply_area": ["P01A"],
            "num_reg_circ": ["C1"],
            "deposit_type": ["B01"],
            "deposit_position": ["Pos1"],
            "container": ["C1"],
            "description": ["Item A"],
            "pack_standard": ["S1"],
            "qty_per_box": [10.0],
            "qty_max_box": [50],
        }).lazy()
        
        df_with_cols = cleaner.create_columns(df)
        result = df_with_cols.collect()
        
        # Should extract rack like P01A
        assert result["rack"][0] == "P01A"
    
    def test_full_cleaning_pipeline(self, cleaner, raw_dataframe):
        """Test entire cleaning pipeline"""
        lf = cleaner.rename_columns(raw_dataframe)
        lf = cleaner.filter_columns(lf)
        lf = cleaner.clean_columns(lf)
        lf = cleaner.create_columns(lf)
        
        result = lf.collect()
        
        # Verify results
        assert result.height == 2  # Only B01 records
        expected_columns = [
            "partnumber", "supply_area", "num_reg_circ", "deposit_type",
            "deposit_position", "container", "description", "pack_standard",
            "qty_per_box", "qty_max_box", "total_theoretical_qty",
            "qty_for_restock", "lb_balance", "rack", "lb_balance_box"
        ]
        assert set(result.columns) == set(expected_columns)


class TestPKMCBase:
    """Test suite for PKMCBase class"""
    
    def test_load_file_success(self, temp_excel_file):
        """Test successful file loading"""
        base = PKMCBase()
        df = base.load_file(temp_excel_file)
        
        assert isinstance(df, pl.DataFrame)
        assert df.height >= 0
    
    def test_load_file_not_found(self):
        """Test error handling for missing file"""
        base = PKMCBase()
        
        with pytest.raises(FileNotFoundError):
            base.load_file("/nonexistent/file.xlsx")
    
    def test_rename_method(self):
        """Test rename utility method"""
        base = PKMCBase()
        
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
