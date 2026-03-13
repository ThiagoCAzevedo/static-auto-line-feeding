import pytest
from pydantic import ValidationError
from modules.pkmc.application.dtos import PKMCRecordDTO, PKMCBulkCreateDTO


class TestPKMCRecordDTO:
    """Test suite for PKMCRecordDTO"""
    
    @pytest.fixture
    def valid_record_data(self):
        """Valid record data"""
        return {
            "partnumber": "PN-1234",
            "supply_area": "P01A",
            "num_reg_circ": "CIRC-001",
            "deposit_type": "B01",
            "deposit_position": "Pos1",
            "container": "Container1",
            "description": "Item Description",
            "pack_standard": "Standard1",
            "qty_per_box": 10.0,
            "qty_max_box": 50.0,
            "total_theoretical_qty": 500.0,
            "qty_for_restock": 490.0,
            "rack": "Rack1",
            "lb_balance": 2000.0,
            "lb_balance_box": 200.0,
        }
    
    def test_valid_record_creation(self, valid_record_data):
        """Test creating a valid record"""
        record = PKMCRecordDTO(**valid_record_data)
        
        assert record.partnumber == "PN-1234"
        assert record.supply_area == "P01A"
        assert record.qty_per_box == 10.0
        assert record.qty_max_box == 50.0
    
    def test_qty_per_box_must_be_positive(self, valid_record_data):
        """Test that qty_per_box must be greater than 0"""
        valid_record_data["qty_per_box"] = 0
        
        with pytest.raises(ValidationError):
            PKMCRecordDTO(**valid_record_data)
    
    def test_qty_max_box_must_be_positive(self, valid_record_data):
        """Test that qty_max_box must be greater than 0"""
        valid_record_data["qty_max_box"] = 0
        
        with pytest.raises(ValidationError):
            PKMCRecordDTO(**valid_record_data)
    
    def test_qty_per_box_cannot_be_negative(self, valid_record_data):
        """Test that qty_per_box cannot be negative"""
        valid_record_data["qty_per_box"] = -5.0
        
        with pytest.raises(ValidationError):
            PKMCRecordDTO(**valid_record_data)
    
    def test_qty_max_box_cannot_be_negative(self, valid_record_data):
        """Test that qty_max_box cannot be negative"""
        valid_record_data["qty_max_box"] = -5.0
        
        with pytest.raises(ValidationError):
            PKMCRecordDTO(**valid_record_data)
    
    def test_missing_required_field(self, valid_record_data):
        """Test that required fields cannot be missing"""
        del valid_record_data["partnumber"]
        
        with pytest.raises(ValidationError):
            PKMCRecordDTO(**valid_record_data)
    
    def test_float_fields(self, valid_record_data):
        """Test that float fields accept numeric values"""
        valid_record_data["qty_per_box"] = 10
        valid_record_data["qty_max_box"] = "50"  # Should be coercible
        
        record = PKMCRecordDTO(**valid_record_data)
        
        assert isinstance(record.qty_per_box, float)
        assert isinstance(record.qty_max_box, float)
    
    def test_from_attributes_config(self, valid_record_data):
        """Test that model can be created from ORM objects"""
        record = PKMCRecordDTO(**valid_record_data)
        
        # Should be convertible to dict and back
        record_dict = record.model_dump()
        record2 = PKMCRecordDTO(**record_dict)
        
        assert record == record2


class TestPKMCBulkCreateDTO:
    """Test suite for PKMCBulkCreateDTO"""
    
    @pytest.fixture
    def valid_record_data(self):
        """Valid record data"""
        return {
            "partnumber": "PN-1234",
            "supply_area": "P01A",
            "num_reg_circ": "CIRC-001",
            "deposit_type": "B01",
            "deposit_position": "Pos1",
            "container": "Container1",
            "description": "Item Description",
            "pack_standard": "Standard1",
            "qty_per_box": 10.0,
            "qty_max_box": 50.0,
            "total_theoretical_qty": 500.0,
            "qty_for_restock": 490.0,
            "rack": "Rack1",
            "lb_balance": 2000.0,
            "lb_balance_box": 200.0,
        }
    
    def test_bulk_create_single_record(self, valid_record_data):
        """Test bulk create with single record"""
        bulk_dto = PKMCBulkCreateDTO(records=[valid_record_data])
        
        assert len(bulk_dto.records) == 1
        assert bulk_dto.records[0].partnumber == "PN-1234"
    
    def test_bulk_create_multiple_records(self, valid_record_data):
        """Test bulk create with multiple records"""
        record1 = valid_record_data
        record2 = valid_record_data.copy()
        record2["partnumber"] = "PN-5678"
        
        bulk_dto = PKMCBulkCreateDTO(records=[record1, record2])
        
        assert len(bulk_dto.records) == 2
        assert bulk_dto.records[0].partnumber == "PN-1234"
        assert bulk_dto.records[1].partnumber == "PN-5678"
    
    def test_bulk_create_empty_records(self):
        """Test bulk create with empty records list"""
        bulk_dto = PKMCBulkCreateDTO(records=[])
        
        assert len(bulk_dto.records) == 0
    
    def test_bulk_create_invalid_record(self, valid_record_data):
        """Test that invalid record in bulk fails"""
        invalid_record = valid_record_data.copy()
        invalid_record["qty_per_box"] = -5.0  # Invalid
        
        with pytest.raises(ValidationError):
            PKMCBulkCreateDTO(records=[invalid_record])
    
    def test_bulk_create_validates_all_records(self, valid_record_data):
        """Test that all records in bulk are validated"""
        record1 = valid_record_data
        record2 = valid_record_data.copy()
        record2["qty_max_box"] = 0  # Invalid
        
        with pytest.raises(ValidationError):
            PKMCBulkCreateDTO(records=[record1, record2])


class TestDTOIntegration:
    """Integration tests for DTOs with realistic data"""
    
    def test_dto_with_real_database_values(self):
        """Test DTO with realistic values from database"""
        data = {
            "partnumber": "PKMC-005-ABC",
            "supply_area": "P01A",
            "num_reg_circ": "REG-2024-001",
            "deposit_type": "B01",
            "deposit_position": "A1-01-01",
            "container": "EUR-000-150-0",
            "description": "Industrial Component",
            "pack_standard": "5-Pack",
            "qty_per_box": 50.0,
            "qty_max_box": 100.0,
            "total_theoretical_qty": 5000.0,
            "qty_for_restock": 4950.0,
            "rack": "Rack-A-01",
            "lb_balance": 2000.0,
            "lb_balance_box": 40.0,
        }
        
        record = PKMCRecordDTO(**data)
        
        # Verify all fields
        assert record.partnumber == "PKMC-005-ABC"
        assert record.supply_area == "P01A"
        assert record.qty_per_box == 50.0
        assert record.qty_max_box == 100.0
        assert record.description == "Industrial Component"
    
    def test_dto_serialization(self):
        """Test DTO serialization to dict"""
        data = {
            "partnumber": "PN-001",
            "supply_area": "P01A",
            "num_reg_circ": "CIRC-001",
            "deposit_type": "B01",
            "deposit_position": "Pos1",
            "container": "Container1",
            "description": "Test",
            "pack_standard": "Standard1",
            "qty_per_box": 10.0,
            "qty_max_box": 50.0,
            "total_theoretical_qty": 500.0,
            "qty_for_restock": 490.0,
            "rack": "Rack1",
            "lb_balance": 2000.0,
            "lb_balance_box": 200.0,
        }
        
        record = PKMCRecordDTO(**data)
        serialized = record.model_dump()
        
        # Verify serialization
        assert isinstance(serialized, dict)
        assert serialized["partnumber"] == "PN-001"
        assert serialized["qty_per_box"] == 10.0
        
        # Should be able to recreate from serialized data
        record2 = PKMCRecordDTO(**serialized)
        assert record == record2


class TestPKMCDTOSpecificValidation:
    """Test PKMC-specific validation rules"""
    
    def test_deposit_type_b01(self):
        """Test that common deposit type B01 is accepted"""
        data = {
            "partnumber": "PN001",
            "supply_area": "P01A",
            "num_reg_circ": "CIRC001",
            "deposit_type": "B01",
            "deposit_position": "Pos1",
            "container": "C1",
            "description": "Item",
            "pack_standard": "S1",
            "qty_per_box": 10.0,
            "qty_max_box": 50.0,
            "total_theoretical_qty": 500.0,
            "qty_for_restock": 490.0,
            "rack": "Rack1",
            "lb_balance": 2000.0,
            "lb_balance_box": 200.0,
        }
        
        record = PKMCRecordDTO(**data)
        assert record.deposit_type == "B01"
    
    def test_supply_area_format(self):
        """Test supply area with typical format"""
        data = {
            "partnumber": "PN001",
            "supply_area": "P02B",
            "num_reg_circ": "CIRC001",
            "deposit_type": "B01",
            "deposit_position": "Pos1",
            "container": "C1",
            "description": "Item",
            "pack_standard": "S1",
            "qty_per_box": 10.0,
            "qty_max_box": 50.0,
            "total_theoretical_qty": 500.0,
            "qty_for_restock": 490.0,
            "rack": "Rack1",
            "lb_balance": 2000.0,
            "lb_balance_box": 200.0,
        }
        
        record = PKMCRecordDTO(**data)
        assert record.supply_area == "P02B"
