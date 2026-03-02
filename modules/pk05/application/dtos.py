from pydantic import BaseModel, Field


class PK05RecordDTO(BaseModel):
    partnumber: str
    supply_area: str
    num_reg_circ: str
    deposit_type: str
    deposit_position: str
    container: str
    description: str
    pack_standard: str
    qty_per_box: float = Field(gt=0)
    total_theoretical_qty: float
    qty_for_restock: float
    rack: str
    lb_balance: float
    lb_balance_box: float

    class Config:
        from_attributes = True


class PK05BulkCreateDTO(BaseModel):
    records: list[PK05RecordDTO]