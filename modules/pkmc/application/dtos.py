from __future__ import annotations
from typing import TypedDict


class PKMC_RecordDTO(TypedDict):
    id: int
    partnumber: str
    supply_area: str
    num_reg_circ: str
    deposit_type: str
    deposit_position: str
    container: str
    description: str
    pack_standard: str
    qty_per_box: float
    qty_max_box: float
    total_theoretical_qty: float
    qty_for_restock: float
    rack: str
    lb_balance: float
    lb_balance_box: float
