from dataclasses import dataclass


@dataclass(frozen=True)
class PKMCRecord:
    id: int
    partnumber: str
    supply_area: str
    deposit_type: str
    deposit_position: str
    description: str
    qty_per_box: float
    qty_max_box: float
    total_theoretical_qty: float
    qty_for_restock: float
    rack: str
    lb_balance: float
    lb_balance_box: float
