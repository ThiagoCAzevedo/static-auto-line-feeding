from __future__ import annotations
from typing import TypedDict


class PK05_RecordDTO(TypedDict):
    id: int
    takt: str
    deposit: str
    description: str
    supply_area: str