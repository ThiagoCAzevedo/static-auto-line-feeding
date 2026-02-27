from dataclasses import dataclass


@dataclass(frozen=True)
class PK05Record:
    id: int
    takt: str
    deposit: str
    description: str
    supply_area: str