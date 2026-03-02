from dataclasses import dataclass


@dataclass(frozen=True)
class PK05Record:
    takt: str
    deposit: str
    description: str
    supply_area: str