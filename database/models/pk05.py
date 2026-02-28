from sqlalchemy import Column, String, Integer, DateTime, Float
from sqlalchemy.sql import func
from database.base import Base


class PK05(Base):
    __tablename__ = "pk05"

    id = Column(Integer, primary_key=True, index=True)
    supply_area = Column(String(100), nullable=False)
    deposit = Column(String(50), nullable=False)
    responsible = Column(String(100))
    discharge_point = Column(String(100))
    description = Column(String(255), nullable=False)
    takt = Column(String(10), nullable=False)
    
    created_at = Column(DateTime, server_default=func.now(), nullable=False)