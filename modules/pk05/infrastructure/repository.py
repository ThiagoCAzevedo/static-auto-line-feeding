from sqlalchemy.orm import Session
from common.base_repository import BaseRepository
from modules.pk05.infrastructure.models import PK05


class PK05Repository(BaseRepository):
    def __init__(self, db: Session):
        super().__init__(db, PK05, "pk05_repository")