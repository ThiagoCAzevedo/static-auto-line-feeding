from sqlalchemy.orm import Session
from common.base_repository import BaseRepository
from modules.pkmc.infrastructure.models import PKMC


class PKMCRepository(BaseRepository):
    def __init__(self, db: Session):
        super().__init__(db, PKMC, "pkmc_repository")