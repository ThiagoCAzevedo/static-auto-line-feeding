from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from common.logger import logger
from modules.pk05.application.dtos import PK05_RecordDTO
from modules.pk05.infrastructure.models import PK05



class PK05Repository:
    def __init__(self, db):
        self.db = db

    def bulk_upsert(self, df, batch_size: int = 10000):
        rows = df.to_dicts()
        total = 0

        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]

            stmt = insert(PK05).values(chunk)

            ignore_cols = ["created_at", "updated_at"]

            update_dict = {
                c.name: stmt.inserted[c.name]
                for c in PK05.__table__.columns
                if c.name not in ignore_cols
            }

            stmt = stmt.on_duplicate_key_update(**update_dict)

            self.db.execute(stmt)
            total += len(chunk)

        self.db.commit()
        return total
