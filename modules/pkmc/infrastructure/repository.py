from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from common.logger import logger
from modules.pkmc.application.dtos import PKMC_RecordDTO
from database.models.pkmc import PKMC


class PKMCRepository:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("pkmc")

    def fetch_all(self, limit: int = None) -> list[dict]:
        try:
            self.log.debug(f"Fetching PKMC records{f' (limit={limit})' if limit else ''}")
            query = self.db.query(PKMC)
            if limit:
                query = query.limit(limit)
            records = query.all()
            count = len(records)
            self.log.info(f"Retrieved {count} PKMC records from database")
            return [record.__dict__ for record in records]
        except Exception as e:
            self.log.error(f"Failed to fetch PKMC records: {str(e)}", exc_info=True)
            raise

    def bulk_upsert(self, df, batch_size: int = 10000) -> int:
        rows = df.to_dicts()
        total = 0
        
        self.log.info(f"Starting bulk upsert: {len(rows)} rows, batch_size={batch_size}")

        try:
            for batch_num, i in enumerate(range(0, len(rows), batch_size), 1):
                chunk = rows[i : i + batch_size]

                stmt = insert(PKMC).values(chunk)

                ignore_cols = ["created_at"]

                update_dict = {
                    c.name: stmt.inserted[c.name]
                    for c in PKMC.__table__.columns
                    if c.name not in ignore_cols
                }

                stmt = stmt.on_duplicate_key_update(**update_dict)

                self.db.execute(stmt)
                total += len(chunk)
                self.log.debug(f"Batch #{batch_num}: {total}/{len(rows)} rows processed")

            self.db.commit()
            self.log.info(f"Bulk upsert completed successfully: {total} rows")
            return total

        except Exception as e:
            self.log.error(f"Bulk upsert failed at {total} rows: {str(e)}", exc_info=True)
            self.db.rollback()
            raise