from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from common.logger import logger
from modules.pkmc.application.dtos import PKMC_RecordDTO
from database.models.pkmc import PKMC


class PKMCRepository:
    def __init__(self, db: Session):
        self.db = db
        self.log = logger("pkmc")

    def fetch_all(self, limit: int = None):
        self.log.info("Fetching all PKMC records from database")
        try:
            query = self.db.query(PKMC)
            if limit:
                query = query.limit(limit)
            records = query.all()
            self.log.info(f"Retrieved {len(records)} PKMC records")
            return [record.__dict__ for record in records]
        except Exception:
            self.log.error("Error fetching PKMC records", exc_info=True)
            raise

    def update(self, records: list[dict]) -> int:
        self.log.info(f"Starting PKMC update with {len(records)} records")
        total_updated = 0

        try:
            for record in records:
                record_id = record.pop("id", None)
                if record_id is None:
                    self.log.warning("Record missing 'id' field, skipping")
                    continue

                self.db.query(PKMC).filter(PKMC.id == record_id).update(
                    record,
                    synchronize_session=False
                )
                total_updated += 1

            self.db.commit()
            self.log.info(f"PKMC update completed successfully - Total records updated: {total_updated}")
            return total_updated

        except Exception as e:
            self.log.error("Error during PKMC update", exc_info=True)
            self.db.rollback()
            raise

    def bulk_upsert(self, df, batch_size: int = 10000):
        self.log.info(f"Starting PKMC bulk upsert with batch_size={batch_size}")
        rows = df.to_dicts()
        total = 0

        try:
            for i in range(0, len(rows), batch_size):
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
                self.log.info(f"Processed {total} rows")

            self.db.commit()
            self.log.info(f"PKMC bulk upsert completed successfully - Total rows: {total}")
            return total

        except Exception as e:
            self.log.error("Error during PKMC bulk upsert", exc_info=True)
            self.db.rollback()
            raise
