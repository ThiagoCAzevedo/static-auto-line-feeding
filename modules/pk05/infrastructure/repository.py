from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from common.logger import logger
from modules.pk05.application.dtos import PK05_RecordDTO
from modules.pk05.infrastructure.models import PK05



class PK05Repository:
    def __init__(self, db):
        self.db = db
        self.log = logger("pk05")

    def fetch_all(self, limit: int = None):
        self.log.info("Fetching all PK05 records from database")
        try:
            query = self.db.query(PK05)
            if limit:
                query = query.limit(limit)
            records = query.all()
            self.log.info(f"Retrieved {len(records)} PK05 records")
            return [record.__dict__ for record in records]
        except Exception:
            self.log.error("Error fetching PK05 records", exc_info=True)
            raise

    def update(self, records: list[dict]) -> int:
        self.log.info(f"Starting PK05 update with {len(records)} records")
        total_updated = 0

        try:
            for record in records:
                record_id = record.pop("id", None)
                if record_id is None:
                    self.log.warning("Record missing 'id' field, skipping")
                    continue

                self.db.query(PK05).filter(PK05.id == record_id).update(
                    record,
                    synchronize_session=False
                )
                total_updated += 1

            self.db.commit()
            self.log.info(f"PK05 update completed successfully - Total records updated: {total_updated}")
            return total_updated

        except Exception as e:
            self.log.error("Error during PK05 update", exc_info=True)
            self.db.rollback()
            raise

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
