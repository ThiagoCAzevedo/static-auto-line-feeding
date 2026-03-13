from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from sqlalchemy import text
from common.logger import logger
import time


class BaseRepository:
    def __init__(self, db: Session, model, service_name: str):
        self.db = db
        self.model = model
        self.log = logger(service_name)

    def fetch_all(self, limit: int = None) -> list[dict]:
        try:
            start_time = time.time()
            self.log.debug(f"Fetching all {self.model.__tablename__} records")
            
            table_name = self.model.__tablename__
            query_str = f"SELECT * FROM {table_name}"
            
            if limit:
                query_str += f" LIMIT {limit}"
            
            result = self.db.execute(text(query_str))
            records = [dict(row) for row in result.mappings().all()]
            
            elapsed = time.time() - start_time
            count = len(records)
            if count == 0:
                self.log.warning(f"No {self.model.__tablename__} records found")
            self.log.info(f"Retrieved {count} {self.model.__tablename__} records in {elapsed:.2f}s")
            return records
            
        except Exception as e:
            self.log.error(f"Failed to fetch records: {str(e)}", exc_info=True)
            raise

    def bulk_upsert(self, df, batch_size: int = 10000) -> int:
        rows = df.to_dicts()
        total = 0
        if not rows:
            self.log.warning("bulk_upsert called with no rows to process")
            return 0
        self.log.info(f"Starting bulk upsert: {len(rows)} rows, batch_size={batch_size}")

        try:
            for batch_num, i in enumerate(range(0, len(rows), batch_size), 1):
                chunk = rows[i : i + batch_size]
                stmt = insert(self.model).values(chunk)
                ignore_cols = ["created_at"]
                update_dict = {
                    c.name: stmt.inserted[c.name]
                    for c in self.model.__table__.columns
                    if c.name not in ignore_cols
                }
                stmt = stmt.on_duplicate_key_update(**update_dict)
                self.db.execute(stmt)
                total += len(chunk)
                self.log.debug(f"Batch #{batch_num}: {total}/{len(rows)} rows processed")

            self.db.commit()
            self.log.info(f"Bulk upsert completed: {total} rows")
            return total

        except Exception as e:
            self.log.error(f"Bulk upsert failed at {total} rows: {str(e)}", exc_info=True)
            self.db.rollback()
            raise

    def _to_dict(self, record) -> dict:
        return {k: v for k, v in record.__dict__.items() if not k.startswith('_')}