from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.pk05.application.pipeline import PK05Pipeline
from modules.pk05.infrastructure.repository import PK05Repository
from database.session import get_db
from common.logger import logger


router = APIRouter()
log = logger("pk05")


@router.get("/response", summary="Get cleaned PK05 values")
def get_raw(limit: int = Query(50, ge=1, le=1000)):
    log.info(f"GET /pk05/response (limit={limit})")

    try:
        pipeline = PK05Pipeline()
        df = pipeline.run().head(limit).collect()
        count = df.height
        log.info(f"Returned {count} cleaned PK05 records")
        return df.to_dicts()

    except Exception as e:
        log.error(f"Failed to get clean PK05 data: {str(e)}", exc_info=True)
        raise


@router.get("/db", summary="Get all PK05 values from database")
def get_from_db(limit: int = None, db: Session = Depends(get_db)):
    log.info(f"GET /pk05/db{f' (limit={limit})' if limit else ''}")

    try:
        repo = PK05Repository(db)
        records = repo.fetch_all(limit)
        return records

    except Exception as e:
        log.error(f"Failed to fetch from database: {str(e)}", exc_info=True)
        raise


@router.post("/upsert", summary="Upsert PK05 values into the database")
def upsert(batch_size: int = Query(10_000, ge=1, le=100_000), db: Session = Depends(get_db)):
    log.info(f"POST /pk05/upsert (batch_size={batch_size})")

    try:
        pipeline = PK05Pipeline()
        df = pipeline.run().collect()
        row_count = df.height
        log.debug(f"Pipeline produced {row_count} rows for upsert")

        repo = PK05Repository(db)
        rows = repo.bulk_upsert(df, batch_size)

        log.info(f"Upsert completed: {rows} rows")

        return {
            "message": "PK05 upsert completed successfully",
            "rows": rows,
            "batch_size": batch_size,
            "table": "pk05",
        }

    except Exception as e:
        log.error(f"Upsert operation failed: {str(e)}", exc_info=True)
        raise