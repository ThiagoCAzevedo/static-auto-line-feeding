from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.pk05.application.pipeline import PK05Pipeline
from modules.pk05.infrastructure.repository import PK05Repository
from database.session import get_db
from common.logger import logger
from common.response_handler import ResponseHandler


router = APIRouter()
log = logger("pk05")


@router.get("/response", summary="Get cleaned PK05 values")
def get_raw(limit: int = Query(50, ge=1, le=1000)):
    log.info(f"GET /pk05/response (limit={limit})")

    try:
        pipeline = PK05Pipeline()
        df = pipeline.run().head(limit).collect()
        records = df.to_dicts()

        return ResponseHandler.success(
            data=records,
            message=f"Returned {len(records)} cleaned PK05 records"
        )

    except Exception as e:
        log.error(f"Failed to get clean PK05 data: {str(e)}", exc_info=True)
        return ResponseHandler.error(str(e))


@router.get("/response/db", summary="Get all PK05 values from database")
def get_from_db(limit: int = None, db: Session = Depends(get_db)):
    log.info(f"GET /pk05/response/db{f' (limit={limit})' if limit else ''}")

    try:
        repo = PK05Repository(db)
        records = repo.fetch_all(limit)

        return ResponseHandler.success(
            data=records,
            message="Fetched PK05 values from DB"
        )

    except Exception as e:
        log.error(f"Failed to fetch from database: {str(e)}", exc_info=True)
        return ResponseHandler.error(str(e))


@router.post("/upsert", summary="Upsert PK05 values into the database")
def upsert(batch_size: int = Query(10_000, ge=1, le=100_000), db: Session = Depends(get_db)):
    log.info(f"POST /pk05/upsert (batch_size={batch_size})")

    try:
        pipeline = PK05Pipeline()
        df = pipeline.run().collect()

        repo = PK05Repository(db)
        rows = repo.bulk_upsert(df, batch_size)

        return ResponseHandler.success(
            data={
                "rows": rows,
                "batch_size": batch_size,
                "table": "pk05",
            },
            message="PK05 upsert completed successfully"
        )

    except Exception as e:
        log.error(f"Upsert operation failed: {str(e)}", exc_info=True)
        return ResponseHandler.error(str(e))