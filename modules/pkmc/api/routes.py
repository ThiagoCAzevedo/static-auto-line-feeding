from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.pkmc.application.pipeline import PKMCPipeline
from modules.pkmc.infrastructure.repository import PKMCRepository
from database.session import get_db
from common.logger import logger
import polars as pl


router = APIRouter()
log = logger("pkmc")


@router.get("/response", summary="Get cleaned PKMC values")
def get_clean_pkmc(limit: int = Query(50, ge=1, le=1000)):
    log.info(f"GET /static/response/processed — limit={limit}")

    try:
        pipeline = PKMCPipeline()
        df = pipeline.run().head(limit).collect()
        log.info(f"Cleaned PKMC processed successfully — rows returned: {df.height}")
        return df.to_dicts()

    except Exception as e:
        log.error("Error processing PKMC (clean)", exc_info=True)
        raise


@router.get("/db", summary="Get all PKMC values from database")
def get_from_db(limit: int = None, db: Session = Depends(get_db)):
    log.info(f"GET /pkmc/db — limit={limit}")

    try:
        repo = PKMCRepository(db)
        records = repo.fetch_all(limit)
        log.info(f"Retrieved {len(records)} PKMC records from database")
        return records

    except Exception as e:
        log.error("Error fetching PKMC records from database", exc_info=True)
        raise


@router.post("/upsert", summary="Upsert PKMC values into the database")
def upsert_pkmc(batch_size: int = Query(10_000, ge=1, le=100_000), db: Session = Depends(get_db)):
    log.info(f"POST /static/upsert — batch_size={batch_size}")

    try:
        pipeline = PKMCPipeline()
        df = pipeline.run().collect()

        log.info(f"PKMC processed before upsert — total rows: {df.height}")

        repo = PKMCRepository(db)
        rows = repo.bulk_upsert(df, batch_size)

        log.info(f"PKMC upsert completed successfully — rows upserted: {rows}")

        return {
            "message": "PKMC upsert completed successfully.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "pkmc",
        }

    except Exception as e:
        log.error("Error during PKMC upsert", exc_info=True)
        raise
