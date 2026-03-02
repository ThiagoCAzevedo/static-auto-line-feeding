from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.pkmc.application.pipeline import PKMCPipeline
from modules.pkmc.infrastructure.repository import PKMCRepository
from database.session import get_db
from common.logger import logger
from common.response_handler import ResponseHandler


router = APIRouter()
log = logger("pkmc")


@router.get("/response", summary="Get cleaned PKMC values")
def get_clean_pkmc(limit: int = Query(50, ge=1, le=1000)):
    log.info(f"GET /pkmc/response (limit={limit})")

    try:
        pipeline = PKMCPipeline()
        df = pipeline.run().head(limit).collect()
        result = df.to_dicts()

        return ResponseHandler.success(
            data=result,
            message=f"Returned {len(result)} cleaned PKMC records"
        )

    except Exception as e:
        log.error(f"Failed to get clean PKMC data: {str(e)}", exc_info=True)
        return ResponseHandler.error(str(e))


@router.get("/response/db", summary="Get all PKMC values from database")
def get_from_db(limit: int = None, db: Session = Depends(get_db)):
    log.info(f"GET /pkmc/response/db{f' (limit={limit})' if limit else ''}")

    try:
        repo = PKMCRepository(db)
        records = repo.fetch_all(limit)

        return ResponseHandler.success(
            data=records,
            message="Fetched PKMC values from DB"
        )

    except Exception as e:
        log.error(f"Failed to fetch from database: {str(e)}", exc_info=True)
        return ResponseHandler.error(str(e))


@router.post("/upsert", summary="Upsert PKMC values into the database")
def upsert_pkmc(batch_size: int = Query(10_000, ge=1, le=100_000), db: Session = Depends(get_db)):
    log.info(f"POST /pkmc/upsert (batch_size={batch_size})")

    try:
        pipeline = PKMCPipeline()
        df = pipeline.run().collect()
        repo = PKMCRepository(db)

        rows = repo.bulk_upsert(df, batch_size)

        return ResponseHandler.success(
            data={
                "rows": rows,
                "batch_size": batch_size,
                "table": "pkmc",
            },
            message="PKMC upsert completed successfully"
        )

    except Exception as e:
        log.error(f"Upsert operation failed: {str(e)}", exc_info=True)
        return ResponseHandler.error(str(e))