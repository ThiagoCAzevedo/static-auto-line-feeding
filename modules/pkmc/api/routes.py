from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from modules.pkmc.application.pipeline import PKMCPipeline
from modules.pkmc.infrastructure.repository import PKMCRepository
from database.session import get_db
from common.logger import logger


router = APIRouter()
log = logger("pkmc")


@router.get("/response", summary="Get cleaned PKMC values")
def get_clean_pkmc(limit: int = Query(50, ge=1, le=1000)):
    log.info(f"GET /pkmc/response (limit={limit})")

    try:
        pipeline = PKMCPipeline()
        df = pipeline.run().head(limit).collect()
        count = df.height
        log.info(f"✓ Returned {count} cleaned PKMC records")
        return df.to_dicts()

    except Exception as e:
        log.error(f"✗ Failed to get clean PKMC data: {str(e)}", exc_info=True)
        raise


@router.get("/db", summary="Get all PKMC values from database")
def get_from_db(limit: int = None, db: Session = Depends(get_db)):
    log.info(f"GET /pkmc/db{f' (limit={limit})' if limit else ''}")

    try:
        repo = PKMCRepository(db)
        records = repo.fetch_all(limit)
        return records

    except Exception as e:
        log.error(f"Failed to fetch from database: {str(e)}", exc_info=True)
        raise


@router.post("/update", summary="Update PKMC records in database")
def update(records: list[dict], db: Session = Depends(get_db)):
    log.info(f"POST /pkmc/update ({len(records)} records)")

    try:
        repo = PKMCRepository(db)
        total = repo.update(records)
        return {"message": "Records updated successfully", "count": total}

    except Exception as e:
        log.error(f"✗ Update failed: {str(e)}", exc_info=True)
        raise


@router.post("/upsert", summary="Upsert PKMC values into the database")
def upsert_pkmc(batch_size: int = Query(10_000, ge=1, le=100_000), db: Session = Depends(get_db)):
    log.info(f"POST /pkmc/upsert (batch_size={batch_size})")

    try:
        pipeline = PKMCPipeline()
        df = pipeline.run().collect()
        row_count = df.height
        log.debug(f"Pipeline produced {row_count} rows for upsert")

        repo = PKMCRepository(db)
        rows = repo.bulk_upsert(df, batch_size)

        log.info(f"Upsert completed: {rows} rows")

        return {
            "message": "PKMC upsert completed successfully",
            "rows": rows,
            "batch_size": batch_size,
            "table": "pkmc",
        }

    except Exception as e:
        log.error(f"Upsert operation failed: {str(e)}", exc_info=True)
        raise