from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from modules.pk05.application.pipeline import PK05Pipeline
from modules.pk05.infrastructure.repository import PK05Repository
from database.session import get_db


router = APIRouter()


@router.get("/response")
def get_raw(limit: int = 50):
    df = PK05Pipeline().run().collect()
    return df.head(limit).to_dicts()


@router.get("/db")
def get_from_db(limit: int = None, db: Session = Depends(get_db)):
    repo = PK05Repository(db)
    return repo.fetch_all(limit)


@router.post("/update")
def update(records: list[dict], db: Session = Depends(get_db)):
    repo = PK05Repository(db)
    total = repo.update(records)
    return {"message": "Records updated successfully", "count": total}


@router.post("/upsert")
def upsert(batch_size: int = 10_000, db: Session = Depends(get_db)):
    pipeline = PK05Pipeline()
    repo = PK05Repository(db)
    df = pipeline.run().collect()
    rows = repo.bulk_upsert(df, batch_size)
    return {"rows": rows, "batch_size": batch_size}

