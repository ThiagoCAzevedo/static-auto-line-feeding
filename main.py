from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from common.logger import logger
from modules.pk05.api.routes import router as pk05_router
import uvicorn


log = logger("main")


def create_app() -> FastAPI:
    log.info("FastAPI app initialized")

    app = FastAPI(
        title="Auto Line Feeding API",
        description="Main backend static responsible for save and clean static files for Auto Line Feeding system.",
        docs_url="/static-docs",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(
        pk05_router,
        prefix="/pk05",
        tags=["pk05"]
    )

    return app


app = create_app()


if __name__ == "__main__":
    log.info("Starting Uvicorn server with reload support")
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )