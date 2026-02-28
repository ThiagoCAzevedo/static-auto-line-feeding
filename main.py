from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from common.logger import logger
from modules.pk05.api.routes import router as pk05_router
from modules.pkmc.api.routes import router as pkmc_router
from modules.files.api.routes import router as files_router
import uvicorn


log = logger("main")


def create_app() -> FastAPI:
    log.info("FastAPI app initialized")

    app = FastAPI(
        title="Auto Line Feeding API",
        description="Main backend static responsible for save and clean static files for Auto Line Feeding system.",
        docs_url="/static-files-docs",
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

    app.include_router(
        pkmc_router,
        prefix="/pkmc",
        tags=["pkmc"]
    )

    app.include_router(
        files_router,
        prefix="/files",
        tags=["files"]
    )

    return app


app = create_app()


if __name__ == "__main__":
    log.info("Starting Uvicorn server with reload support")
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8004,
        reload=True
    )