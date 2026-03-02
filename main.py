from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from common.logger import logger
from modules.pk05.api.routes import router as pk05_router
from modules.pkmc.api.routes import router as pkmc_router
from modules.files.api.routes import router as files_router
from middleware.error_handler import setup_error_handlers
from config.settings import settings
import uvicorn


log = logger("main")


def create_app() -> FastAPI:
    log.info("Initializing FastAPI application")

    app = FastAPI(
        title="Auto Line Feeding API",
        description="Auto Line Feeding microservice responsible for powering the static files (pkmc and pk05) to Auto Line Feeding Core microservice.",
        docs_url="/static-files-docs",
    )

    setup_error_handlers(app)

    log.debug("Adding CORS middleware")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    log.debug("Registering pk05 routes")
    app.include_router(
        pk05_router,
        prefix="/pk05",
        tags=["pk05"]
    )

    log.debug("Registering pkmc routes")
    app.include_router(
        pkmc_router,
        prefix="/pkmc",
        tags=["pkmc"]
    )

    log.debug("Registering files routes")
    app.include_router(
        files_router,
        prefix="/files",
        tags=["files"]
    )

    log.info("FastAPI application initialized successfully")
    return app


app = create_app()


if __name__ == "__main__":
    log.info("Starting Uvicorn server (127.0.0.1:8004, reload=True)")
    try:
        uvicorn.run(
            "main:app",
            host="127.0.0.1",
            port=8004,
            reload=True
        )
    except Exception as e:
        log.error(f"Uvicorn server failed: {str(e)}", exc_info=True)
        raise


# -- ROUTE FOR HEALTH CHECK --
@app.get("/health")
def health_check():
    return {"status": "healthy", "app": settings.APP_NAME}