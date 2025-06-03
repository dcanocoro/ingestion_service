from fastapi import FastAPI
from .routes.ingest import router as ingest_router
from .database import create_db_and_tables
from .logging_config import setup_logging, get_logger

logger = get_logger("app")

def create_app() -> FastAPI:
    setup_logging()
    logger.info("Creating FastAPI application")
    
    app = FastAPI(title="API Ingestion Service", version="0.1.0")
    app.include_router(ingest_router, prefix="/api")
    
    logger.info("Router included successfully")

    @app.on_event("startup")
    async def on_startup():
        logger.info("Starting up application - creating database and tables")
        create_db_and_tables()
        logger.info("Database and tables created successfully")

    return app

app = create_app()
