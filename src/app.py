from fastapi import FastAPI
from .routes.ingest import router as ingest_router
from .database import create_db_and_tables

def create_app() -> FastAPI:
    app = FastAPI(title="API Ingestion Service", version="0.1.0")
    app.include_router(ingest_router, prefix="/api")

    @app.on_event("startup")
    async def on_startup():
        create_db_and_tables()

    return app

app = create_app()
