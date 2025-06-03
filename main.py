import uvicorn
from src.app import app
from src.logging_config import setup_logging

if __name__ == "__main__":
    setup_logging()
    uvicorn.run(app, host="0.0.0.0", port=8000)
