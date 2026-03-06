from pathlib import Path
from pydantic_settings import BaseSettings

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    lake_dir: str = str(PROJECT_ROOT / "data" / "lake")
    cors_origins: list[str] = [
        "http://localhost:5173",
        "http://localhost:3000",
        "https://aradune.co",
        "https://www.aradune.co",
    ]
    max_rows: int = 10_000
    port: int = 8000

    model_config = {"env_prefix": "ARADUNE_"}


settings = Settings()
