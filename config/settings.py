from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # APP CONFIG
    FILES_DRIVER: str
    APP_URL: str

    # MYSQL
    MYSQL_HOST: str
    MYSQL_PORT: int = Field(default=3306)
    MYSQL_USER: str
    MYSQL_PSWD: str | None = ""
    MYSQL_DATABASE: str

    # STORAGE
    STORAGE_PATH: str
    EXCEL_PATH: str

    # EXCEL FILES
    PKMC_PATH: str
    PK05_PATH: str

    class Config:
        env_file = "config/.env"
        extra = "forbid"
        case_sensitive = True


settings = Settings()