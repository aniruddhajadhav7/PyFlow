from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "PyFlow"
    debug: bool = False
    log_level: str = "INFO"
    redis_url: str = "redis://localhost:6379/0"

    # Rate Limiting Settings
    rate_limit_algorithm: str = "token_bucket"  # or "sliding_window"
    rate_limit_requests: int = 100
    rate_limit_window: int = 60  # in seconds

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
