from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True)

    API_ID: int
    API_HASH: str

    REF_ID: str = '00001N6y'

    USE_RANDOM_DELAY_IN_RUN: bool = False
    RANDOM_DELAY_IN_RUN: list[int] = [0, 300]

    EMOJI_TO_SET: Optional[str] = "🍅"

    POINTS_COUNT: list[int] = [450, 600]
    AUTO_PLAY_GAME: bool = True

    AUTO_TASK: bool = True
    AUTO_TASK_3RD: bool = True

    AUTO_DAILY_REWARD: bool = True
    AUTO_CLAIM_STARS: bool = True
    AUTO_CLAIM_COMBO: bool = True
    AUTO_RANK_UPGRADE: bool = False

    USE_PROXY: bool = True

    SLEEP_TIME: list[int] = [32000, 42000]

settings = Settings()

