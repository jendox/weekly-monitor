from contextvars import ContextVar
from typing import Self

from pydantic import BaseModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from models import Region

app_settings: ContextVar["AppSettings"] = ContextVar("app_settings")


class SpreadsheetId(BaseModel):
    products: str
    uk: str
    us: str
    fr: str
    it: str
    es: str
    de: str

    def by_region(self, region: Region) -> str:
        return getattr(self, region.value)


class Helium(BaseModel):
    account_id: int
    auth_token: SecretStr
    pacvue_access_token: SecretStr
    cells_range: list[str]


class Sellerboard(BaseModel):
    current_cells_range: list[str]
    historical_cells_range: list[str]


class Sns(BaseModel):
    cells_range: list[str]


class Business(BaseModel):
    current_cells_range: list[str]
    historical_cells_range: list[str]
    title: dict[str, str]

    def title_by_region(self, region: Region) -> str:
        return self.title.get(region.value)


class Campaigns(BaseModel):
    cells_range: list[str]


class AppSettings(BaseSettings):
    credentials: str
    update_offset: int
    spreadsheet_id: SpreadsheetId
    helium: Helium
    sellerboard: Sellerboard
    sns: Sns
    business: Business
    campaigns: Campaigns

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    @classmethod
    def load(cls) -> Self:
        return cls()
