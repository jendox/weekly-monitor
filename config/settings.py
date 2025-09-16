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


# class Business(BaseModel):
#     title: dict[str, str]
#     real_units: list[str]
#
#
# class Sellerboard(BaseModel):
#     current: CellsRange
#     update: CellsRange
#
#
# class Amazon(BaseModel): ...

class Helium(BaseModel):
    account_id: int
    auth_token: SecretStr
    pacvue_token: SecretStr


# class SNS(BaseModel): ...


class AppSettings(BaseSettings):
    credentials: str
    update_offset: int
    spreadsheet_id: SpreadsheetId
    helium: Helium

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    @classmethod
    def load(cls) -> Self:
        return cls()
