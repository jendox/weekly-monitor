from enum import Enum

from pydantic import BaseModel


class Region(str, Enum):
    uk = "uk"
    us = "us"
    fr = "fr"
    it = "it"
    es = "es"
    de = "de"


class Sellerboard(BaseModel):
    """Модель данных, получаемых из отчета Sellerboard"""
    profit: float = 0.0
    margin: float = 0.0


class KeywordRank(BaseModel):
    word: str = None
    rank: int = 0


class Helium(BaseModel):
    id: int
    ranks: list[KeywordRank] = None


class Campaign(BaseModel):
    name: str
    spend: float = 0.0  # PPC
    clicks: int = 0
    ctr: float = 0.0
    cpc: float = 0.0
    orders: int = 0
    acos: float = 0.0


class Business(BaseModel):
    title: str = None
    sku: str = None
    sessions: int = 0
    units: int = 0
    sales: int = 0
    orders: int = 0


class BusinessUpdate(BaseModel):
    units: int = 0


class Sns(BaseModel):
    subscriptions: int = 0
    shipped_units: int = 0


class Product(BaseModel):
    asin: str
    sheet_title: str
    row_index: int = -1
    campaign: Campaign
    sb_current: Sellerboard = Sellerboard()
    sb_update: Sellerboard = Sellerboard()
    helium: Helium
    business_current: Business = Business()
    business_update: BusinessUpdate = BusinessUpdate()
    sns: Sns = Sns()
