"""Pydantic models used to type cast all data requested to the API."""

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional
from enum import Enum, IntEnum
from typing import Literal
import pandas as pd
from pydantic import field_validator, model_validator, BaseModel, Field, Json
import orjson
import json

from pydantic import TypeAdapter


PAGE_SIZE = 50000
MIN_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 100000


# Rolling methods
class RollMethods(str, Enum):
    """Enum to store the different roll methods."""

    LAST_TRADING_DAY = "last_trading_day"
    LAST_DAY_PREV_MONTH = "last_day_prev_month"
    FIRST_DAY_LAST_MONTH = "first_day_last_month"
    FIRST_DELIVERY_DAY = "first_delivery_day"


def json_to_baseModel(basemodel: BaseModel, json: dict) -> dict[str:Any]:
    """Cast and validate all values of the dictionary based on the Pydantic Basemodel.

    Parameters
    ----------
    basemodel: BaseModel
        BaseModel object to use as reference for the data types of all fields
    json: dict
        Dictionary with values representing an object of Basemodel with str types as values.

    Returns
    -------
    dict[str:Any]
        Dictionary all values casted into the correct data type.

    """
    return TypeAdapter(basemodel).validate_python(json).model_dump()


def listJson_to_baseModel(
    basemodel: BaseModel, list_json: list[dict]
) -> list[dict[str:Any]]:
    """Cast and validate all values of the dictionary based on the Pydantic Basemodel.

    Parameters
    ----------
    basemodel: BaseModel
        BaseModel object to use as reference for the data types of all fields.
    list_json: list[dict]
        List of dictionaries with values representing an object of Basemodel with str types as values.

    Returns
    -------
    list[dict[str:Any]]
        List of dictionary with all values casted into the correct data type.

    """
    return [
        TypeAdapter(basemodel).validate_python(json_item).model_dump()
        for json_item in list_json
    ]


class Product(BaseModel):
    """Model representing a Product in the database."""

    product: str
    product_type: str
    exchange: Optional[str] = None
    description: Optional[str] = None
    family: Optional[str] = None
    subfamily: Optional[str] = None
    curve: Optional[str] = None
    curve_bd: Optional[str] = None
    listed_contracts: Optional[int] = None
    listed_contracts_letters: Optional[str] = None
    listed_contracts_liquid: Optional[int] = None
    listed_contracts_offcycle: Optional[int] = None
    listed_contracts_offcycle_letters: Optional[str] = None
    nominal: Optional[Decimal] = None
    dv01: Optional[Decimal] = None
    pv01: Optional[Decimal] = None
    currency: Optional[str] = None
    bloomberg_ticker: Optional[str] = None
    bloomberg_suffix: Optional[str] = None
    refinitiv_ticker: Optional[str] = None
    tt_ticker: Optional[str] = None
    exchange_ticker: Optional[str] = None
    eod_columns: Optional[list[str]] = None
    intraday_tables: Optional[list[str]] = None
    db2tt_factor: Optional[int] = None
    ticksize: Optional[Decimal] = None
    last_tradable_time: Optional[str] = None
    holidays: Optional[str] = None
    has_delivery: Optional[bool] = None
    url: Optional[str] = None
    intra_source: Optional[str] = None
    eod_source: Optional[str] = None
    t2t: Optional[bool] = None
    spread_distance: Optional[Json] = None
    column_fields_override: Optional[Json] = None
    seasonal_factor_close: Optional[Decimal] = None
    seasonal_factor_early_close: Optional[Decimal] = None
    seasonal_reference_instrument: Optional[str] = None
    yield_type: Optional[str] = None
    cmt_tenor: Optional[Decimal] = None
    cmt_spline_tenor: Optional[Decimal] = None

    @field_validator("spread_distance", mode="before")
    @classmethod
    def _parse_json(cls, data):
        return str(data) if data else None

    @field_validator("spread_distance", mode="after")
    @classmethod
    def _parse_json_after(cls, data):
        return str(data) if data else None

    @field_validator("column_fields_override", mode="before")
    @classmethod
    def _parse_json_dict(cls, data):
        if data:
            if isinstance(data, str):
                data = orjson.loads(data)

            if isinstance(data, dict):
                data = {k.lower().strip().replace(" ", "_"): v for k, v in data.items()}
                return orjson.dumps(data)
            else:
                raise ValueError(
                    "column_fields_override should be a key value collection (a dictionary)"
                )
        else:
            return None

    @field_validator("column_fields_override", mode="after")
    @classmethod
    def _parse_json_dict_after(cls, data):
        if data:
            data = {k.lower().strip().replace(" ", "_"): v for k, v in data.items()}
            return json.dumps(data)
        else:
            return None


class CompleteProduct(Product, BaseModel):
    """Abstraction of Product with list of tags."""

    tags: Optional[list[str]] = None


class Exchange(BaseModel):
    """Model representing a Exchange in the database."""

    mic: str = Field(description="Market Identifier Code", examples=["XCME"])
    description: str = Field(
        description="Description of the exchange",
        examples=["Chicago Mercantile Exchange"],
    )
    url: Optional[str] = Field(
        description="URL of the exchange", examples=["www.cme.com"], default=None
    )
    arfimaname: Optional[str] = Field(
        description="Arfima name of the exchange",
        examples=["CME"],
        default=None,
    )
    comment: Optional[str] = Field(description="Additional comments", default=None)
    tt_ticker: Optional[str] = Field(
        description="TT ticker of the exchange", examples=["CME"], default=None
    )


class Family(BaseModel):
    """Model representing a Family in the database."""

    family: str = Field(description="Name of the family", examples=["Interest Rates"])
    description: Optional[str] = Field(description="Description of the family")


class Subfamily(BaseModel):
    """Model representing a Subfamily in the database."""

    subfamily: str = Field(description="Name of the subfamily", examples=["STIRS"])
    family: str = Field(
        description="Name of the family which the subfamily belongs to",
        examples=["Fixed Income"],
    )
    description: Optional[str] = Field(description="Description of the subfamily")


class Instrument(BaseModel):
    """Model representing an instrument in the database."""

    instrument: str
    product: str
    product_type: str
    first_tradeable_date: Optional[date] = None
    last_tradeable_date: Optional[datetime] = None
    first_delivery_date: Optional[date] = None
    last_delivery_date: Optional[date] = None
    underlying_coupon: Optional[Decimal] = None
    description: Optional[str] = None
    refinitiv_ticker: Optional[str] = None
    bloomberg_ticker: Optional[str] = None
    bloomberg_suffix: Optional[str] = None
    column_fields_override: Optional[Json] = None
    seasonal_factor_close: Optional[Decimal] = None
    seasonal_factor_early_close: Optional[Decimal] = None

    @field_validator("column_fields_override", mode="before")
    @classmethod
    def _parse_json_dict(cls, data):
        if data:
            if isinstance(data, str):
                data = orjson.loads(data)

            if isinstance(data, dict):
                data = {k.lower().strip().replace(" ", "_"): v for k, v in data.items()}
                return orjson.dumps(data)
            else:
                raise ValueError(
                    "column_fields_override should be a key value collection (a dictionary)"
                )
        else:
            return None

    @field_validator("column_fields_override", mode="after")
    @classmethod
    def _parse_json_dict_after(cls, data):
        if data:
            data = {k.lower().strip().replace(" ", "_"): v for k, v in data.items()}
            return json.dumps(data)
        else:
            return None


class CompleteInstrument(BaseModel):
    """Model representing a CompleteInstrument in the database.

    The view includes the tags and the roll_constant.
    """

    instrument: str
    product: str
    product_type: str
    first_tradeable_date: Optional[date] = None
    last_tradeable_date: Optional[datetime] = None
    first_delivery_date: Optional[date] = None
    last_delivery_date: Optional[date] = None
    underlying_coupon: Optional[Decimal] = None
    description: Optional[str] = None
    refinitiv_ticker: Optional[str] = None
    bloomberg_ticker: Optional[str] = None
    bloomberg_suffix: Optional[str] = None
    column_fields_override: Optional[Json] = None
    seasonal_factor_close: Optional[Decimal] = None
    seasonal_factor_early_close: Optional[Decimal] = None
    roll_constant: Optional[Decimal] = None
    tags: Optional[list[str]] = None

    @field_validator("column_fields_override", mode="before")
    @classmethod
    def _parse_json_dict(cls, data):
        if data:
            if isinstance(data, str):
                data = orjson.loads(data)

            if isinstance(data, dict):
                data = {k.lower().strip().replace(" ", "_"): v for k, v in data.items()}
                return orjson.dumps(data)
            else:
                raise ValueError(
                    "column_fields_override should be a key value collection (a dictionary)"
                )
        else:
            return None

    @field_validator("column_fields_override", mode="after")
    @classmethod
    def _parse_json_dict_after(cls, data):
        if data:
            data = {k.lower().strip().replace(" ", "_"): v for k, v in data.items()}
            return json.dumps(data)
        else:
            return None


class CustomIndex(BaseModel):
    """Model representing a custom instrument in the database.

    It is the view that has custom_index, description, class_name with the tags it appears on.
    """

    custom_index: str
    description: str
    class_name: str
    tags: Optional[list[str]] = None


class Driver(BaseModel):  #
    """Abstraction of driver_legs and drivers."""

    driver: str
    description: str
    dtype: str
    legs: Optional[list[dict[str, Any]]] = None
    tags: Optional[list[str]] = None
    store: Optional[bool] = False


class DriverLeg(BaseModel):  # table driver_legs
    """Model representing a DriverLeg in the database."""

    driver: str
    weight: Decimal
    instrument: str
    attr: str
    roll_method: Optional[str] = None


class BaseDriver(BaseModel):  # table drivers
    """Model representing a Driver in the database."""

    driver: str
    dtype: str
    description: str
    tags: Optional[list[str]] = None


class Event(BaseModel):
    """Model representing an Event in the database."""

    start_date: date
    start_dt: Optional[datetime] = None
    end_date: date
    end_dt: Optional[datetime] = None
    event_name: Optional[str] = None
    event_category: str
    event_subcategory: Optional[str] = None
    description: Optional[str] = None
    event_analysis: Optional[str] = None
    other_information: Optional[Json] = None
    event_short_name: Optional[str] = None
    event_origin: Optional[str] = None
    peak_date: Optional[date] = None
    peak_dt: Optional[datetime] = None
    credit_tag: Optional[str] = None

    @field_validator("start_date", "end_date", "peak_date", mode="before")
    @classmethod
    def _parse_dates(cls, dt):
        if isinstance(dt, str):
            return pd.Timestamp(dt).date().isoformat()
        return dt

    @field_validator("start_dt", "end_dt", "peak_dt", mode="before")
    @classmethod
    def _parse_datetime(cls, dt):
        if isinstance(dt, str):
            return pd.Timestamp(dt).isoformat()
        return dt

    @field_validator("other_information", mode="before")
    @classmethod
    def _parse_json(cls, data):
        if data:
            if isinstance(data, str):
                data = orjson.loads(data)

            if isinstance(data, dict):
                data = {k.lower().strip().replace(" ", "_"): v for k, v in data.items()}
                return orjson.dumps(data)
            else:
                raise ValueError(
                    "other_information should be a key value collection (a dictionary)"
                )
        else:
            return None

    @field_validator("other_information", mode="after")
    @classmethod
    def _parse_json_after(cls, data):
        if data:
            data = {k.lower().strip().replace(" ", "_"): v for k, v in data.items()}
            return json.dumps(data)
        else:
            return None


class EventID(Event):
    """Model with the EventID."""

    id: int


class InstrumentDeliverable(BaseModel):
    """Model representing an InstrumentDeliverable in the database."""

    instrument: str
    product: str
    product_type: str
    dtime: Optional[date] = None
    isin: str
    bond_name: Optional[str] = None
    cusip: Optional[str] = None
    coupon: Optional[Decimal] = None
    maturity: Optional[date] = None
    left_wing: Optional[str] = None  # REFERENCES info.bonds,
    right_wing: Optional[str] = None  # REFERENCES info.bonds,
    issue_date: Optional[date] = None
    issuer: Optional[str] = None
    coupon_type: Optional[str] = None
    coupon_formula: Optional[str] = None
    day_count_convention: Optional[str] = None
    ammount_issued: Optional[Decimal] = None
    ammount_outstanding: Optional[Decimal] = None
    first_accrual_date: Optional[date] = None
    first_coupon_date: Optional[date] = None
    country_of_risk: Optional[str] = None
    currency: Optional[str] = None
    coupon_freq: Optional[Decimal] = None  # numeric
    conversion_factor: Optional[Decimal] = None
    has_been_cheapest: Optional[bool] = None
    first_tradeable_date: Optional[date] = None
    last_tradeable_date: Optional[datetime] = None
    first_delivery_date: Optional[date] = None
    last_delivery_date: Optional[date] = None
    underlying_coupon: Optional[Decimal] = None
    description: Optional[str] = None
    refinitiv_ticker: Optional[str] = None
    bloomberg_ticker: Optional[str] = None
    bloomberg_suffix: Optional[str] = None
    roll_constant: Optional[Decimal] = None
    tags: Optional[list[str]] = None


class Deliverable(BaseModel):
    """Model representing a Deliverable in the database."""

    instrument: str
    dtime: Optional[date] = None
    isin: str
    bond_name: Optional[str] = None
    cusip: Optional[str] = None
    coupon: Optional[Decimal] = None
    maturity: Optional[date] = None
    left_wing: Optional[str] = None  # REFERENCES info.bonds,
    right_wing: Optional[str] = None  # REFERENCES info.bonds,
    issue_date: Optional[date] = None
    issuer: Optional[str] = None
    coupon_type: Optional[str] = None
    coupon_formula: Optional[str] = None
    day_count_convention: Optional[str] = None
    ammount_issued: Optional[Decimal] = None
    ammount_outstanding: Optional[Decimal] = None
    first_accrual_date: Optional[date] = None
    first_coupon_date: Optional[date] = None
    country_of_risk: Optional[str] = None
    currency: Optional[str] = None
    coupon_freq: Optional[Decimal] = None  # numeric
    conversion_factor: Optional[Decimal] = None
    has_been_cheapest: Optional[bool] = None


class CheapestDeliverable(BaseModel):
    """Model representing a CheapestDeliverable in the database."""

    instrument: str
    dtime: date
    cheapest: Optional[str] = None
    cheapest_fixed: Optional[str] = None


class Bond(BaseModel):
    """Model representing a Bond in the database."""

    isin: str
    bond_name: Optional[str] = None
    cusip: Optional[str] = None
    coupon: Optional[Decimal] = None
    maturity: Optional[date] = None
    left_wing: Optional[str] = None  # REFERENCES info.bonds,
    right_wing: Optional[str] = None  # REFERENCES info.bonds,
    issue_date: Optional[date] = None
    issuer: Optional[str] = None
    coupon_type: Optional[str] = None
    coupon_formula: Optional[str] = None
    day_count_convention: Optional[str] = None
    ammount_issued: Optional[Decimal] = None
    ammount_outstanding: Optional[Decimal] = None
    first_accrual_date: Optional[date] = None
    first_coupon_date: Optional[date] = None
    country_of_risk: Optional[str] = None
    currency: Optional[str] = None
    coupon_freq: Optional[Decimal] = None  # numeric


class EcoRelease(BaseModel):
    """Model representing an EcoRelease in the database."""

    instrument: str
    product: str
    product_type: str
    download_polls: Optional[str] = None
    description: Optional[str] = None
    bloomberg_ticker: Optional[str] = None
    bloomberg_suffix: Optional[str] = None
    old_name: Optional[str] = None
    country: Optional[str] = None
    last_release_ticker: Optional[str] = None
    polls_median_ticker: Optional[str] = None
    polls_mean_ticker: Optional[str] = None
    polls_low_ticker: Optional[str] = None
    polls_high_ticker: Optional[str] = None
    first_release_ticker: Optional[str] = None
    frequency: Optional[str] = None
    eod_source: Optional[str] = None


class Tag(BaseModel):
    """Model representing a tag in the database.

    It is the view that has product,product_type pairs, instruments, strategy_filters, custom_instrument_filters
    and the lists of filtered strategies, filtered custom instruments and inherited instruments.
    """

    tag: str
    products: Optional[Json] = None
    instruments: Optional[list[str]] = None
    strategy_filters: Optional[list[str]] = None
    strategies: Optional[list[str]] = None
    custom_instrument_filters: Optional[list[str]] = None
    custom_instruments: Optional[list[str]] = None
    inherited_instruments: Optional[list[str]] = None

    @field_validator("products", mode="before")
    @classmethod
    def _parse_json(cls, data):
        return orjson.dumps(data) if data else None


class TagBase(BaseModel):
    """Model representing a tag in the database.

    The 'Tag' table has a single column 'tag' which is the primary key.
    """

    tag: str


class TagProduct(BaseModel):
    """Model representing the relationship between a tag and a product.

    The 'TagProduct' table has a composite primary key ('tag', 'product', 'product_type').
    This model represents a row linking a tag with a product and its type.

    Attributes:
        tag (str): The tag name.
        product (str): The product name.
        product_type (str): The product type.

    """

    tag: str
    product: str
    product_type: str


class TagInstrument(BaseModel):
    """Model representing the relationship between a tag and an instrument.

    The 'TagInstrument' table has a composite primary key ('tag', 'instrument').
    This model represents a row linking a tag with an instrument.

    Attributes:
        tag (str): The tag name.
        instrument (str): The instrument name.

    """

    tag: str
    instrument: str


class TagStrategy(BaseModel):
    """Model representing the relationship between a tag and a strategy filter.

    The 'TagStrategy' table has a composite primary key ('tag', 'strategy_filter').
    This model represents a row linking a tag with a strategy filter.

    Attributes:
        tag (str): The tag name.
        strategy_filter (str): The regex to filter strategies by.

    """

    tag: str
    strategy_filter: str


class TagCustomInstruments(BaseModel):
    """Model representing the relationship between a tag and a custom instrument filter.

    The 'TagCustomInstruments' table has a composite primary key ('tag', 'custom_instrument_filter').
    This model represents a row linking a tag with a custom instrument filter.

    Attributes:
        tag (str): The tag name.
        custom_instrument_filter (str): The regex to filter custom instruments by.

    """

    tag: str
    custom_instrument_filter: str


### MARKET DATA ###


class QuoteTick(BaseModel):
    """Model representing a QuoteTick."""

    dtime: datetime = Field(description="Datetime of the quote")
    bid_price0: Decimal = Field(description="Bid price of the quote")
    ask_price0: Decimal = Field(description="Ask price of the quote")
    bid_size0: int = Field(description="Bid size of the quote")
    ask_size0: int = Field(description="Ask size of the quote")


class TradeTick(BaseModel):
    """Model representing a TradeTick."""

    dtime: datetime = Field(description="Datetime of the trade")
    trade_price: Decimal = Field(description="Trade price of the trade")
    trade_size: int = Field(description="Trade size of the trade")
    aggressor: str = Field(description="Aggressor of the trade")
    exch_trade_id: str = Field(description="Exchange trade id of the trade")


class QuoteBar(BaseModel):
    """Model representing a QuoteBar."""

    dtime: datetime = Field(description="Datetime of the quote bar")
    bid_open: Decimal = Field(description="Bid open of the quote bar")
    bid_high: Decimal = Field(description="Bid high of the quote bar")
    bid_low: Decimal = Field(description="Bid low of the quote bar")
    bid_close: Decimal = Field(description="Bid close of the quote bar")
    bid_volume: int = Field(description="Bid volume of the quote bar")
    ask_open: Decimal = Field(description="Ask open of the quote bar")
    ask_high: Decimal = Field(description="Ask high of the quote bar")
    ask_low: Decimal = Field(description="Ask low of the quote bar")
    ask_close: Decimal = Field(description="Ask close of the quote bar")
    ask_volume: int = Field(description="Ask volume of the quote bar")


class TradeBar(BaseModel):
    """Model representing a TradeBar."""

    dtime: datetime = Field(description="Datetime of the trade bar")
    trade_open: Decimal = Field(description="Trade open of the trade bar")
    trade_high: Decimal = Field(description="Trade high of the trade bar")
    trade_low: Decimal = Field(description="Trade low of the trade bar")
    trade_close: Decimal = Field(description="Trade close of the trade bar")
    trade_volume: int = Field(description="Trade volume of the trade bar")


class EODBar(BaseModel):
    """Model representing an EODBar."""

    pass


class MarketData(BaseModel):
    """Model representing MarketData in the database."""

    dtime: datetime
    bid_price0: Optional[str] = None
    ask_price0: Optional[str] = None
    bid_size0: Optional[int] = None
    ask_size0: Optional[int] = None
    trade_price: Optional[str] = None
    trade_size: Optional[int] = None
    aggressor: Optional[str] = None
    exch_trade_id: Optional[str] = None


class LoadOptions(BaseModel):
    """Options for loading data."""

    roll_method: Optional[RollMethods] = None
    """Optional[RollMethods]: Method to handle roll logic. Defaults to None."""

    all_events: Optional[bool] = False
    """Optional[bool]: If True, includes all events. Defaults to False."""

    cheapest_filter: Optional[Literal["cheapest", "cheapest_fixed", "all"]] = (
        "cheapest_fixed"
    )
    """Optional[Literal["cheapest", "cheapest_fixed", "all"]]: 
		Filter to select instruments. Defaults to "cheapest_fixed"."""


class LoadRequest(BaseModel):
    """Request model for data loading."""

    dtype: str
    """str: Type of data to load. Required."""

    ticker: Optional[str] = None
    """Optional[str]: Specific instrument ticker to load. Defaults to None."""

    from_date: Optional[datetime | date] = None
    """Optional[datetime | date]: Start of the date range. Defaults to None."""

    to_date: Optional[datetime | date] = None
    """Optional[datetime | date]: End of the date range. Defaults to None."""

    freq: timedelta = None
    """timedelta: Frequency for the data. Required."""

    filters: Optional[dict] = None
    """Optional[dict]: Additional filter criteria. Defaults to None."""

    build: Optional[bool] = False
    """Optional[bool]: Whether to trigger a build process. Defaults to False."""

    options: Optional[LoadOptions] = LoadOptions()
    """Optional[LoadOptions]: Advanced loading options. Defaults to a new LoadOptions()."""

    @field_validator("from_date", "to_date", mode="before")
    @classmethod
    def _parse_datetime(cls, ts):
        return pd.Timestamp(ts) if ts else None

    @field_validator("freq", mode="before")
    @classmethod
    def _parse_freq(cls, freq):
        return pd.Timedelta(freq) if freq else None


class AuditTrailLoadRequest(LoadRequest):
    """Request model for Audit Trail data loading."""

    dtype: str = "tt_audittrail"
    account: Optional[str] = None
    originator_email: Optional[str] = None
    instrument: Optional[str] = None
    product: Optional[str] = None
    exchange: Optional[str] = None
    message_type: Optional[str] = None
    execution_type: Optional[str] = None
    tt_order_id: Optional[str] = None
    tt_parent_id: Optional[str] = None
    custom_filter: Optional[str] = None
    limit: Optional[int] = None
    default_filter_attributes: list[str] = [
        "account",
        "originator_email",
        "instrument",
        "product",
        "exchange",
        "message_type",
        "execution_type",
        "tt_order_id",
        "tt_parent_id",
    ]


class AccessLevel(IntEnum):
    """Model representing the AccessLevel in the database."""

    NONE = 0
    READ = 1
    WRITE = 2
    ADMIN = 3

    @staticmethod
    def _from_str(level: str):
        if level.lower() == "none":
            return AccessLevel.NONE
        if level.lower() == "read":
            return AccessLevel.READ
        elif level.lower() == "write":
            return AccessLevel.WRITE
        elif level.lower() == "admin":
            return AccessLevel.ADMIN
        else:
            raise ValueError(f"Invalid access level: {level}")

    @staticmethod
    def _to_str(level):
        if level == AccessLevel.NONE:
            return "none"
        elif level == AccessLevel.READ:
            return "read"
        elif level == AccessLevel.WRITE:
            return "write"
        elif level == AccessLevel.ADMIN:
            return "admin"


class LogRecord(BaseModel):
    """Model representing a LogRecord in the database."""

    id: int
    dtime: datetime
    action_type: str
    user_name: str
    schema_name: str
    table_name: str
    old_data: Optional[str | dict] = None
    new_data: Optional[str | dict] = None

    @field_validator("dtime", mode="before")
    @classmethod
    def _parse_datetime(cls, ts):
        return pd.Timestamp(ts) if ts else None

    @field_validator("old_data", "new_data", mode="before")
    @classmethod
    def _parse_json(cls, data):
        return orjson.dumps(data) if data else None

    @model_validator(mode="before")
    @classmethod
    def _check_at_leat_one(cls, field_values):
        old_data = field_values.get("old_data")
        new_data = field_values.get("new_data")
        if not old_data and not new_data:
            raise ValueError("At least one of old_data and new_data must be not None")
        return field_values


class UserStr(BaseModel):
    """Model representing a LogRecord in the database."""

    username: str
    name: str
    products: Optional[str] = "read"
    instruments: Optional[str] = "read"
    drivers: Optional[str] = "read"
    users: Optional[str] = "none"
    exchanges: Optional[str] = "read"
    families: Optional[str] = "read"
    subfamilies: Optional[str] = "read"
    frontend_log: Optional[str] = "read"
    events: Optional[str] = "read"
    columns: Optional[str] = "read"
    spreads: Optional[str] = "read"
    market: Optional[str] = "read"
    tags: Optional[str] = "read"

    @field_validator(
        "products",
        "instruments",
        "drivers",
        "users",
        "exchanges",
        "families",
        "subfamilies",
        "frontend_log",
        "events",
        "columns",
        "spreads",
        "market",
        "tags",
        mode="before",
    )
    @classmethod
    def _parse_access_level(cls, level):
        if isinstance(level, int):
            return AccessLevel._to_str(AccessLevel(level))
        elif isinstance(level, str):
            return level


class User(BaseModel):
    """Model representing a User in the database."""

    username: str
    name: str
    products: AccessLevel
    instruments: AccessLevel
    drivers: AccessLevel
    users: AccessLevel
    exchanges: AccessLevel
    families: AccessLevel
    subfamilies: AccessLevel
    frontend_log: AccessLevel
    events: AccessLevel
    columns: AccessLevel
    spreads: AccessLevel
    market: AccessLevel
    tags: AccessLevel

    @field_validator(
        "products",
        "instruments",
        "drivers",
        "users",
        "exchanges",
        "families",
        "subfamilies",
        "frontend_log",
        "events",
        "columns",
        "spreads",
        "market",
        "tags",
        mode="before",
    )
    @classmethod
    def _parse_access_level(cls, level):
        if isinstance(level, AccessLevel):
            return level
        elif isinstance(level, str):
            return AccessLevel._from_str(level)
        elif isinstance(level, int):
            return AccessLevel(level)


# class T2TLoadRequest(LoadRequest):
#     from_date: datetime | date | str = pd.Timestamp.now() - pd.Timedelta(days=1)
#     to_date: datetime | date | str = pd.Timestamp.now()


# class IntradayLoadRequest(LoadRequest):
#     from_date: datetime | date | str = pd.Timestamp.now() - pd.Timedelta(days=1)
#     to_date: datetime | date | str = pd.Timestamp.now()

# class RealTimeLoadRequest(LoadRequest):
#     pass


class AuditTrailFTPFile(BaseModel):
    """Model representing a AuditTrailFTPFile in the database."""

    file_name: str
    insertion_time: Optional[datetime | str] = None
    num_rows: Optional[int] = None


class Accounts(BaseModel):
    """Model representing an Accounts list in the database."""

    accounts: list[str]
    insertion_times: Optional[list[datetime | str]] = None


class Account(BaseModel):
    """Model representing an Account in the database."""

    account: str
    insertion_time: Optional[datetime | str] = None


class Column(BaseModel):
    """Model representing a Column in the database."""

    column_name: str
    description: Optional[str] = None
    field_tt: Optional[str] = None
    field_bloomberg: Optional[str] = None
    field_refinitiv: Optional[str] = None
    field_rjo: Optional[str] = None
    field_wb: Optional[str] = None


class SpreadLeg(BaseModel):
    """Model representing a SpreadLeg in the database."""

    instrument: str
    execution_id: Optional[int] = None
    weight_spread: Optional[Decimal] = None
    weight_price: Optional[Decimal] = None
    weight_yield: Optional[Decimal] = None
    weight_yield_th: Optional[Decimal] = None
    weight_yield_cmt: Optional[Decimal] = None
    is_lean_indicative: Optional[bool] = None
    is_hedging: Optional[bool] = None
    min_lean_qty: Optional[str] = None
    payup_ticks: Optional[Decimal] = None
    queue_holder_orders: Optional[int] = None
    hedge_qty: Optional[int] = None
    lean_qty: Optional[int] = None
    active_quoting: Optional[bool] = None
    tt_account: Optional[str] = None
    quote_max_aggr: Optional[Decimal] = None


class SpreadExecution(BaseModel):
    """Model representing a SpreadExecution in the database."""

    spread: str
    execution_id: Optional[int] = None  # autoincremental
    legs: list[SpreadLeg]
    quoters: Optional[list[str]] = None
    scenarios: Optional[list[str]] = None
    ticket_parameters: Optional[dict] = None


class Spread(BaseModel):  #
    """Abstraction of spread and spread_legs."""

    arfima_name: str
    executions: list[SpreadExecution]
    violet_name: Optional[str] = None
    name_short: Optional[str] = None
    auto_scalper_name: Optional[str] = None
    scalper_mode: Optional[str] = None
    violet_display_name: Optional[str] = None
    violet_portfolio_name: Optional[str] = None
    master_portfolio_name: Optional[str] = None
    constant: Optional[Decimal] = None
    constant_yield: Optional[Decimal] = None
    constant_yield_th: Optional[Decimal] = None
    tick_size: Optional[Decimal] = None
    tt_formula: Optional[str] = None
    currency: Optional[str] = None
    market_scale_factor: Optional[Decimal] = None
    fees: Optional[Decimal] = None
    dv01: Optional[Decimal] = None
    pv01: Optional[Decimal] = None
    rules: Optional[list[str]] = None
    rules_sheet: Optional[str] = None
    using_user_defined_ticksize: Optional[bool] = None
    user_defined_numerator: Optional[Decimal] = None
    user_defined_denominator: Optional[Decimal] = None
    violet_ticket_name: Optional[str] = None
    qm_parameters: Optional[dict] = None
    rs_parameters: Optional[dict] = None

    @field_validator("executions", mode="before")
    @classmethod
    def _check_executions(cls, executions, info):
        spread_name = info.data.get("arfima_name")
        for execution in executions:
            execution["spread"] = spread_name
        return executions


class CompleteSpread(Spread, BaseModel):
    """Abstraction of Product with list of tags."""

    tags: Optional[list[str]] = None
