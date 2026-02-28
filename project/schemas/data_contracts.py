import pandera as pa
import pandas as pd
from pandera.typing import DataFrame, Series

class Cleaned5mBarsSchema(pa.SchemaModel):
    symbol: Series[str] = pa.Field(coerce=True)
    timestamp: Series[pd.DatetimeTZDtype] = pa.Field(dtype_kwargs={"unit": "ns", "tz": "UTC"})
    open: Series[float] = pa.Field(ge=0.0, nullable=True)
    high: Series[float] = pa.Field(ge=0.0, nullable=True)
    low: Series[float] = pa.Field(ge=0.0, nullable=True)
    close: Series[float] = pa.Field(ge=0.0, nullable=True)
    volume: Series[float] = pa.Field(ge=0.0)

    @pa.dataframe_check
    def check_high_low(cls, df: DataFrame) -> Series[bool]:
        return df["high"].isna() | (df["high"] >= df["low"])

    class Config:
        strict = False  # Allow other columns like quote_volume or taker_buy_volume

class EventRegistrySchema(pa.SchemaModel):
    symbol: Series[str] = pa.Field(coerce=True)
    enter_ts: Series[int] = pa.Field(ge=1577836800000)
    exit_ts: Series[int] = pa.Field(ge=1577836800000)
    event_id: Series[str] = pa.Field(nullable=False)
    signal_column: Series[str] = pa.Field(nullable=False)

    @pa.dataframe_check
    def check_exit_after_enter(cls, df: DataFrame) -> Series[bool]:
        return df["exit_ts"] >= df["enter_ts"]

    class Config:
        strict = False

class Phase2CandidateSchema(pa.SchemaModel):
    symbol: Series[str] = pa.Field(coerce=True)
    enter_ts: Series[int] = pa.Field(ge=1577836800000)
    exit_ts: Series[int] = pa.Field(ge=1577836800000)
    event_id: Series[str] = pa.Field(nullable=False)
    q_value: Series[float] = pa.Field(ge=0.0, le=1.0)
    
    class Config:
        strict = False
