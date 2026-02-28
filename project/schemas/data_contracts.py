import pandera as pa
import pandas as pd
from pandera.typing import DataFrame, Series

class Cleaned5mBarsSchema(pa.DataFrameModel):
    symbol: Series[str] = pa.Field(coerce=True)
    timestamp: Series[pd.DatetimeTZDtype] = pa.Field(dtype_kwargs={"unit": "ns", "tz": "UTC"})
    open: Series[float] = pa.Field(ge=0.0, nullable=True)
    high: Series[float] = pa.Field(ge=0.0, nullable=True)
    low: Series[float] = pa.Field(ge=0.0, nullable=True)
    close: Series[float] = pa.Field(ge=0.0, nullable=True)
    volume: Series[float] = pa.Field(ge=0.0)
    quote_volume: Series[float] = pa.Field(ge=0.0, nullable=True)
    is_gap: Series[bool] = pa.Field()
    funding_rate_realized: Series[float] = pa.Field(nullable=True)

    @pa.dataframe_check
    def check_high_low(cls, df: DataFrame) -> Series[bool]:
        return df["high"].isna() | (df["high"] >= df["low"])

    class Config:
        strict = False  # Allow other columns like quote_volume or taker_buy_volume

class EventRegistrySchema(pa.DataFrameModel):
    symbol: Series[str] = pa.Field(coerce=True)
    phenom_enter_ts: Series[int] = pa.Field(ge=1577836800000)
    enter_ts: Series[int] = pa.Field(ge=1577836800000)
    detected_ts: Series[int] = pa.Field(ge=1577836800000)
    signal_ts: Series[int] = pa.Field(ge=1577836800000)
    exit_ts: Series[int] = pa.Field(ge=1577836800000)
    event_id: Series[str] = pa.Field(nullable=False)
    signal_column: Series[str] = pa.Field(nullable=False)

    @pa.dataframe_check
    def check_exit_after_enter(cls, df: DataFrame) -> Series[bool]:
        return df["exit_ts"] >= df["enter_ts"]

    @pa.dataframe_check
    def check_detected_after_phenom(cls, df: DataFrame) -> Series[bool]:
        return df["detected_ts"] >= df["phenom_enter_ts"]

    @pa.dataframe_check
    def check_signal_after_detected(cls, df: DataFrame) -> Series[bool]:
        return df["signal_ts"] >= df["detected_ts"]

    class Config:
        strict = False

class Phase2CandidateSchema(pa.DataFrameModel):
    symbol: Series[str] = pa.Field(coerce=True)
    enter_ts: Series[int] = pa.Field(ge=1577836800000)
    exit_ts: Series[int] = pa.Field(ge=1577836800000)
    event_id: Series[str] = pa.Field(nullable=False)
    q_value: Series[float] = pa.Field(ge=0.0, le=1.0)
    
    class Config:
        strict = False
