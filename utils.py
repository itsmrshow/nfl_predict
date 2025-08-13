import pandas as pd
import numpy as np

def mk_game_id(season: int, week: int, home: str, away: str) -> str:
    return f"{int(season):04d}_{int(week):02d}_{home}_{away}"

def time_slot(day: str, hr: float | int | None) -> str:
    if day is None or pd.isna(hr):
        return "Unknown"
    if day == "Thursday": return "Thursday"
    if day == "Monday":   return "Monday"
    if day == "Sunday":
        h = int(hr)
        if h < 12:       return "Sunday Morning"
        if h == 13:      return "Sunday Early Window"
        if h in (15,16): return "Sunday Late Window"
        if h >= 19:      return "Sunday Night"
    return "Unknown"

def downcast_floats(df: pd.DataFrame) -> pd.DataFrame:
    fcols = df.select_dtypes(include="float").columns
    if len(fcols):
        df[fcols] = df[fcols].apply(pd.to_numeric, downcast="float")
    return df

def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def safe_get(df, col, default=np.nan):
    return df[col] if col in df.columns else default
