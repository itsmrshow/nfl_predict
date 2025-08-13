import pandas as pd
import numpy as np
import nfl_data_py as nfl
from sqlalchemy import text
from config import DB_SCHEMA, LINES_BOOK_FILTER
from utils import coerce_numeric

def _normalize_book_name(s: str) -> str:
    if not isinstance(s, str): return ""
    b = s.strip(); low = b.casefold()
    mapping = {
        "draftkings":"DraftKings","draft kings":"DraftKings","dk":"DraftKings",
        "fanduel":"FanDuel","fan duel":"FanDuel","fd":"FanDuel",
        "fanatics":"Fanatics","fanatics sportsbook":"Fanatics","betfanatics":"Fanatics",
    }
    if low in mapping: return mapping[low]
    if "draft" in low and "king" in low: return "DraftKings"
    if "fan" in low and "duel" in low:  return "FanDuel"
    if "fanatic" in low:                 return "Fanatics"
    return b

def load_vegas_lines(years: list[int], schedule: pd.DataFrame) -> pd.DataFrame:
    lines = pd.DataFrame()
    try:
        if hasattr(nfl, "import_betting_lines"):
            lines = nfl.import_betting_lines(years)
        elif hasattr(nfl, "import_lines"):
            lines = nfl.import_lines(years)
        else:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

    if lines.empty:
        return lines

    cols = {c.lower(): c for c in lines.columns}
    def pick(*names):
        for n in names:
            if n in cols: return cols[n]
        return None

    season_col = pick("season")
    week_col   = pick("week","week_number")
    home_col   = pick("home_team","team_home","home")
    away_col   = pick("away_team","team_away","away")
    book_col   = pick("provider","book","sportsbook")
    spread_o   = pick("spread_open","spreadline_open","spread_opening")
    spread_c   = pick("spread_close","spreadline_close","spread_closing","spread")
    total_o    = pick("total_open","total_opening")
    total_c    = pick("total_close","total_closing","total")
    fav_col    = pick("favorite","favorite_team")
    hm_ml_col  = pick("home_moneyline","ml_home")
    aw_ml_col  = pick("away_moneyline","ml_away")
    ts_col     = pick("timestamp","line_timestamp","updated_at")

    out = pd.DataFrame()
    def maybe_set(col, src):
        out[col] = lines[src] if isinstance(src,str) and src in lines.columns else np.nan

    for col, name in [
        ("season", season_col), ("week", week_col),
        ("home_team", home_col), ("away_team", away_col),
        ("book", book_col), ("spread_open", spread_o), ("spread_close", spread_c),
        ("total_open", total_o), ("total_close", total_c),
        ("favorite_team", fav_col),
        ("home_moneyline", hm_ml_col), ("away_moneyline", aw_ml_col),
        ("line_timestamp", ts_col),
    ]:
        maybe_set(col, name)

    # join game_id
    sched = schedule[['season','week','home_team','away_team','game_id']].drop_duplicates()
    out = out.merge(sched, on=['season','week','home_team','away_team'], how='left')

    out['book'] = out['book'].astype(str).str.strip().map(_normalize_book_name)
    if LINES_BOOK_FILTER:
        wanted = { _normalize_book_name(b) for b in LINES_BOOK_FILTER }
        out = out[out['book'].isin(wanted)]

    coerce_numeric(out, ['spread_open','spread_close','total_open','total_close'])
    for c in ['home_moneyline','away_moneyline']:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors='coerce').astype('Int64')
    out['line_timestamp'] = pd.to_datetime(out['line_timestamp'], errors='coerce')

    out = out.dropna(subset=['game_id'])
    if 'line_source' not in out.columns:
        out['line_source'] = pd.NA

    cols = ['game_id','season','week','book','home_team','away_team','favorite_team',
            'spread_open','spread_close','total_open','total_close',
            'home_moneyline','away_moneyline','line_source','line_timestamp']
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out[cols]

def upsert_lines(engine, df: pd.DataFrame):
    if df.empty:
        return
    cols = list(df.columns)
    tmp = "tmp_lines"
    from db import copy_from_dataframe
    with engine.begin() as con:
        con.execute(text(f"CREATE TEMP TABLE {tmp} (LIKE {DB_SCHEMA}.dim_vegas_lines INCLUDING DEFAULTS) ON COMMIT DROP;"))
        copy_from_dataframe(con, df[cols], tmp)
        con.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.dim_vegas_lines ({",".join(cols)})
            SELECT {",".join(cols)} FROM {tmp}
            ON CONFLICT (game_id, book, line_timestamp) DO NOTHING;
        """))
