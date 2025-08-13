import numpy as np
import pandas as pd
from sqlalchemy import text
from config import YEARS, DB_SCHEMA
from db import copy_from_dataframe
from utils import coerce_numeric

def build_fact_all(wk: pd.DataFrame) -> pd.DataFrame:
    grp = ['game_id','season','week','team','opponent','time_slot','player_id','player_name','position']

    mean_cols = []
    def maybe(c): 
        if c in wk.columns: mean_cols.append(c)
    for c in ['passing_yards','passing_tds','interceptions','attempts','completions']: maybe(c)
    for c in ['rushing_yards','rushing_tds','carries']: maybe(c)
    for c in ['receptions','receiving_yards','receiving_tds']: maybe(c)
    for c in ['sacks','fumbles_recovered']: maybe(c)
    if 'total_touchdowns' in wk.columns: maybe('total_touchdowns')

    agg = {c:'mean' for c in mean_cols}
    g = wk.groupby(grp, as_index=False).agg(agg)

    rename = {
        'passing_yards':'passing_yards_avg','passing_tds':'passing_tds_avg','interceptions':'interceptions_avg',
        'attempts':'attempts_avg','completions':'completions_avg',
        'rushing_yards':'rushing_yards_avg','rushing_tds':'rushing_tds_avg','carries':'carries_avg',
        'receptions':'receptions_avg','receiving_yards':'receiving_yards_avg','receiving_tds':'receiving_tds_avg',
        'sacks':'sacks_avg','fumbles_recovered':'fumbles_recovered_avg','total_touchdowns':'total_touchdowns_avg',
    }
    g = g.rename(columns=rename)

    if 'interceptions_avg' in g.columns:
        g['def_interceptions_avg'] = np.where(g['position']=='DEF', g['interceptions_avg'], np.nan)

    g = g.rename(columns={'team':'team_abbr','opponent':'opponent_abbr'})
    g['games_played'] = 1
    g['season_range'] = f"{min(YEARS)}–{max(YEARS)}"
    g['current_roster_only'] = False  # will be set by caller if needed

    expected = [
        'game_id','season','week','team_abbr','opponent_abbr','time_slot','player_id','player_name','position',
        'passing_yards_avg','passing_tds_avg','interceptions_avg','attempts_avg','completions_avg',
        'rushing_yards_avg','rushing_tds_avg','carries_avg',
        'receptions_avg','receiving_yards_avg','receiving_tds_avg',
        'sacks_avg','def_interceptions_avg','fumbles_recovered_avg',
        'total_touchdowns_avg','games_played','season_range','current_roster_only'
    ]
    for c in expected:
        if c not in g.columns:
            g[c] = np.nan
    return g[expected]

def upsert_fact(engine, fact: pd.DataFrame):
    if fact.empty:
        return
    cols = list(fact.columns)
    num_cols = [
        "passing_yards_avg","passing_tds_avg","interceptions_avg","attempts_avg","completions_avg",
        "rushing_yards_avg","rushing_tds_avg","carries_avg",
        "receptions_avg","receiving_yards_avg","receiving_tds_avg",
        "sacks_avg","def_interceptions_avg","fumbles_recovered_avg","total_touchdowns_avg"
    ]
    coerce_numeric(fact, num_cols)
    fact["games_played"] = pd.to_numeric(fact["games_played"], errors="coerce").fillna(0).astype(int)
    fact["current_roster_only"] = fact["current_roster_only"].astype(bool)

    tmp = f"tmp_fact_player_timeslot"
    with engine.begin() as con:
        con.execute(text(f"CREATE TEMP TABLE {tmp} (LIKE {DB_SCHEMA}.fact_player_timeslot INCLUDING DEFAULTS) ON COMMIT DROP;"))
        copy_from_dataframe(con, fact[cols], tmp)
        con.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.fact_player_timeslot ({",".join(cols)})
            SELECT {",".join(cols)} FROM {tmp}
            ON CONFLICT (game_id, season, week, team_abbr, opponent_abbr, time_slot, player_id, position)
            DO NOTHING;
        """))
