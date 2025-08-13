import pandas as pd
import requests
from urllib.parse import urlencode
from sqlalchemy import text
from config import THEODDS_API_KEY, PROPS_BOOKS, PROPS_MARKETS, SPORT_KEY, DB_SCHEMA
from teams import team_alias_map
from utils import mk_game_id
from db import copy_from_dataframe

def _theodds_events(api_key:str) -> list[dict]:
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events/?{urlencode({'apiKey': api_key, 'regions':'us'})}"
    r = requests.get(url, timeout=30); r.raise_for_status()
    return r.json()

def _theodds_event_props(api_key:str, event_id:str, markets:list[str]) -> dict:
    params = {'apiKey': api_key, 'regions':'us', 'markets':','.join(markets), 'oddsFormat':'american'}
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events/{event_id}/odds/?{urlencode(params)}"
    r = requests.get(url, timeout=30); r.raise_for_status()
    return r.json()

def fetch_player_props_from_theodds(years: list[int], schedule: pd.DataFrame) -> pd.DataFrame:
    if not THEODDS_API_KEY:
        return pd.DataFrame()

    team_map = team_alias_map()
    events = _theodds_events(THEODDS_API_KEY)
    if not events:
        return pd.DataFrame()

    rows = []
    sched = schedule[['season','week','home_team','away_team','game_date']].copy()
    sched['game_id'] = schedule['game_id']

    for ev in events:
        event_id = ev.get('id')
        commence = pd.to_datetime(ev.get('commence_time'), errors='coerce', utc=True)
        home_name = (ev.get('home_team') or "").strip().lower()
        away_name = (ev.get('away_team') or "").strip().lower()
        home = team_map.get(home_name); away = team_map.get(away_name)
        if not home or not away:
            continue

        # try to locate the scheduled game row
        date_key = commence.date() if pd.notna(commence) else None
        cand = schedule[(schedule['home_team']==home)&(schedule['away_team']==away)]
        if date_key:
            lo = date_key - pd.Timedelta(days=2)
            hi = date_key + pd.Timedelta(days=2)
            cand = cand[(cand['game_date'].dt.date >= lo) & (cand['game_date'].dt.date <= hi)]
        if cand.empty:
            cand = schedule[(schedule['home_team']==home)&(schedule['away_team']==away)]
        if cand.empty:
            continue

        season = int(cand.iloc[0]['season']); week = int(cand.iloc[0]['week'])
        game_id = cand.iloc[0]['game_id']

        try:
            ev_odds = _theodds_event_props(THEODDS_API_KEY, event_id, PROPS_MARKETS)
        except Exception:
            continue

        for bk in ev_odds.get('bookmakers', []):
            book_name = (bk.get('title') or "").strip()
            if PROPS_BOOKS and book_name.lower() not in PROPS_BOOKS:
                continue
            for m in bk.get('markets', []):
                market_key = m.get('key')
                # Some APIs provide separate Over/Under rows under outcomes
                pool = {}
                for out in m.get('outcomes', []):
                    player_name = out.get('description') or out.get('name') or ""
                    line_value  = out.get('point')
                    price       = out.get('price')
                    side        = (out.get('name') or "").lower()
                    d = pool.setdefault((player_name, line_value), {"over":None,"under":None})
                    if 'over' in side:  d["over"]  = price
                    if 'under' in side: d["under"] = price
                for (player_name, line_value), both in pool.items():
                    rows.append({
                        "game_id": game_id, "season": season, "week": week,
                        "book": book_name, "player_name": player_name,
                        "market": market_key, "line_value": line_value,
                        "over_odds": both["over"], "under_odds": both["under"],
                        "ts": pd.Timestamp.utcnow()
                    })

    df = pd.DataFrame(rows)
    if df.empty: return df

    # seasonweek for easier slicing in BI
    df['seasonweek'] = df['season']*100 + df['week']
    cols = ["game_id","season","week","seasonweek","book","player_id","player_name","market","line_value","over_odds","under_odds","ts"]
    for c in cols:
        if c not in df.columns: df[c] = None
    return df[cols]

def upsert_player_props(engine, props_df: pd.DataFrame):
    if props_df.empty:
        return
    cols = list(props_df.columns)
    tmp = "tmp_prop_lines"
    with engine.begin() as con:
        con.execute(text(f"""
            CREATE TEMP TABLE {tmp} (
              game_id text, season int, week int, seasonweek int,
              book text, player_id text, player_name text,
              market text, line_value numeric, over_odds numeric, under_odds numeric, ts timestamptz
            ) ON COMMIT DROP;
        """))
        copy_from_dataframe(con, props_df[cols], tmp)
        con.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.fact_player_prop_lines ({",".join(cols)})
            SELECT {",".join(cols)} FROM {tmp}
            ON CONFLICT (game_id, book, player_name, market, ts) DO NOTHING;
        """))
