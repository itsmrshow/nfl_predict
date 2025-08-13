import pandas as pd
import nfl_data_py as nfl
from sqlalchemy import text
from config import DB_SCHEMA

def load_reference():
    teams = nfl.import_team_desc()
    upsert = teams[['team_abbr','team_name']].drop_duplicates()
    return teams, upsert

def upsert_dim_team(engine, teams_df: pd.DataFrame):
    if teams_df.empty:
        return
    with engine.begin() as con:
        rows = teams_df[['team_abbr','team_name']].drop_duplicates().to_dict(orient='records')
        sql = f"""
        INSERT INTO {DB_SCHEMA}.dim_team (team_abbr, team_name)
        VALUES (:team_abbr, :team_name)
        ON CONFLICT (team_abbr) DO UPDATE SET team_name = EXCLUDED.team_name;
        """
        for i in range(0, len(rows), 1000):
            con.execute(text(sql), rows[i:i+1000])

def upsert_dim_timeslot(engine):
    with engine.begin() as con:
        con.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.dim_timeslot (timeslot_key, time_slot) VALUES
            (1,'Thursday'),
            (2,'Monday'),
            (3,'Sunday Morning'),
            (4,'Sunday Early Window'),
            (5,'Sunday Late Window'),
            (6,'Sunday Night')
            ON CONFLICT (timeslot_key) DO UPDATE SET time_slot = EXCLUDED.time_slot;
        """))

def team_alias_map() -> dict[str,str]:
    try:
        teams = nfl.import_team_desc()
    except Exception:
        teams = pd.DataFrame(columns=['team_abbr','team_name'])

    name_to_abbr: dict[str, str] = {}
    if {'team_abbr','team_name'}.issubset(teams.columns):
        for _, r in teams[['team_abbr','team_name']].dropna().drop_duplicates().iterrows():
            name_to_abbr[str(r['team_name']).strip().lower()] = str(r['team_abbr']).strip().upper()

    aliases = {
        # current + common variants
        "arizona cardinals":"ARI","atlanta falcons":"ATL","baltimore ravens":"BAL","buffalo bills":"BUF",
        "carolina panthers":"CAR","chicago bears":"CHI","cincinnati bengals":"CIN","cleveland browns":"CLE",
        "dallas cowboys":"DAL","denver broncos":"DEN","detroit lions":"DET","green bay packers":"GB",
        "houston texans":"HOU","indianapolis colts":"IND","jacksonville jaguars":"JAX","kansas city chiefs":"KC",
        "las vegas raiders":"LV","los angeles chargers":"LAC","la chargers":"LAC","los angeles rams":"LAR","la rams":"LAR",
        "miami dolphins":"MIA","minnesota vikings":"MIN","new england patriots":"NE","new orleans saints":"NO",
        "new york giants":"NYG","new york jets":"NYJ","philadelphia eagles":"PHI","pittsburgh steelers":"PIT",
        "san francisco 49ers":"SF","seattle seahawks":"SEA","tampa bay buccaneers":"TB","tennessee titans":"TEN",
        "washington commanders":"WAS","washington football team":"WAS","washington redskins":"WAS",
        "oakland raiders":"LV","st. louis rams":"LAR","san diego chargers":"LAC",
        "arizona":"ARI","atlanta":"ATL","baltimore":"BAL","buffalo":"BUF","carolina":"CAR","chicago":"CHI","cincinnati":"CIN",
        "cleveland":"CLE","dallas":"DAL","denver":"DEN","detroit":"DET","green bay":"GB","houston":"HOU","indianapolis":"IND",
        "jacksonville":"JAX","kansas city":"KC","las vegas":"LV","los angeles":"LAR","miami":"MIA","minnesota":"MIN",
        "new england":"NE","new orleans":"NO","philadelphia":"PHI","pittsburgh":"PIT","san francisco":"SF",
        "seattle":"SEA","tampa bay":"TB","tennessee":"TEN","washington":"WAS",
    }
    for k, v in aliases.items():
        name_to_abbr.setdefault(k, v)
    return name_to_abbr
