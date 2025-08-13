import pandas as pd
import numpy as np
import nfl_data_py as nfl
from config import YEARS, CURRENT_YEAR, CURRENT_ROSTER_ONLY
from utils import time_slot, mk_game_id, downcast_floats

def load_weekly_with_timeslot(years: list[int]) -> pd.DataFrame:
    weekly = nfl.import_weekly_data(years)

    if 'team' not in weekly.columns:
        if 'recent_team' in weekly.columns: weekly = weekly.rename(columns={'recent_team':'team'})
        elif 'player_team' in weekly.columns: weekly = weekly.rename(columns={'player_team':'team'})
        else: raise KeyError("No team column in weekly data.")

    if 'opponent' not in weekly.columns:
        if 'opponent_team' in weekly.columns: weekly = weekly.rename(columns={'opponent_team':'opponent'})
        else: raise KeyError("No opponent column in weekly data.")

    for c in ['player_name','player_display_name','player']:
        if c in weekly.columns:
            weekly = weekly.rename(columns={c:'player_name'})
            break
    else:
        raise KeyError("No player name column in weekly data.")

    schedule = nfl.import_schedules(years)
    if 'gameday' in schedule.columns: schedule['game_date'] = schedule['gameday']
    elif 'game_date' not in schedule.columns: schedule['game_date'] = pd.NaT

    schedule['game_id'] = schedule.apply(lambda r: mk_game_id(r['season'], r['week'], r['home_team'], r['away_team']), axis=1)

    sched_home = schedule[['season','week','home_team','game_date','game_id']].rename(columns={'home_team':'team'})
    sched_away = schedule[['season','week','away_team','game_date','game_id']].rename(columns={'away_team':'team'})
    sched_long = pd.concat([sched_home, sched_away], ignore_index=True)

    weekly = weekly.merge(sched_long, on=['season','week','team'], how='left')

    weekly['game_datetime'] = pd.to_datetime(weekly['game_date'], errors='coerce')
    weekly['day_of_week'] = weekly['game_datetime'].dt.day_name()
    weekly['hour'] = weekly['game_datetime'].dt.hour
    weekly['time_slot'] = np.vectorize(time_slot)(weekly['day_of_week'], weekly['hour'])

    weekly = weekly[weekly['time_slot'] != "Unknown"].copy()
    weekly['total_touchdowns'] = weekly.get('receiving_tds', 0).fillna(0) + weekly.get('rushing_tds', 0).fillna(0)

    return downcast_floats(weekly)

def build_player_id_resolver(years: list[int]) -> dict[str, str]:
    rost = nfl.import_seasonal_rosters(years, columns=['player_id','player_name','team','position'])
    rost = rost.dropna(subset=['player_id','player_name','team'])
    rost['k'] = rost['player_name'].str.lower().str.strip() + '|' + rost['team'] + '|' + rost['position'].fillna('')
    return (rost.groupby('k')['player_id']
                .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0])
                .to_dict())

def filter_to_current_roster(weekly: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if CURRENT_ROSTER_ONLY:
        rost = nfl.import_seasonal_rosters([CURRENT_YEAR], columns=['player_id','player_name','team','position'])
        keep_ids = set(rost['player_id'].dropna().astype(str).unique())
        wk = weekly[weekly['player_id'].astype(str).isin(keep_ids)].copy()
        dim_player = (rost.rename(columns={'position':'primary_position','team':'last_team'})
                         .drop_duplicates('player_id', keep='last'))
        return wk, dim_player[['player_id','player_name','primary_position','last_team']]

    wk = weekly.copy()
    cols = ['player_id','player_name','position','team','season','week']
    have = [c for c in cols if c in wk.columns]
    snap = wk[have].dropna(subset=['player_id']).copy()
    snap = snap.sort_values(['player_id','season','week']).drop_duplicates('player_id', keep='last')
    snap = snap.rename(columns={'position':'primary_position','team':'last_team'})
    dim_player = snap[['player_id','player_name','primary_position','last_team']].fillna({"player_name":"Unknown"})
    return wk, dim_player
