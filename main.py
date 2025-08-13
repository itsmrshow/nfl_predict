import hashlib
import pandas as pd
import numpy as np
import nfl_data_py as nfl
from logutil import get_logger
from config import YEARS, CURRENT_ROSTER_ONLY, REPLACE_MODE, DAILY_MODE, RECENT_WEEKS
from config import DB_SCHEMA
from db import get_engine, ensure_schema, create_tables, ensure_fact_schema_up_to_date, add_indexes, delete_fact_and_lines_for_seasons, ensure_props_schema_up_to_date
from teams import load_reference, upsert_dim_team, upsert_dim_timeslot
from weekly import load_weekly_with_timeslot, build_player_id_resolver, filter_to_current_roster
from facts import build_fact_all, upsert_fact
from lines import load_vegas_lines, upsert_lines
from props import fetch_player_props_from_theodds, upsert_player_props
from backfill import backfill_legacy_ids
from utils import mk_game_id
from sqlalchemy import text

logger = get_logger()

def _fill_player_id_with_resolver(row, resolver: dict[str,str]):
    pid = row.get('player_id')
    if pd.notna(pid) and str(pid).strip() not in ("", "None", "nan"):
        return str(pid)
    name = str(row.get('player_name','')).lower().strip()
    team = str(row.get('team','') or row.get('team_abbr',''))
    pos  = str(row.get('position','') or '')
    k = f"{name}|{team}|{pos}"
    if resolver and k in resolver and pd.notna(resolver[k]):
        return str(resolver[k])
    basis = f"{row.get('player_name','unknown')}|{row.get('season')}|{row.get('week')}|{team}|{row.get('opponent')}"
    return "legacy_" + hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]

def main():
    logger.info(f"Loading seasons {min(YEARS)}-{max(YEARS)} | roster filter={CURRENT_ROSTER_ONLY} | replace={REPLACE_MODE} | daily={DAILY_MODE} (last {RECENT_WEEKS} weeks)")

    engine = get_engine()
    ensure_schema(engine)
    create_tables(engine)
    ensure_fact_schema_up_to_date(engine)
    ensure_props_schema_up_to_date(engine)
    upsert_dim_timeslot(engine)

    teams_all, dim_team = load_reference()
    upsert_dim_team(engine, dim_team)

    weekly = load_weekly_with_timeslot(YEARS)

    if DAILY_MODE and not weekly.empty:
        max_season = weekly['season'].max()
        wks = weekly.loc[weekly['season'].eq(max_season), 'week']
        if not wks.empty:
            cutoff = max(int(wks.max()) - RECENT_WEEKS + 1, int(wks.min()))
            weekly = weekly.query("season == @max_season and week >= @cutoff").copy()

    logger.info(f"Weekly data after time slot join: {weekly.shape}")

    resolver = build_player_id_resolver(YEARS)
    weekly['player_id'] = weekly.apply(lambda r: _fill_player_id_with_resolver(r, resolver), axis=1).astype(str)

    weekly, dim_player = filter_to_current_roster(weekly)
    logger.info(f"Weekly data after roster filter: {weekly.shape} (CURRENT_ROSTER_ONLY={CURRENT_ROSTER_ONLY})")

    # upsert dim_player
    if not dim_player.empty:
        with engine.begin() as con:
            rows = dim_player.to_dict(orient='records')
            sql = f"""
                INSERT INTO {DB_SCHEMA}.dim_player (player_id, player_name, primary_position, last_team)
                VALUES (:player_id, :player_name, :primary_position, :last_team)
                ON CONFLICT (player_id) DO UPDATE SET
                    player_name = EXCLUDED.player_name,
                    primary_position = EXCLUDED.primary_position,
                    last_team = EXCLUDED.last_team;
            """
            for i in range(0, len(rows), 1000):
                con.execute(text(sql), rows[i:i+1000])

    fact = build_fact_all(weekly)
    fact['current_roster_only'] = CURRENT_ROSTER_ONLY
    logger.info(f"Fact (pre-clean) shape: {fact.shape}")

    pk_cols = ['game_id','season','week','team_abbr','opponent_abbr','time_slot','player_id','position']
    before = len(fact)
    fact = fact.drop_duplicates(subset=pk_cols, keep='last')
    after = len(fact)
    logger.info(f"Deduped fact rows on PK: {before:,} -> {after:,}")
    logger.info(f"Rows with NULL player_id (should be 0): {fact['player_id'].isna().sum()}")

    schedule = nfl.import_schedules(YEARS)
    schedule['game_date'] = schedule['gameday'] if 'gameday' in schedule.columns else schedule.get('game_date')
    schedule['game_date'] = pd.to_datetime(schedule['game_date'], errors='coerce', utc=True)
    schedule['game_id'] = schedule.apply(lambda r: mk_game_id(r['season'], r['week'], r['home_team'], r['away_team']), axis=1)

    lines = load_vegas_lines(YEARS, schedule)
    logger.info(f"Lines shape: {lines.shape if isinstance(lines, pd.DataFrame) else (0,0)}")

    if REPLACE_MODE:
        delete_fact_and_lines_for_seasons(engine, YEARS)
        logger.info(f"Cleared facts & lines for seasons {min(YEARS)}-{max(YEARS)}")

    upsert_fact(engine, fact)
    upsert_lines(engine, lines)
    add_indexes(engine)

    backfill_legacy_ids(engine, YEARS)

    props_df = fetch_player_props_from_theodds(YEARS, schedule)
    logger.info(f"Props shape: {props_df.shape}")
    upsert_player_props(engine, props_df)

    logger.info(f"Inserted {len(fact):,} fact rows across {fact['season'].nunique()} seasons, {fact['team_abbr'].nunique()} teams.")
    logger.info(f"Inserted/updated {0 if lines is None or lines.empty else len(lines)} vegas line rows.")
    logger.info("Load complete.")

if __name__ == "__main__":
    main()
