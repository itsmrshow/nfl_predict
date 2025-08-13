import pandas as pd
import nfl_data_py as nfl
from sqlalchemy import text
from config import DB_SCHEMA

def backfill_legacy_ids(engine, years: list[int]):
    rost = nfl.import_seasonal_rosters(years, columns=['player_id','player_name','team','position','season']).dropna(subset=['player_id','player_name','team'])
    rost['player_name_norm'] = rost['player_name'].str.lower().str.strip()
    rost['k'] = rost['player_name_norm'] + '|' + rost['team'] + '|' + rost['position'].fillna('')
    rmap = (rost.groupby('k')['player_id']
                .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0])
                .reset_index())

    with engine.begin() as con:
        con.execute(text("CREATE TEMP TABLE temp_player_id_map (k text PRIMARY KEY, real_player_id text) ON COMMIT DROP;"))
        rows = rmap.rename(columns={'player_id':'real_player_id'}).to_dict(orient='records')
        for i in range(0, len(rows), 1000):
            con.execute(text("INSERT INTO temp_player_id_map (k, real_player_id) VALUES (:k, :real_player_id) ON CONFLICT DO NOTHING;"), rows[i:i+1000])

        con.execute(text(f"""
            WITH fact_keys AS (
                SELECT
                    game_id, season, week, team_abbr, opponent_abbr, time_slot, player_id, position, player_name,
                    LOWER(TRIM(player_name)) || '|' || team_abbr || '|' || COALESCE(position,'') AS k
                FROM {DB_SCHEMA}.fact_player_timeslot
                WHERE player_id LIKE 'legacy_%'
            )
            UPDATE {DB_SCHEMA}.fact_player_timeslot f
            SET player_id = m.real_player_id
            FROM fact_keys fk
            JOIN temp_player_id_map m ON m.k = fk.k
            WHERE f.game_id=fk.game_id AND f.season=fk.season AND f.week=fk.week
              AND f.team_abbr=fk.team_abbr AND f.opponent_abbr=fk.opponent_abbr
              AND f.time_slot=fk.time_slot AND f.player_id=fk.player_id
              AND f.position=fk.position;
        """))

        dim_player = (rost.rename(columns={'team':'last_team','position':'primary_position'})
                         [['player_id','player_name','primary_position','last_team']]
                         .drop_duplicates('player_id'))
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
