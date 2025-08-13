from sqlalchemy import create_engine, text
from config import PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, DB_SCHEMA
from io import StringIO

def get_engine():
    url = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
    return create_engine(url, pool_pre_ping=True, hide_parameters=True)

def ensure_schema(engine):
    with engine.begin() as con:
        con.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA};"))

def create_tables(engine):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.dim_team (
        team_abbr text PRIMARY KEY,
        team_name text
    );
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.dim_player (
        player_id text PRIMARY KEY,
        player_name text,
        primary_position text,
        last_team text
    );
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.dim_timeslot (
        timeslot_key smallint PRIMARY KEY,
        time_slot text UNIQUE
    );
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.dim_vegas_lines (
        game_id text,
        season int,
        week int,
        book text,
        home_team text,
        away_team text,
        favorite_team text,
        spread_open numeric,
        spread_close numeric,
        total_open numeric,
        total_close numeric,
        home_moneyline int,
        away_moneyline int,
        line_source text,
        line_timestamp timestamptz,
        load_ts timestamptz DEFAULT now(),
        PRIMARY KEY (game_id, book, line_timestamp)
    );
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.fact_player_timeslot (
        game_id text,
        season int,
        week int,
        team_abbr text,
        opponent_abbr text,
        time_slot text,
        player_id text,
        player_name text,
        position text,
        passing_yards_avg numeric,
        passing_tds_avg numeric,
        interceptions_avg numeric,
        attempts_avg numeric,
        completions_avg numeric,
        rushing_yards_avg numeric,
        rushing_tds_avg numeric,
        carries_avg numeric,
        receptions_avg numeric,
        receiving_yards_avg numeric,
        receiving_tds_avg numeric,
        sacks_avg numeric,
        def_interceptions_avg numeric,
        fumbles_recovered_avg numeric,
        total_touchdowns_avg numeric,
        games_played int,
        season_range text,
        current_roster_only boolean,
        load_ts timestamp default now(),
        PRIMARY KEY (game_id, season, week, team_abbr, opponent_abbr, time_slot, player_id, position)
    );
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.fact_player_prop_lines (
        game_id   text,
        season    int,
        week      int,
        seasonweek int,
        book      text,
        player_id text,
        player_name text,
        market    text,
        line_value numeric,
        over_odds  int,
        under_odds int,
        ts         timestamptz,
        load_ts    timestamptz default now(),
        PRIMARY KEY (game_id, book, player_name, market, ts)
    );
    """
    with engine.begin() as con:
        con.execute(text(ddl))

def ensure_fact_schema_up_to_date(engine):
    needed = {
        "game_id": "text",
        "interceptions_avg": "numeric",
        "def_interceptions_avg": "numeric",
        "fumbles_recovered_avg": "numeric",
        "season_range": "text",
        "current_roster_only": "boolean",
    }
    with engine.begin() as con:
        cols = con.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema=:s AND table_name='fact_player_timeslot'
        """), {"s": DB_SCHEMA}).fetchall()
        existing = {r[0] for r in cols}
        for col, typ in needed.items():
            if col not in existing:
                con.execute(text(f"ALTER TABLE {DB_SCHEMA}.fact_player_timeslot ADD COLUMN {col} {typ};"))

def ensure_props_schema_up_to_date(engine):
    """Add missing columns for fact_player_prop_lines created by older versions."""
    with engine.begin() as con:
        cols = con.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :s AND table_name = 'fact_player_prop_lines'
        """), {"s": DB_SCHEMA}).fetchall()
        existing = {r[0] for r in cols}
        if "seasonweek" not in existing:
            con.execute(text(f"ALTER TABLE {DB_SCHEMA}.fact_player_prop_lines ADD COLUMN seasonweek int;"))

def add_indexes(engine):
    with engine.begin() as con:
        con.execute(text(f"CREATE INDEX IF NOT EXISTS ix_fact_season_week ON {DB_SCHEMA}.fact_player_timeslot(season, week);"))
        con.execute(text(f"CREATE INDEX IF NOT EXISTS ix_fact_team_opp_slot ON {DB_SCHEMA}.fact_player_timeslot(team_abbr, opponent_abbr, time_slot);"))
        con.execute(text(f"CREATE INDEX IF NOT EXISTS ix_fact_player ON {DB_SCHEMA}.fact_player_timeslot(player_id);"))
        con.execute(text(f"CREATE INDEX IF NOT EXISTS ix_fact_game ON {DB_SCHEMA}.fact_player_timeslot(game_id);"))
        con.execute(text(f"CREATE INDEX IF NOT EXISTS ix_lines_game_book ON {DB_SCHEMA}.dim_vegas_lines(game_id, book);"))

        # only if seasonweek exists
        cols = con.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :s AND table_name = 'fact_player_prop_lines'
        """), {"s": DB_SCHEMA}).fetchall()
        existing = {r[0] for r in cols}
        if "seasonweek" in existing:
            con.execute(text(f"CREATE INDEX IF NOT EXISTS ix_props_seasonweek ON {DB_SCHEMA}.fact_player_prop_lines(seasonweek);"))

def delete_fact_and_lines_for_seasons(engine, years: list[int]):
    with engine.begin() as con:
        con.execute(text(f"DELETE FROM {DB_SCHEMA}.fact_player_timeslot WHERE season = ANY(:y);"), {"y": years})
        con.execute(text(f"DELETE FROM {DB_SCHEMA}.dim_vegas_lines WHERE season = ANY(:y);"), {"y": years})
        con.execute(text(f"DELETE FROM {DB_SCHEMA}.fact_player_prop_lines WHERE season = ANY(:y);"), {"y": years})

def copy_from_dataframe(conn, df, table_name: str):
    buf = StringIO()
    df_to_copy = df.copy()
    if "current_roster_only" in df_to_copy.columns:
        df_to_copy["current_roster_only"] = df_to_copy["current_roster_only"].map({True: 't', False: 'f'})
    df_to_copy.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    raw = conn.connection  # psycopg2 connection
    cols_csv = ",".join(df.columns)
    with raw.cursor() as cur:
        cur.copy_expert(f"COPY {table_name} ({cols_csv}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')", buf)
