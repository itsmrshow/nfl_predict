import os
import re
import datetime as dt
from dotenv import load_dotenv

load_dotenv()

CURRENT_YEAR = dt.datetime.now().year

def _parse_years(s: str) -> list[int]:
    s = (s or "").strip()
    if not s:
        return list(range(2015, CURRENT_YEAR + 1))
    m = re.fullmatch(r"\s*(\d{4})\s*-\s*(\d{4})\s*", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if a > b: a, b = b, a
        return list(range(a, b + 1))
    return [int(x.strip()) for x in s.split(",") if x.strip()]

PGHOST      = os.getenv("PGHOST")
PGPORT      = int(os.getenv("PGPORT", "5432"))
PGDATABASE  = os.getenv("PGDATABASE")
PGUSER      = os.getenv("PGUSER")
PGPASSWORD  = os.getenv("PGPASSWORD")
DB_SCHEMA   = os.getenv("DB_SCHEMA", "nfl")

YEARS               = _parse_years(os.getenv("YEARS", f"2015-{CURRENT_YEAR}"))
CURRENT_ROSTER_ONLY = os.getenv("CURRENT_ROSTER_ONLY", "false").lower() in ("1","true","yes")
REPLACE_MODE        = os.getenv("REPLACE_MODE", "true").lower() in ("1","true","yes")
DAILY_MODE          = os.getenv("DAILY_MODE", "false").lower() in ("1","true","yes")
RECENT_WEEKS        = int(os.getenv("RECENT_WEEKS", "4"))

LINES_BOOK_FILTER = [b.strip() for b in os.getenv("LINES_BOOK_FILTER","DraftKings,FanDuel,Fanatics").split(",") if b.strip()]

THEODDS_API_KEY = os.getenv("THEODDS_API_KEY")
PROPS_BOOKS  = [b.strip().lower() for b in os.getenv("PROPS_BOOKS","DraftKings,FanDuel,Fanatics").split(",") if b.strip()]
PROPS_MARKETS = [m.strip() for m in os.getenv("PROPS_MARKETS","player_pass_yds,player_rush_yds,player_rec_yds,player_receptions").split(",") if m.strip()]

SPORT_KEY = "americanfootball_nfl"
