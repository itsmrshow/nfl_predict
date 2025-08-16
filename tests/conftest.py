import sys
import types
import pandas as pd
from pathlib import Path

# ensure project root on path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# provide a lightweight stub for nfl_data_py to satisfy imports during tests
fake_nfl = types.SimpleNamespace(
    import_team_desc=lambda: pd.DataFrame(columns=["team_abbr","team_name"]),
    import_weekly_data=lambda years: pd.DataFrame(),
    import_schedules=lambda years: pd.DataFrame(),
    import_seasonal_rosters=lambda years, columns=None: pd.DataFrame(columns=columns or []),
    import_betting_lines=lambda years: pd.DataFrame(),
)
sys.modules.setdefault("nfl_data_py", fake_nfl)

# minimal stub for sqlalchemy used in modules
def _sqlalchemy_text(s):
    return s
def _create_engine(*args, **kwargs):
    class Dummy:
        def connect(self):
            raise RuntimeError("stub")
    return Dummy()
fake_sqlalchemy = types.SimpleNamespace(text=_sqlalchemy_text, create_engine=_create_engine)
sys.modules.setdefault("sqlalchemy", fake_sqlalchemy)
