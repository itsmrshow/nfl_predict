import math
import pandas as pd
from utils import mk_game_id, time_slot, coerce_numeric

def test_mk_game_id():
    assert mk_game_id(2024, 1, "KC", "LV") == "2024_01_KC_LV"

def test_time_slot():
    assert time_slot("Sunday", 13) == "Sunday Early Window"
    assert time_slot("Monday", 20) == "Monday"
    assert time_slot("Thursday", 20) == "Thursday"
    assert time_slot("Sunday", 10) == "Sunday Morning"
    assert time_slot("Sunday", 16) == "Sunday Late Window"
    assert time_slot("Sunday", 21) == "Sunday Night"
    assert time_slot("Wednesday", 12) == "Unknown"
    assert time_slot(None, None) == "Unknown"

def test_coerce_numeric():
    df = pd.DataFrame({"a": ["1", "x"]})
    coerce_numeric(df, ["a"])
    assert df["a"].iloc[0] == 1.0
    assert math.isnan(df["a"].iloc[1])
