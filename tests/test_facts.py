import pandas as pd
import facts

def test_build_fact_all(monkeypatch):
    monkeypatch.setattr(facts, "YEARS", [2024])
    data = [
        {
            "game_id": "gid1",
            "season": 2024,
            "week": 1,
            "team": "KC",
            "opponent": "LV",
            "time_slot": "Sunday Night",
            "player_id": "1",
            "player_name": "A",
            "position": "QB",
            "passing_yards": 200,
            "passing_tds": 2,
            "interceptions": 1,
        },
        {
            "game_id": "gid1",
            "season": 2024,
            "week": 1,
            "team": "KC",
            "opponent": "LV",
            "time_slot": "Sunday Night",
            "player_id": "1",
            "player_name": "A",
            "position": "QB",
            "passing_yards": 300,
            "passing_tds": 4,
            "interceptions": 0,
        },
    ]
    wk = pd.DataFrame(data)
    fact = facts.build_fact_all(wk)
    assert len(fact) == 1
    row = fact.iloc[0]
    assert row["passing_yards_avg"] == 250
    assert row["passing_tds_avg"] == 3
    assert row["interceptions_avg"] == 0.5
    assert row["season_range"] == "2024–2024"
