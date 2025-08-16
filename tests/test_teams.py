from teams import team_alias_map

def test_team_alias_map_basic():
    m = team_alias_map()
    assert m["los angeles rams"] == "LAR"
    assert m["oakland raiders"] == "LV"
    assert m["seattle"] == "SEA"
