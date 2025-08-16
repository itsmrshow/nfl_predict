from lines import _normalize_book_name

def test_normalize_book_name():
    assert _normalize_book_name("draft kings") == "DraftKings"
    assert _normalize_book_name("Fan duel") == "FanDuel"
    assert _normalize_book_name("Fanatics Sportsbook") == "Fanatics"
    assert _normalize_book_name("Other") == "Other"
