import pytest
from pipeline.validate import validate_record

# ─────────────────────────────────────────────────────────────────
# validate_record
# ─────────────────────────────────────────────────────────────────

def make_movie(**kwargs):
    """Return a valid base movie record, optionally overriding fields."""
    base = {
        "id": 1,
        "title": "Test Movie",
        "release_date": "2024-01-15",
        "budget": 1000000,
        "revenue": 5000000,
        "runtime": 120,
        "vote_average": 7.5,
        "vote_count": 1000,
        "popularity": 50.0,
    }
    base.update(kwargs)
    return base


def test_valid_record():
    seen_ids = set()
    valid, reason = validate_record(make_movie(), seen_ids)
    assert valid is True
    assert reason == ""


def test_missing_id():
    seen_ids = set()
    valid, reason = validate_record(make_movie(id=None), seen_ids)
    assert valid is False
    assert "id" in reason


def test_missing_title():
    seen_ids = set()
    valid, reason = validate_record(make_movie(title=None), seen_ids)
    assert valid is False
    assert "title" in reason


def test_missing_release_date():
    seen_ids = set()
    valid, reason = validate_record(make_movie(release_date=None), seen_ids)
    assert valid is False
    assert "release_date" in reason


def test_duplicate_id():
    seen_ids = {1}
    valid, reason = validate_record(make_movie(id=1), seen_ids)
    assert valid is False
    assert "Duplicate" in reason


def test_non_numeric_budget():
    seen_ids = set()
    valid, reason = validate_record(make_movie(budget="not_a_number"), seen_ids)
    assert valid is False
    assert "budget" in reason


def test_vote_average_out_of_range():
    seen_ids = set()
    valid, reason = validate_record(make_movie(vote_average=11.0), seen_ids)
    assert valid is False
    assert "vote_average" in reason


def test_negative_runtime():
    seen_ids = set()
    valid, reason = validate_record(make_movie(runtime=-10), seen_ids)
    assert valid is False
    assert "runtime" in reason


def test_invalid_release_date_format():
    seen_ids = set()
    valid, reason = validate_record(make_movie(release_date="15-01-2024"), seen_ids)
    assert valid is False
    assert "release_date" in reason


def test_seen_ids_updated_on_valid_record():
    seen_ids = set()
    validate_record(make_movie(id=42), seen_ids)
    assert 42 in seen_ids


def test_zero_vote_average_is_valid():
    seen_ids = set()
    valid, reason = validate_record(make_movie(vote_average=0), seen_ids)
    assert valid is True


def test_zero_runtime_is_valid():
    seen_ids = set()
    valid, reason = validate_record(make_movie(runtime=0), seen_ids)
    assert valid is True


def test_none_numeric_fields_are_valid():
    seen_ids = set()
    valid, reason = validate_record(make_movie(budget=None, revenue=None, runtime=None), seen_ids)
    assert valid is True