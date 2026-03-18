import pytest
from pipeline.transform import flatten_movie


# ─────────────────────────────────────────────────────────────────
# flatten_movie
# ─────────────────────────────────────────────────────────────────

def make_raw_movie(**kwargs):
    """Return a raw TMDB API movie record."""
    base = {
        "id": 1,
        "imdb_id": "tt1234567",
        "title": "Test Movie",
        "original_title": "Test Movie",
        "original_language": "en",
        "release_date": "2024-01-15",
        "status": "Released",
        "budget": 1000000,
        "revenue": 5000000,
        "runtime": 120,
        "vote_average": 7.5,
        "vote_count": 1000,
        "popularity": 50.0,
        "genres": [{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}],
        "spoken_languages": [{"iso_639_1": "en", "name": "English"}],
        "production_companies": [{"id": 1, "name": "Warner Bros"}],
        "production_countries": [{"iso_3166_1": "US"}],
        "overview": "A test movie.",
        "tagline": "Just a test.",
        "homepage": "http://test.com",
    }
    base.update(kwargs)
    return base


def test_flatten_basic_fields():
    row = flatten_movie(make_raw_movie())
    assert row["movie_id"] == 1
    assert row["title"] == "Test Movie"
    assert row["original_language"] == "en"
    assert row["budget"] == 1000000
    assert row["revenue"] == 5000000


def test_flatten_multiple_genres():
    row = flatten_movie(make_raw_movie())
    assert row["genre_ids"] == "28|12"
    assert row["genre_names"] == "Action|Adventure"


def test_flatten_single_language():
    row = flatten_movie(make_raw_movie())
    assert row["language_codes"] == "en"
    assert row["language_names"] == "English"


def test_flatten_production_company():
    row = flatten_movie(make_raw_movie())
    assert row["company_ids"] == "1"
    assert row["company_names"] == "Warner Bros"


def test_flatten_country_codes():
    row = flatten_movie(make_raw_movie())
    assert row["country_codes"] == "US"


def test_flatten_empty_genres():
    row = flatten_movie(make_raw_movie(genres=[]))
    assert row["genre_ids"] == ""
    assert row["genre_names"] == ""


def test_flatten_null_genres():
    row = flatten_movie(make_raw_movie(genres=None))
    assert row["genre_ids"] == ""
    assert row["genre_names"] == ""


def test_flatten_missing_release_date():
    row = flatten_movie(make_raw_movie(release_date=None))
    assert row["release_date"] is None


def test_flatten_multiple_companies():
    movie = make_raw_movie(production_companies=[
        {"id": 1, "name": "Warner Bros"},
        {"id": 2, "name": "Universal"},
    ])
    row = flatten_movie(movie)
    assert row["company_ids"] == "1|2"
    assert row["company_names"] == "Warner Bros|Universal"


def test_flatten_returns_all_expected_keys():
    row = flatten_movie(make_raw_movie())
    expected_keys = [
        "movie_id", "imdb_id", "title", "original_title", "original_language",
        "release_date", "status", "budget", "revenue", "runtime", "vote_average",
        "vote_count", "popularity", "genre_ids", "genre_names", "language_codes",
        "language_names", "company_ids", "company_names", "country_codes",
        "overview", "tagline", "homepage",
    ]
    for key in expected_keys:
        assert key in row