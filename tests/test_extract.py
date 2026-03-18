import pytest
from unittest.mock import patch, MagicMock
from pipeline.extract import get_changed_movie_ids


# ─────────────────────────────────────────────────────────────────
# get_changed_movie_ids
# ─────────────────────────────────────────────────────────────────

def make_changes_response(movie_ids, page=1, total_pages=1):
    """Build a fake TMDB changes API response."""
    return {
        "results": [{"id": mid, "adult": False} for mid in movie_ids],
        "page": page,
        "total_pages": total_pages,
    }


@patch("pipeline.extract.requests.get")
def test_single_page(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = make_changes_response([1, 2, 3])
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    ids = get_changed_movie_ids("2026-03-16")
    assert ids == [1, 2, 3]


@patch("pipeline.extract.requests.get")
def test_multiple_pages(mock_get):
    page1 = MagicMock()
    page1.json.return_value = make_changes_response([1, 2], page=1, total_pages=2)
    page1.raise_for_status = MagicMock()

    page2 = MagicMock()
    page2.json.return_value = make_changes_response([3, 4], page=2, total_pages=2)
    page2.raise_for_status = MagicMock()

    mock_get.side_effect = [page1, page2]

    ids = get_changed_movie_ids("2026-03-16")
    assert ids == [1, 2, 3, 4]
    assert mock_get.call_count == 2


@patch("pipeline.extract.requests.get")
def test_adult_content_filtered(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "results": [
            {"id": 1, "adult": False},
            {"id": 2, "adult": True},
            {"id": 3, "adult": False},
        ],
        "total_pages": 1,
    }
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    ids = get_changed_movie_ids("2026-03-16")
    assert ids == [1, 3]
    assert 2 not in ids


@patch("pipeline.extract.requests.get")
def test_empty_results(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = make_changes_response([])
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    ids = get_changed_movie_ids("2026-03-16")
    assert ids == []


@patch("pipeline.extract.requests.get")
def test_correct_date_range(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = make_changes_response([1])
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    get_changed_movie_ids("2026-03-16")

    call_params = mock_get.call_args[1]["params"] if mock_get.call_args[1] else mock_get.call_args[0][1]
    assert call_params["start_date"] == "2026-03-15"
    assert call_params["end_date"] == "2026-03-16"