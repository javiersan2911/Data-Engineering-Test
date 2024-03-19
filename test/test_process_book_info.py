import os
import json
from datetime import datetime, timedelta
from unittest.mock import patch

import requests
import pandas as pd
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.models import DagBag

from dags.process_docs_info import extract_books_info

# Define the test search terms
TEST_SEARCH_TERMS = "test search terms"

# Define the test file path
TEST_FILE_PATH = "/tmp/test_file.json"

# Define the test HTTP settings
TEST_HTTP_SETTINGS = {
    "base_url": "https://example.com/api/v1",
    "params": {
        "query.author": "test author",
        "filter": "test filter",
        "rows": 10,
        "offset": 0
    },
    "headers": {
        "Authorization": "Bearer abcdefg"
    },
    "filter_fields": "test filter fields"
}


@patch("requests.request")
def test_extract_books_info_pytest(mock_request):
    # Define the test date range
    test_date_start = days_ago(2).date()
    test_date_end = days_ago(1).date()

    # Define the expected response from the API
    expected_response = {
        "status": "ok",
        "message-type": "response",
        "message-version": "1.0.0",
        "total-results": 10,
        "items": [
            {
                "DOI": "10.1234/abcde.12345",
                "ISBN": ["978-1-2345678-9"],
                "URL": ["https://example.com/book/12345"],
                "author": [
                    {
                        "family": "Test Author"
                    }
                ],
                "container-title": [
                    {
                        "title": "Test Journal"
                    }
                ],
                "issn": ["1234-5678"],
                "issue": "12",
                "page": "123-133",
                "page-count": 10,
                "published-online": {
                    "date-parts": [[2023, 2, 1]]
                },
                "publisher": "Test Publisher",
                "title": "Test Title",
                "type": "journal-article"
            }
        ]
    }

    # Mock the API request
    mock_request.return_value = expected_response

    # Load the DAGs
    dagbag = DagBag(os.environ["AIRFLOW_HOME"])

    # Create the test file
    with open(TEST_FILE_PATH, "w") as file:
        json.dump([], file)

    # Define the arguments for the function to be tested
    args = (test_date_start, test_date_end, TEST_SEARCH_TERMS, TEST_FILE_PATH, TEST_HTTP_SETTINGS)

    # Call the function to be tested
    num_books_extracted = extract_books_info(*args)

    # Verify the function was called with the expected arguments
    mock_request.assert_called_once_with(
        "GET",
        "https://example.com/api/v1",
        headers={"Authorization": "Bearer abcdefg"},
        params={
            "query.author": "test author",
            "filter": f"from-pub-date:{test_date_start},until-pub-date:{test_date_end},test filter fields",
            "rows": 10,
            "offset": 0
        }
    )

    # Verify the expected response was saved to the file
    with open(TEST_FILE_PATH, "r") as file:
        saved_response = json.load(file)
        assert expected_response == saved_response

    # Verify the number of books was returned
    assert 1 == num_books_extracted


if __name__ == "__main__":
    test_extract_books_info_pytest()