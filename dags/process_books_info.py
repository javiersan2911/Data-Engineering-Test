import json
import os
import re
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from utils.file_utils import append_json

# Define the search terms
SEARCH_TERMS = "{{ var.value.search_terms }}"

# Define the file paths for the data lake and data warehouse
FILE_PATH_DATA_LAKE = (
    "/opt/airflow/data/data_lake/{{ var.value.search_terms }}/{{ data_interval_start | ds_nodash }}"
    "_{{ data_interval_end | ds_nodash}}.json"
)
FILE_PATH_DATA_WAREHOUSE = (
    "/opt/airflow/data/data_warehouse/{{ var.value.search_terms }}/{{ data_interval_start | ds_nodash }}"
    "_{{ data_interval_end | ds_nodash}}.csv"
)

# Define the start and end dates of the data interval
INTERVAL_START = "{{ data_interval_start | ds }}"
INTERVAL_END = "{{ data_interval_end | ds }}"


@dag(
    dag_id="process-docs-info",
    schedule_interval="@monthly",
    start_date=pendulum.datetime(2024, 2, 1, tz="UTC"),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
)
def process_docs_info():
    """
    This function defines the Airflow DAG for processing the document information.

    Args:
        dag_id (str): The ID of the DAG.
        schedule_interval (str): The schedule interval for running the DAG.
        start_date (pendulum.datetime): The start date of the DAG.
        catchup (bool): A flag indicating whether to catch up on missed runs.
        dagrun_timeout (timedelta): The timeout for a DAG run.

    Returns:
        None: The function does not return any values.
    """

    def join_dates(x: list):
        """
        This function joins the date parts of a date string.

        Args:
            x (str): The date string.

        Returns:
            str: The joined date string.
        """
        return "-".join(map(str, x[0]))

    def calculate_page_interval(page_range: str):
        """
        This function calculates the page interval from a page range string.

        Args:
            page_range (str): The page range string.

        Returns:
            int: The page interval.
        """
        # Extract numeric parts from the page range
        numeric_parts = re.findall(r"\d+", page_range)
        if len(numeric_parts) == 2:  # Check if there are two numeric parts
            start, end = map(int, numeric_parts)
            return end - start + 1
        else:
            return 0  # Return 0 if unable to calculate interval

    @task()
    def create_transform_file(path: str):
        """
        This function creates a transform file.

        Args:
            path (str): The path to the transform file.

        Returns:
            None: The function does not return any values.
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as file:
            file.write("[]")

    @task()
    def create_load_file(path: str):
        """
        This function creates a load file.

        Args:
            path (str): The path to the load file.

        Returns:
            None: The function does not return any values.
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as file:
            file.write("")

    @task()
    def extract_books_info(date_start: str, date_end: str, search_terms: str, file_path: str):
        """
        This function extracts the book information from CrossRef API.

        Args:
            date_start (str): The start date of the data interval.
            date_end (str): The end date of the data interval.
            search_terms (str): The search terms.
            file_path (str): The path to the file for saving the book information.
            settings (dict): The HTTP settings.

        Returns:
            int: The number of books extracted.
        """
        import requests

        # Define the HTTP settings
        settings = Variable.get("http_api_settings")

        base_url = settings["base_url"]
        params = settings["params"]
        params["filter"] = f"from-pub-date:{date_start},until-pub-date:{date_end},{settings['filter_fields']}"
        params["query"] = search_terms
        headers = settings["headers"]
        start_index = params["offset"]
        rows = params["rows"]

        response = requests.request("GET", base_url, headers=headers, params=params).json()["message"]

        if "items" in response and len(response["items"]) > 0:

            total_results = response["total-results"]
            with open(file_path, "w") as file:
                json.dump(response["items"], file, indent=4)

            while start_index + rows <= total_results:
                start_index += rows
                params["offset"] = start_index
                paginated_response = requests.request("GET", base_url, headers=headers, params=params).json()["message"]
                if "items" in paginated_response and len(paginated_response["items"]) > 0:
                    append_json(paginated_response["items"], file_path)
            return 1
        else:
            return 0

    @task()
    def transform_books_info(source_file: str, dest_file: str):
        """
        This function transforms the book information from a source file to a destination file.

        Args:
            source_file (str): The path to the source file.
            dest_file (str): The path to the destination file.

        Returns:
            None: The function does not return any values.
        """
        import pandas as pd

        with open(source_file, "r") as source_file:

            df = pd.json_normalize(json.load(source_file))
            fields_to_keep = [
                "title",
                "publisher",
                "type",
                "DOI",
                "ISBN",
                "author",
                "page",
                "page-count",
                "references-count",
                "is-referenced-by-count",
                "published-date",
                "journal-issue.issue",
                "indexed-date",
                "subject",
                "editor",
                "event.name",
                "event.location",
                "publisher-location",
            ]

            df["title"] = df["title"].fillna(" ").apply(
                lambda title: title[0])
            df["author"] = df["author"].fillna('[{"family":""}]').apply(
                lambda x: [(author['given'] + ' ' if 'given' in author else '') + author['family'] for author in
                           x if 'family' in author])
            df["page-count"] = df["page"].fillna("0").apply(
                calculate_page_interval)
            df["indexed-date"] = pd.to_datetime(
                df["indexed.date-parts"].apply(join_dates), format='mixed')
            df["published-date"] = pd.to_datetime(
                df["published.date-parts"].apply(join_dates), format='mixed')
            df["indexed-date"] = pd.to_datetime(
                df["indexed.date-parts"].apply(join_dates), format='mixed')

            df[list(set(fields_to_keep).difference(df.columns)) + list(
                set(fields_to_keep).intersection(df.columns))].fillna('').to_csv(dest_file)

    # Create the transform and load files
    [create_transform_file(FILE_PATH_DATA_LAKE), create_load_file(FILE_PATH_DATA_WAREHOUSE)] >> extract_books_info(
        INTERVAL_START, INTERVAL_END, SEARCH_TERMS, FILE_PATH_DATA_LAKE) >> transform_books_info(
        FILE_PATH_DATA_LAKE, FILE_PATH_DATA_WAREHOUSE)


# Run the DAG
process_docs_info()
