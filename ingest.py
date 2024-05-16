import os
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import dlt
from dlt.sources.helpers import requests

DATASET_NAME = "fitbit_ingest"
TABLE_NAME = "steps"
START_DATE = "2024-01-02"
FITBIT_API_URL = "https://api.fitbit.com/1/user/-/activities/steps/date"

def get_last_loaded_date(client):
    """Retrieve the last date for which data was loaded."""
    query = f"""
        SELECT MAX(date_time) AS max_loaded_date FROM `{client.project}.{DATASET_NAME}.{TABLE_NAME}`
    """
    try:
        result = client.query(query).result()
        return datetime.strptime(list(result)[0].max_loaded_date, '%Y-%m-%d').date()
    except NotFound:
        return datetime.strptime(START_DATE, '%Y-%m-%d').date() + timedelta(days=1)

def fetch_fitbit_data(start_date, end_date):
    """Fetch data from Fitbit API."""
    url = f"{FITBIT_API_URL}/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}.json"
    headers = {
        'accept': 'application/json',
        'authorization': f'Bearer {os.getenv("FITBIT_ACCESS_TOKEN")}'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()['activities-steps']

def main():
    """Main function to orchestrate data loading from Fitbit API to BigQuery."""
    # Setup local environment variables if not running in GitHub Actions
    if not os.getenv('GITHUB_ACTIONS') == 'true':
        load_dotenv()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '../GCLOUD_SERVICE_ACCOUNT_KEY_FILE.json'

    client = bigquery.Client()

    last_loaded_date = get_last_loaded_date(client)
    today = date.today()
    load_period = today - last_loaded_date

    if load_period.days > 1095:
        # Maximum date range for `steps` endpoint is 1095 days
        # https://dev.fitbit.com/build/reference/web-api/activity-timeseries/get-activity-timeseries-by-date-range/#Resource-Options
        raise ValueError(f"The difference in days ({load_period.days}) is greater than 1095 days.")

    data = fetch_fitbit_data(last_loaded_date - timedelta(days=1), today)
    pipeline = dlt.pipeline(
        pipeline_name="steps_pipeline",
        destination="bigquery",
        dataset_name=DATASET_NAME,
    )
    load_info = pipeline.run(
        data,
        table_name=TABLE_NAME,
        write_disposition="merge",
        primary_key="date_time"
    )
    print(load_info)

if __name__ == "__main__":
    main()
