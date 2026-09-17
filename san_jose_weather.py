import logging
import math
from contextlib import closing
from datetime import date, timedelta

import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


LOGGER = logging.getLogger(__name__)
TIMEZONE = "America/Los_Angeles"
API_URL = "https://archive-api.open-meteo.com/v1/archive"
TARGET_TABLE = "DEV.RAW.SAN_JOSE_WEATHER"
SNOWFLAKE_CONN_ID = "snowflake_conn"
WINDOW_DAYS = 60
DAILY_FIELDS = (
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "weather_code",
)


def check_table_stats(cursor, expected):
    cursor.execute(
        f"""SELECT COUNT(*), COUNT(DISTINCT date), MIN(date), MAX(date),
                   COUNT_IF(latitude = %s AND longitude = %s)
            FROM {TARGET_TABLE}""",
        (expected["latitude"], expected["longitude"]),
    )
    count, unique_dates, first_date, last_date, matching_location = cursor.fetchone()
    actual = (count, unique_dates, str(first_date), str(last_date), matching_location)
    wanted = (
        WINDOW_DAYS,
        WINDOW_DAYS,
        expected["start_date"],
        expected["end_date"],
        WINDOW_DAYS,
    )
    if actual != wanted:
        raise ValueError(f"Refresh validation failed: expected {wanted}, got {actual}")
    return {
        "table": TARGET_TABLE,
        "row_count": count,
        "distinct_dates": unique_dates,
        "start_date": str(first_date),
        "end_date": str(last_date),
        "latitude": expected["latitude"],
        "longitude": expected["longitude"],
    }


@task(multiple_outputs=False, show_return_value_in_logs=False)
def extract():
    latitude = float(Variable.get("latitude"))
    longitude = float(Variable.get("longitude"))
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        raise ValueError("Latitude or longitude is outside its valid range")

    interval_end = get_current_context()["data_interval_end"].in_timezone(TIMEZONE)
    end_date = interval_end.date() - timedelta(days=1)
    start_date = end_date - timedelta(days=WINDOW_DAYS - 1)
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": ",".join(DAILY_FIELDS),
        "timezone": TIMEZONE,
        "temperature_unit": "celsius",
        "precipitation_unit": "mm",
    }
    response = requests.get(API_URL, params=params, timeout=(10, 120))
    response.raise_for_status()
    data = response.json()
    if data.get("error"):
        raise ValueError(f"Open-Meteo error: {data.get('reason')}")
    LOGGER.info("San Jose weather: (%s, %s), %s through %s", latitude, longitude,
                start_date, end_date)
    return {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": data["daily"],
    }


@task(multiple_outputs=False, show_return_value_in_logs=False)
def transform(payload):
    daily = payload["daily"]
    start_date = date.fromisoformat(payload["start_date"])
    expected_dates = [
        (start_date + timedelta(days=i)).isoformat() for i in range(WINDOW_DAYS)
    ]
    if daily.get("time") != expected_dates or expected_dates[-1] != payload["end_date"]:
        raise ValueError("Expected exactly 60 consecutive, unique dates in the requested window")
    for field in DAILY_FIELDS:
        if len(daily.get(field, [])) != WINDOW_DAYS:
            raise ValueError(f"Missing or incomplete daily field: {field}")

    records = []
    for i, day in enumerate(expected_dates):
        values = [daily[field][i] for field in DAILY_FIELDS]
        if any(isinstance(value, bool) or not isinstance(value, (int, float))
               or not math.isfinite(value) for value in values):
            raise ValueError(f"Missing or invalid weather measurement for {day}")
        temp_max, temp_min, precipitation, weather_code = values
        if temp_min > temp_max or precipitation < 0 or int(weather_code) != weather_code:
            raise ValueError(f"Invalid weather measurement for {day}: {values}")
        records.append([
            payload["latitude"], payload["longitude"], day,
            temp_max, temp_min, precipitation, int(weather_code),
        ])
    LOGGER.info("Validated %s daily weather records; no missing or duplicate dates", len(records))
    return {
        "latitude": payload["latitude"],
        "longitude": payload["longitude"],
        "start_date": payload["start_date"],
        "end_date": payload["end_date"],
        "records": records,
    }


@task(multiple_outputs=False)
def load(payload):
    records = payload["records"]
    if len(records) != WINDOW_DAYS:
        raise ValueError("Refusing to replace the table with fewer or more than 60 rows")
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    with closing(hook.get_conn()) as connection:
        with closing(connection.cursor()) as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                    latitude FLOAT,
                    longitude FLOAT,
                    date DATE,
                    temp_max FLOAT,
                    temp_min FLOAT,
                    precipitation FLOAT,
                    weather_code INTEGER,
                    PRIMARY KEY (latitude, longitude, date)
                )
            """)
            try:
                cursor.execute("BEGIN TRANSACTION")
                LOGGER.info("BEGIN TRANSACTION: full refresh of %s", TARGET_TABLE)
                cursor.execute(f"DELETE FROM {TARGET_TABLE}")
                LOGGER.info("Deleted %s previous rows", cursor.rowcount)
                cursor.executemany(
                    f"""INSERT INTO {TARGET_TABLE}
                        (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    records,
                )
                LOGGER.info("Inserted %s weather records using bound parameters", len(records))
                summary = check_table_stats(cursor, payload)
                cursor.execute("COMMIT")
                LOGGER.info("COMMIT: full refresh succeeded; %s rows, %s through %s",
                            summary["row_count"], summary["start_date"], summary["end_date"])
            except Exception:
                cursor.execute("ROLLBACK")
                LOGGER.exception("ROLLBACK: full refresh failed; previous rows preserved")
                raise
    return summary


@task(multiple_outputs=False)
def verify(load_summary):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    with closing(hook.get_conn()) as connection:
        with closing(connection.cursor()) as cursor:
            summary = check_table_stats(cursor, load_summary)
            cursor.execute(f"SELECT * FROM {TARGET_TABLE} ORDER BY date DESC LIMIT 5")
            for record in cursor.fetchall():
                LOGGER.info("Recent weather row: %s", record)
    LOGGER.info("VERIFIED: %s contains %s rows / %s unique dates, %s through %s",
                TARGET_TABLE, summary["row_count"], summary["distinct_dates"],
                summary["start_date"], summary["end_date"])
    LOGGER.info("Full refresh complete. Re-running this window must still leave exactly 60 rows.")
    return summary


with DAG(
    dag_id="san_jose_weather_hw2",
    dag_display_name="san_jose_weather_hw3",
    description="HW3: 60-day San Jose weather full refresh into Snowflake",
    start_date=pendulum.datetime(2026, 9, 1, tz=TIMEZONE),
    schedule="30 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Salabao",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["DATA226", "HW3", "weather", "Snowflake"],
) as dag:
    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)
    verify(loaded)
