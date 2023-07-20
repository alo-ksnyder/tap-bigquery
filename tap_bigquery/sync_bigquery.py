import copy, json, time 
import dateutil.parser
from decimal import Decimal
from datetime import datetime, timedelta

from os import environ
import singer
import singer.metrics as metrics

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

from . import utils
import getschema


LOGGER = utils.get_logger(__name__)

# StitchData compatible timestamp meta data
#  https://www.stitchdata.com/docs/data-structure/system-tables-and-columns
# The timestamp of the record extracted from the source
EXTRACT_TIMESTAMP = "_sdc_extracted_at"
# The timestamp of the record submit to the destination
# (kept null at extraction)
BATCH_TIMESTAMP = "_sdc_batched_at"
# Legacy timestamp field
LEGACY_TIMESTAMP = "_etl_tstamp"

BOOKMARK_KEY_NAME = "last_update"

SERVICE_ACCOUNT_INFO_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS_STRING"
credentials_json = environ.get(SERVICE_ACCOUNT_INFO_ENV_VAR)

def increment_table_date(datetime_str):
    d = datetime.strptime(datetime_str, "%Y%m%d") + timedelta(days=1)
    return datetime.strftime(d, "%Y%m%d")


def get_bigquery_credentials():
    return service_account.Credentials.from_service_account_info(
        json.loads(credentials_json),
    )


def _build_query(keys, filters=[], inclusive_start=True, limit=None):
    columns = ",".join(keys["columns"])
    if "*" not in columns and keys["datetime_key"] not in columns:
        columns = columns + "," + keys["datetime_key"]
    keys["columns"] = columns

    # TODO Select specific tables from the event_ individually to reduce payload
    # on ingest.
    query = ("SELECT {columns} FROM {table}{start_datetime} WHERE 1=1").format(**keys)

    if limit is not None:
        query = query + " LIMIT %d" % limit

    return query


def do_discover(config, stream, output_schema_file=None, add_timestamp=True):
    start_datetime = config.get("start_datetime")

    end_datetime = None
    if config.get("end_datetime"):
        end_datetime = config.get("end_datetime")

    keys = {
        "table": stream["table"],
        "columns": stream["columns"],
        "datetime_key": stream["datetime_key"],
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
    }

    project_id = config.get("project_id", "alo-project-prod")
    bq_credentials = get_bigquery_credentials()
    # Generate schema from first 100 rows
    query = _build_query(keys, stream.get("filters"), limit=100)

    LOGGER.info("Running query:\n    " + query)

    df = pd.read_gbq(
        query=query,
        use_bqstorage_api=True,
        project_id=project_id,
        credentials=bq_credentials,
    )

    data = []
    for row in df.to_json(orient="records", lines=True).splitlines():
        record = json.loads(row)
        data.append(record)

    if not data:
        raise Exception("Cannot infer schema: No record returned.")

    schema = getschema.infer_schema(data)
    if add_timestamp:
        timestamp_format = {"type": ["null", "string"], "format": "date-time"}
        schema["properties"][EXTRACT_TIMESTAMP] = timestamp_format
        schema["properties"][BATCH_TIMESTAMP] = timestamp_format
        # Support the legacy field
        schema["properties"][LEGACY_TIMESTAMP] = {
            "type": ["null", "number"],
            "inclusion": "automatic",
        }

    if output_schema_file:
        with open(output_schema_file, "w") as f:
            json.dump(schema, f, indent=2)

    stream_metadata = [
        {
            "metadata": {
                "selected": True,
                "table": stream["table"],
                "columns": stream["columns"],
                "filters": stream.get("filters", []),
                "datetime_key": stream["datetime_key"]
                # "inclusion": "available",
                # "table-key-properties": ["id"],
                # "valid-replication-keys": ["date_modified"],
                # "schema-name": "users"
            },
            "breadcrumb": [],
        }
    ]

    # TODO: Need to put something in here?
    key_properties = []

    catalog = {
        "selected": True,
        "type": "object",
        "stream": stream["name"],
        "key_properties": key_properties,
        "properties": schema["properties"],
    }

    return stream_metadata, key_properties, catalog


def do_sync(config, state, stream):
    singer.set_currently_syncing(state, stream.tap_stream_id)
    singer.write_state(state)

    metadata = stream.metadata[0]["metadata"]
    tap_stream_id = stream.tap_stream_id

    inclusive_start = True
    start_datetime = singer.get_bookmark(state, tap_stream_id, BOOKMARK_KEY_NAME)

    if state.get("bookmarks") is not None:
        start_datetime = increment_table_date(start_datetime)

    if start_datetime:
        if not config.get("start_always_inclusive"):
            inclusive_start = False
    else:
        start_datetime = config.get("start_datetime")
    start_datetime = start_datetime

    if config.get("end_datetime"):
        end_datetime = config.get("end_datetime")

    singer.write_schema(tap_stream_id, stream.schema.to_dict(), stream.key_properties)

    keys = {
        "table": metadata["table"],
        "columns": metadata["columns"],
        "datetime_key": metadata.get("datetime_key"),
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
    }

    limit = config.get("limit", None)
    project_id = config.get("project_id", "alo-project-prod")
    bq_credentials = get_bigquery_credentials()
    query = _build_query(
        keys, metadata.get("filters", []), inclusive_start, limit=limit
    )

    last_update = start_datetime

    LOGGER.info("Running query:\n    %s" % query)

    df = pd.read_gbq(
        query=query,
        use_bqstorage_api=True,
        project_id=project_id,
        credentials=bq_credentials,
    )

    with metrics.record_counter(tap_stream_id) as counter:
        for row in df.to_json(orient="records", lines=True).splitlines():
            record = json.loads(row)
            last_update = record[keys["datetime_key"]]
            singer.write_record(stream.stream, record)
            counter.increment()

    state = singer.write_bookmark(state, tap_stream_id, BOOKMARK_KEY_NAME, last_update)

    singer.write_state(state)
