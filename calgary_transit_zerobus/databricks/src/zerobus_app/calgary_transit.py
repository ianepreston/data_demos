"""Ingest data from Calgary Transit.

https://data.calgary.ca/Transportation-Transit/Calgary-Transit-Realtime-Vehicle-Positions-GTFS-RT/am7c-qe3u/about_data
https://data.calgary.ca/Transportation-Transit/Calgary-Transit-Realtime-Service-Alerts-GTFS-RT/jhgn-ynqj/about_data
https://data.calgary.ca/Transportation-Transit/Calgary-Transit-Realtime-Trip-Updates-GTFS-RT/gs4m-mdc2/about_data
"""

import json
import os
import time
import logging

import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

logger = logging.getLogger(__name__)

VEHICLE_POSITIONS_ID: str = "am7c-qe3u"
SERVICE_ALERTS_ID: str = "jhgn-ynqj"
TRIP_UPDATES_ID: str = "gs4m-mdc2"


def get_blob_id(data_id: str) -> str | None:
    """Retrieve the blob ID for a data source.

    Blob IDs change every time the data is refreshed (every 30 seconds) so you always have to call this.
    """
    url: str = f"https://data.calgary.ca/api/views/{data_id}.json"
    try:
        response: requests.Response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException:
        logger.exception("Failed to fetch blob_id metadata for %s", data_id)
        return None
    try:
        return json.loads(response.content)["blobId"]
    except (ValueError, KeyError):
        logger.exception(
            "Unexpected blob_id response for %s: status=%s, body=%s",
            data_id,
            response.status_code,
            response.content[:500],
        )
        return None


def get_feed_protobuf(data_id: str):
    """Should make a spec for this at some point, but let's just try it."""
    blob_id = get_blob_id(data_id)
    if blob_id is None:
        return None
    url: str = (
        f"https://data.calgary.ca/api/views/{data_id}/files/{blob_id}?download=true"
    )
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException:
        logger.exception("Failed to fetch feed for %s (blob_id=%s)", data_id, blob_id)
        return None
    try:
        feed.ParseFromString(response.content)
    except DecodeError:
        logger.warning(
            "Failed to parse protobuf for %s: status=%s, content-type=%s, len=%d, body=%s",
            data_id,
            response.status_code,
            response.headers.get("content-type"),
            len(response.content),
            response.content[:500],
        )
        return None
    feed_dict = MessageToDict(feed)
    feed_dict["blob_id"] = blob_id
    feed_dict["data_id"] = data_id
    return feed_dict


def protobuf_to_zerobus(
    feed_dict: dict,
    server_endpoint: str,
    workspace_url: str,
    full_table_name: str,
    client_id: str,
    client_secret: str,
):
    """Write calgary transit data to a delta table using zerobus

    Args:
        feed_dict: The parsed feed from protobuf
        server_endpoint: https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint
        workspace_url: URL of your target databricks workspace
        full_table_name: catalog.schema.table format
        client_id: ID of the service principal for the operation
        client_secret: secret of the service principal for the operation
    """
    data_id = feed_dict.get("data_id", "<unknown>")
    if "header" not in feed_dict or "timestamp" not in feed_dict.get("header", {}):
        logger.warning(
            "Parsed protobuf for %s is missing header/timestamp. Keys: %s, content: %s",
            data_id,
            list(feed_dict.keys()),
            json.dumps(feed_dict)[:500],
        )
        return
    update_ts = int(feed_dict["header"]["timestamp"]) * 1_000_000
    blob_id: str = feed_dict["blob_id"]
    record = {
        "update_ts": update_ts,
        "data_id": data_id,
        "blob_id": blob_id,
        "payload": json.dumps(feed_dict),
    }
    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_properties = TableProperties(full_table_name)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)
    stream = sdk.create_stream(client_id, client_secret, table_properties, options)
    try:
        offset = stream.ingest_record_offset(record)
        stream.wait_for_offset(offset)
    except Exception:
        logger.exception(
            "Zerobus ingest failed for %s (blob_id=%s, table=%s). Record keys=%s, payload sample=%s",
            data_id,
            blob_id,
            full_table_name,
            list(record.keys()),
            json.dumps(record, default=str)[:1000],
        )
        raise
    finally:
        stream.close()


def main():
    """Do the actual ingestion."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    server_endpoint = os.getenv("SERVER_ENDPOINT")
    workspace_url = os.getenv("DATABRICKS_WORKSPACE_URL")
    client_id = os.getenv("ZEROBUS_CLIENT_ID")
    client_secret = os.getenv("ZEROBUS_CLIENT_SECRET")
    keys = [VEHICLE_POSITIONS_ID, SERVICE_ALERTS_ID, TRIP_UPDATES_ID]
    # last blob_id tracker so we don't write duplicates
    last_blobs = {key: "" for key in keys}
    while True:
        for data_id in keys:
            try:
                feed_dict = get_feed_protobuf(data_id)
                if feed_dict is None:
                    logger.warning("Skipping %s: no feed available this cycle", data_id)
                    continue
                blob_id = feed_dict.get("blob_id")
                if blob_id == last_blobs[data_id]:
                    logger.debug("Skipping %s: blob_id %s unchanged", data_id, blob_id)
                    continue
                logger.info(
                    "Ingesting %s (blob_id=%s, prev=%s)",
                    data_id,
                    blob_id,
                    last_blobs[data_id] or "<none>",
                )
                protobuf_to_zerobus(
                    feed_dict,
                    server_endpoint,
                    workspace_url,
                    "classic_stable_ptbvhz_ip.calgary_transit.calgary_transit_ingest",
                    client_id,
                    client_secret,
                )
                last_blobs[data_id] = blob_id
            except Exception:
                logger.exception(
                    "Iteration failed for %s; continuing with next feed", data_id
                )
        time.sleep(10)


if __name__ == "__main__":
    main()
