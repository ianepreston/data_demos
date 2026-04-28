"""Ingest data from Calgary Transit.

https://data.calgary.ca/Transportation-Transit/Calgary-Transit-Realtime-Vehicle-Positions-GTFS-RT/am7c-qe3u/about_data
https://data.calgary.ca/Transportation-Transit/Calgary-Transit-Realtime-Service-Alerts-GTFS-RT/jhgn-ynqj/about_data
https://data.calgary.ca/Transportation-Transit/Calgary-Transit-Realtime-Trip-Updates-GTFS-RT/gs4m-mdc2/about_data
"""

import json
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


def get_blob_id(data_id: str) -> str:
    """Retrieve the blob ID for a data source.

    Blob IDs change every time the data is refreshed (every 30 seconds) so you always have to call this.
    """
    url: str = f"https://data.calgary.ca/api/views/{data_id}.json"
    response: requests.Response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.content)["blobId"]


def get_feed_protobuf(data_id: str):
    """Should make a spec for this at some point, but let's just try it."""
    blob_id = get_blob_id(data_id)
    url: str = (
        f"https://data.calgary.ca/api/views/{data_id}/files/{blob_id}?download=true"
    )
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url)
    response.raise_for_status()
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
    return feed_dict


def protobuf_to_zerobus(
    data_id: str,
    server_endpoint: str,
    workspace_url: str,
    full_table_name: str,
    client_id: str,
    client_secret: str,
) -> None:
    """Write calgary transit data to a delta table using zerobus

    Args:
        data_id: The identifier of the series. See constants at the top of this module
        server_endpoint: https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint
        workspace_url: URL of your target databricks workspace
        full_table_name: catalog.schema.table format
        client_id: ID of the service principal for the operation
        client_secret: secret of the service principal for the operation
    """
    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_properties = TableProperties(full_table_name)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)
    stream = sdk.create_stream(client_id, client_secret, table_properties, options)
    feed_dict = get_feed_protobuf(data_id)
    if feed_dict is None:
        return
    if "header" not in feed_dict or "timestamp" not in feed_dict.get("header", {}):
        logger.warning(
            "Parsed protobuf for %s is missing header/timestamp. Keys: %s, content: %s",
            data_id,
            list(feed_dict.keys()),
            json.dumps(feed_dict)[:500],
        )
        return
    update_ts = feed_dict["header"]["timestamp"]
    blob_id = feed_dict["blob_id"]
    record = {"update_ts": update_ts, "blob_id": blob_id, "payload": feed_dict}
    try:
        offset = stream.ingest_record_offset(record)
        stream.wait_for_offset(offset)
    finally:
        stream.close()
