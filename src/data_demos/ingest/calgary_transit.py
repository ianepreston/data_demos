"""Ingest data from Calgary Transit.

https://data.calgary.ca/Transportation-Transit/Calgary-Transit-Realtime-Vehicle-Positions-GTFS-RT/am7c-qe3u/about_data
https://data.calgary.ca/Transportation-Transit/Calgary-Transit-Realtime-Service-Alerts-GTFS-RT/jhgn-ynqj/about_data
https://data.calgary.ca/Transportation-Transit/Calgary-Transit-Realtime-Trip-Updates-GTFS-RT/gs4m-mdc2/about_data
"""

from io import BytesIO
import json
import logging
import requests
from databricks.sdk import WorkspaceClient
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError

logger = logging.getLogger(__name__)

VEHICLE_POSITIONS_ID: str = "am7c-qe3u"
SERVICE_ALERTS_ID: str = "jhgn-ynqj"
TRIP_UPDATES_ID: str = "gs4m-mdc2"
W = WorkspaceClient()


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
    return feed_dict


def protobuf_to_volume(data_id, volume_path):
    feed_dict = get_feed_protobuf(data_id)
    if feed_dict is None:
        return
    ts_suffix = feed_dict["header"]["timestamp"]
    fs_path = f"{volume_path}/{data_id}/{ts_suffix}.json"
    W.files.upload(fs_path, BytesIO(json.dumps(feed_dict).encode("utf-8")))
