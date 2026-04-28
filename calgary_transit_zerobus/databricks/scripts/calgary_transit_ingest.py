"""Write Calgary Transit data to json in a volume.

This version is meant to be run locally and land the files in a volume for later processing with autoloader.
By default it will run indefinitely. You could also make a version that runs once and then have some sort of cron
job schedule it to run every 30 seconds, but the idea here is for me to just have a simple way to
dump some json into a volume so I can demo downstream capabilities.
"""

import time
from data_demos.ingest import calgary_transit

VOLUME_PATH = "/Volumes/ian_preston/ingest/calgary_transit"

while True:
    for data_id in (
        calgary_transit.VEHICLE_POSITIONS_ID,
        calgary_transit.SERVICE_ALERTS_ID,
        calgary_transit.TRIP_UPDATES_ID,
    ):
        calgary_transit.protobuf_to_volume(data_id, VOLUME_PATH)
        time.sleep(30)
