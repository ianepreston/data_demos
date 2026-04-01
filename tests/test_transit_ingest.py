from data_demos.ingest import calgary_transit


def test_feed_parsing() -> None:
    """Can I just grab and parse a protobuf?"""
    # Just try and get it and see if you get errors
    _ = calgary_transit.get_feed_protobuf(calgary_transit.VEHICLE_POSITIONS_ID)
