import time

import uuid6

from .uuid7 import reproducable_uuid7, reproducable_uuid7_text


def test_custom_uuidv7():
    ts_ms = int(time.time() * 1000)
    number = 42

    custom_uuid = reproducable_uuid7(ts_ms, number)
    assert custom_uuid.version == 7

    decoded_uuid = uuid6.UUID(bytes=custom_uuid.bytes)
    assert decoded_uuid.time == ts_ms


def test_custom_uuidv7_text():
    ts_ms = int(time.time() * 1000)
    text = "nz_bounty_islands_rock_points:123"

    custom_uuid = reproducable_uuid7_text(ts_ms, text)
    assert custom_uuid.version == 7

    decoded_uuid = uuid6.UUID(bytes=custom_uuid.bytes)
    assert decoded_uuid.time == ts_ms

    # Ensure it is reproducible
    custom_uuid_dup = reproducable_uuid7_text(ts_ms, text)
    assert custom_uuid == custom_uuid_dup
