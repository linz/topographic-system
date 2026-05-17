import time
import uuid6
from .uuid7 import reproducable_uuid7

def test_custom_uuidv7():
    ts_ms = int(time.time() * 1000)
    number = 42
    
    custom_uuid = reproducable_uuid7(ts_ms, number)
    assert custom_uuid.version == 7
    
    decoded_uuid = uuid6.UUID(bytes=custom_uuid.bytes)
    assert decoded_uuid.time == ts_ms
