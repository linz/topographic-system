import hashlib
import uuid


def reproducable_uuid7(timestamp_ms: int, fid: int) -> uuid.UUID:
    """
    Generates a UUIDv7 using the given timestamp (in milliseconds) and
    the SHA256 hash of the given integer.
    """
    return reproducable_uuid7_text(timestamp_ms, str(fid))


def reproducable_uuid7_text(timestamp_ms: int, text: str) -> uuid.UUID:
    """
    Generates a UUIDv7 using the given timestamp (in milliseconds) and
    the SHA256 hash of the given integer.
    """
    # 1. 48 bits for the timestamp
    ts_bytes = timestamp_ms.to_bytes(6, byteorder="big")

    # 2. SHA256 of the integer
    hash_obj = hashlib.sha256(text.encode("utf-8"))
    hash_bytes = hash_obj.digest()

    # 3. Construct the 16 bytes of the UUID
    uuid_bytes = bytearray(16)

    # bytes 0-5: timestamp
    uuid_bytes[0:6] = ts_bytes

    # bytes 6-15: randomness from hash
    uuid_bytes[6:16] = hash_bytes[0:10]

    # Apply version 7 mask to byte 6
    uuid_bytes[6] = (uuid_bytes[6] & 0x0F) | 0x70

    # Apply variant 10 mask to byte 8
    uuid_bytes[8] = (uuid_bytes[8] & 0x3F) | 0x80

    return uuid.UUID(bytes=bytes(uuid_bytes))
