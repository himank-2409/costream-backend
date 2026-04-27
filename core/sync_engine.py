import time

from core.room_manager import RoomState

HARD_SYNC_THRESHOLD = 2.0


def build_snapshot(room: RoomState) -> dict:
    """Return a full room-state payload for a connecting or reconnecting user."""
    return {
        "room_id": room.room_id,
        "host_id": room.host_id,
        "guest_id": room.guest_id,
        "media_url": room.media_url,
        "is_playing": room.is_playing,
        "host_position": room.host_position,
        "host_timestamp": room.host_timestamp,
        "server_ts": time.time(),
    }


def calc_expected_position(host_position: float, host_timestamp: float, server_ts: float) -> float:
    """Return active-playback position advanced by elapsed server time."""
    return host_position + (server_ts - host_timestamp)


def build_sync_message(room: RoomState) -> dict:
    """Return the sync event the guest should apply for this room state."""
    server_ts = time.time()
    position = room.host_position
    if room.is_playing:
        position = calc_expected_position(room.host_position, room.host_timestamp, server_ts)

    return {
        "type": "sync",
        "position": position,
        "playing": room.is_playing,
        "server_ts": server_ts,
    }


def should_hard_sync(current_pos: float, expected_pos: float, threshold: float = HARD_SYNC_THRESHOLD) -> bool:
    """Return True when absolute playback drift exceeds the hard threshold."""
    return abs(current_pos - expected_pos) > threshold


def should_soft_sync(current_pos: float, expected_pos: float, threshold: float = 0.3) -> bool:
    """Return True when drift exceeds the minor threshold but is not hard."""
    drift = abs(current_pos - expected_pos)
    return threshold < drift <= HARD_SYNC_THRESHOLD


def get_current_position(room: RoomState) -> float:
    """Return the room's current position, advancing only while playing."""
    if not room.is_playing:
        return room.host_position

    return calc_expected_position(room.host_position, room.host_timestamp, time.time())
