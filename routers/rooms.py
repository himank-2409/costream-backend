import secrets
import string

from fastapi import APIRouter, HTTPException, status

from core.room_manager import RoomState, room_manager
from models.schemas import CreateRoomRequest, RoomResponse

# HTTP endpoints for creating rooms and loading initial room state.

router = APIRouter(prefix="/rooms", tags=["rooms"])

ROOM_CODE_ALPHABET = string.ascii_uppercase + string.digits
ROOM_CODE_LENGTH = 6


def serialize_room(room: RoomState) -> RoomResponse:
    # Keep HTTP responses stable and omit internal-only counters.
    return RoomResponse(
        room_id=room.room_id,
        host_id=room.host_id,
        guest_id=room.guest_id,
        media_url=room.media_url,
        is_playing=room.is_playing,
        host_position=room.host_position,
        host_timestamp=room.host_timestamp,
        created_at=room.created_at,
        last_activity=room.last_activity,
    )


def generate_room_code() -> str:
    # Six characters keeps the room code easy to share by voice or text.
    return "".join(secrets.choice(ROOM_CODE_ALPHABET) for _ in range(ROOM_CODE_LENGTH))


@router.post("", response_model=RoomResponse, status_code=status.HTTP_201_CREATED)
async def create_room(payload: CreateRoomRequest) -> RoomResponse:
    # Retry on the very unlikely chance that a random room code collides.
    for _ in range(10):
        room_id = generate_room_code()
        try:
            room = await room_manager.create_room(
                room_id=room_id,
                host_id=payload.host_id,
                media_url=payload.media_url,
            )
            return serialize_room(room)
        except ValueError:
            continue

    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Could not allocate a unique room code",
    )


@router.get("/{room_id}", response_model=RoomResponse)
async def get_room(room_id: str) -> RoomResponse:
    # Lets the frontend validate a room before opening the WebSocket.
    room = await room_manager.get_room(room_id.upper())
    if not room:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Room not found")

    return serialize_room(room)
