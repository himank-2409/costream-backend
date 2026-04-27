from typing import Optional

from pydantic import BaseModel

# Pydantic request and response schemas shared by CoStream API endpoints.


class CreateRoomRequest(BaseModel):
    host_id: str
    media_url: Optional[str] = None


class RoomResponse(BaseModel):
    room_id: str
    host_id: str
    guest_id: Optional[str] = None
    media_url: Optional[str] = None
    is_playing: bool
    host_position: float
    host_timestamp: float
    created_at: float
    last_activity: float


class ChatMessage(BaseModel):
    user_id: str
    text: str
    timestamp: float
