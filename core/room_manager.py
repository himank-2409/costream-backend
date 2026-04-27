import time
from dataclasses import dataclass
from typing import Dict, Optional
import asyncio

# In-memory room state and mutation helpers for private two-user watch rooms.


@dataclass
class RoomState:
    room_id: str
    host_id: str
    guest_id: Optional[str]
    media_url: Optional[str]
    is_playing: bool
    host_position: float
    host_timestamp: float
    created_at: float
    last_activity: float
    chat_sequence: int


class RoomManager:
    # Keeps all active rooms in memory behind an asyncio lock.

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.rooms: Dict[str, RoomState] = {}

    async def create_room(
        self,
        room_id: str,
        host_id: str,
        media_url: Optional[str] = None,
    ) -> RoomState:
        # Room IDs and host IDs are supplied by the API layer so clients can
        # create short shareable codes and keep their own user identity.
        now = time.time()
        room = RoomState(
            room_id=room_id,
            host_id=host_id,
            guest_id=None,
            media_url=media_url,
            is_playing=False,
            host_position=0.0,
            host_timestamp=now,
            created_at=now,
            last_activity=now,
            chat_sequence=0,
        )

        async with self.lock:
            if room.room_id in self.rooms:
                raise ValueError("room_id already exists")

            self.rooms[room.room_id] = room

        return room

    async def get_room(self, room_id: str) -> Optional[RoomState]:
        async with self.lock:
            room = self.rooms.get(room_id)
            if room:
                room.last_activity = time.time()
            return room

    async def update_position(self, room_id: str, position: float) -> Optional[RoomState]:
        async with self.lock:
            room = self.rooms.get(room_id)
            if not room:
                return None

            now = time.time()
            room.host_position = position
            room.host_timestamp = now
            room.last_activity = now
            return room

    async def set_playing(self, room_id: str, is_playing: bool) -> Optional[RoomState]:
        async with self.lock:
            room = self.rooms.get(room_id)
            if not room:
                return None

            room.is_playing = is_playing
            room.last_activity = time.time()
            return room

    async def set_media(self, room_id: str, media_url: Optional[str]) -> Optional[RoomState]:
        async with self.lock:
            room = self.rooms.get(room_id)
            if not room:
                return None

            room.media_url = media_url
            room.last_activity = time.time()
            return room

    async def register_user(self, room_id: str, user_id: str) -> Optional[str]:
        # Assigns a connecting user to the host or guest slot.
        # Returns the user's role, or None when the room does not exist.
        async with self.lock:
            room = self.rooms.get(room_id)
            if not room:
                return None

            room.last_activity = time.time()

            if user_id == room.host_id:
                return "host"

            if room.guest_id is None:
                room.guest_id = user_id
                return "guest"

            if user_id == room.guest_id:
                return "guest"

            return "full"

    async def remove_user(self, room_id: str, user_id: str) -> Optional[RoomState]:
        # Removes a user from the room identity slots.
        # If the host leaves while a guest remains, the guest becomes host.
        async with self.lock:
            room = self.rooms.get(room_id)
            if not room:
                return None

            if user_id == room.host_id:
                if room.guest_id:
                    room.host_id = room.guest_id
                    room.guest_id = None
            elif user_id == room.guest_id:
                room.guest_id = None

            room.last_activity = time.time()
            return room

    async def next_chat_sequence(self, room_id: str) -> Optional[int]:
        # Produces a monotonically increasing chat sequence per room.
        async with self.lock:
            room = self.rooms.get(room_id)
            if not room:
                return None

            room.chat_sequence += 1
            room.last_activity = time.time()
            return room.chat_sequence


# Shared process-local manager used by HTTP routes, WebSockets, and cleanup.
room_manager = RoomManager()
