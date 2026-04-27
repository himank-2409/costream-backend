import asyncio
import time

from core.room_manager import RoomManager
from routers.ws import connections_lock, room_connections

# Background cleanup loop for removing inactive in-memory rooms.

ROOM_TTL_SECONDS = 60 * 60 * 2
CLEANUP_INTERVAL_SECONDS = 60


async def start_cleanup_loop(room_manager: RoomManager) -> None:
    # Runs forever as a background task started by FastAPI lifespan.
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
        now = time.time()

        # Find expired rooms while holding the room manager lock.
        async with room_manager.lock:
            expired_rooms = [
                (room_id, now - room.last_activity)
                for room_id, room in room_manager.rooms.items()
                if now - room.last_activity > ROOM_TTL_SECONDS
            ]

        for room_id, age in expired_rooms:
            # Detach any live sockets before removing the room.
            async with connections_lock:
                websockets = list(room_connections.pop(room_id, {}).values())

            for websocket in websockets:
                try:
                    await websocket.send_json({"type": "room_expired"})
                    await websocket.close()
                except Exception:
                    pass

            # Delete the room and log what was removed.
            async with room_manager.lock:
                room_manager.rooms.pop(room_id, None)
            print(f"Deleted room {room_id} after {age:.0f}s inactivity")
