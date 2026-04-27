import asyncio
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from core.room_manager import RoomState, room_manager
from core.sync_engine import get_current_position

# WebSocket traffic for synchronized two-person watch rooms.

router = APIRouter(tags=["websocket"])

# Process-local connection registry:
# {room_id: {user_id: websocket}}
room_connections: Dict[str, Dict[str, WebSocket]] = {}
connections_lock = asyncio.Lock()


def room_snapshot(room: RoomState) -> Dict[str, Any]:
    # Full room payload used for initial hydration and sync messages.
    return {
        "room_id": room.room_id,
        "host_id": room.host_id,
        "guest_id": room.guest_id,
        "media_url": room.media_url,
        "is_playing": room.is_playing,
        "host_position": room.host_position,
        "current_position": get_current_position(room),
        "host_timestamp": room.host_timestamp,
        "created_at": room.created_at,
        "last_activity": room.last_activity,
    }


async def register_connection(room_id: str, user_id: str, websocket: WebSocket) -> None:
    # Stores or replaces this user's live WebSocket for the room.
    async with connections_lock:
        room_connections.setdefault(room_id, {})[user_id] = websocket


async def unregister_connection(
    room_id: str,
    user_id: str,
    websocket: WebSocket,
) -> bool:
    # Removes this exact WebSocket and prunes empty room connection buckets.
    # The identity check protects newer reconnects from older sockets closing.
    async with connections_lock:
        connections = room_connections.get(room_id)
        if not connections:
            return False

        if connections.get(user_id) is not websocket:
            return False

        connections.pop(user_id, None)
        if not connections:
            room_connections.pop(room_id, None)

        return True


async def get_connection(room_id: str, user_id: str) -> Optional[WebSocket]:
    # Looks up a single user's WebSocket without exposing the registry.
    async with connections_lock:
        return room_connections.get(room_id, {}).get(user_id)


async def send_to_user(room_id: str, user_id: str, message: Dict[str, Any]) -> None:
    # Sends a JSON message to one connected user when present.
    websocket = await get_connection(room_id, user_id)
    if websocket:
        await websocket.send_json(message)


async def broadcast_to_all(room_id: str, message: Dict[str, Any]) -> None:
    # Broadcasts a JSON message to every connected user in the room.
    async with connections_lock:
        targets = list(room_connections.get(room_id, {}).values())

    for websocket in targets:
        await websocket.send_json(message)


async def broadcast_to_others(
    room_id: str,
    sender_id: str,
    message: Dict[str, Any],
) -> None:
    # Broadcasts a JSON message to every user except the sender.
    async with connections_lock:
        targets = [
            websocket
            for user_id, websocket in room_connections.get(room_id, {}).items()
            if user_id != sender_id
        ]

    for websocket in targets:
        await websocket.send_json(message)


async def broadcast_to_guest(room: RoomState, message: Dict[str, Any]) -> None:
    # Host ticks are useful only to the guest, so avoid echoing them to host.
    if room.guest_id:
        await send_to_user(room.room_id, room.guest_id, message)


async def send_error(websocket: WebSocket, detail: str) -> None:
    # Soft protocol errors keep the socket open for subsequent valid messages.
    await websocket.send_json(
        {
            "type": "error",
            "detail": detail,
            "server_timestamp": time.time(),
        }
    )


def get_position(payload: Dict[str, Any]) -> float:
    # Normalize incoming numeric positions from JSON.
    return float(payload.get("position", 0.0))


async def handle_playback_message(
    room_id: str,
    message_type: str,
    payload: Dict[str, Any],
) -> Optional[RoomState]:
    # Updates shared playback position and play/pause status.
    position = get_position(payload)
    room = await room_manager.update_position(room_id, position)
    if not room:
        return None

    if message_type == "play":
        return await room_manager.set_playing(room_id, True)

    if message_type == "pause":
        return await room_manager.set_playing(room_id, False)

    return room


async def handle_room_message(
    room_id: str,
    user_id: str,
    websocket: WebSocket,
    payload: Dict[str, Any],
) -> None:
    # Dispatches all supported client message types.
    message_type = payload.get("type")
    server_timestamp = time.time()

    if message_type == "play":
        room = await handle_playback_message(room_id, message_type, payload)
        if room:
            await broadcast_to_all(
                room_id,
                {
                    "type": "sync",
                    "is_playing": True,
                    "position": room.host_position,
                    "server_timestamp": server_timestamp,
                },
            )
        return

    if message_type == "pause":
        room = await handle_playback_message(room_id, message_type, payload)
        if room:
            await broadcast_to_all(
                room_id,
                {
                    "type": "pause",
                    "is_playing": False,
                    "position": room.host_position,
                    "server_timestamp": server_timestamp,
                },
            )
        return

    if message_type == "seek":
        position = get_position(payload)
        room = await room_manager.update_position(room_id, position)
        if room:
            await broadcast_to_all(
                room_id,
                {
                    "type": "seek",
                    "is_playing": room.is_playing,
                    "position": room.host_position,
                    "server_timestamp": server_timestamp,
                },
            )
        return

    if message_type == "tick":
        room = await room_manager.get_room(room_id)
        if not room:
            await send_error(websocket, "Room not found")
            return

        if user_id != room.host_id:
            await send_error(websocket, "Only the host can send tick messages")
            return

        updated_room = await room_manager.update_position(room_id, get_position(payload))
        if updated_room:
            await broadcast_to_guest(
                updated_room,
                {
                    "type": "sync",
                    "is_playing": updated_room.is_playing,
                    "position": updated_room.host_position,
                    "server_timestamp": server_timestamp,
                },
            )
        return

    if message_type == "set_media":
        room = await room_manager.get_room(room_id)
        if not room:
            await send_error(websocket, "Room not found")
            return

        if user_id != room.host_id:
            await send_error(websocket, "Only the host can set media")
            return

        updated_room = await room_manager.set_media(room_id, payload.get("url"))
        if updated_room:
            await broadcast_to_all(
                room_id,
                {
                    "type": "set_media",
                    "url": updated_room.media_url,
                    "server_timestamp": server_timestamp,
                },
            )
        return

    if message_type == "buffer_start":
        await broadcast_to_all(
            room_id,
            {
                "type": "buffer_start",
                "user_id": user_id,
                "server_timestamp": server_timestamp,
            },
        )
        return

    if message_type == "buffer_end":
        await broadcast_to_all(
            room_id,
            {
                "type": "buffer_end",
                "user_id": user_id,
                "server_timestamp": server_timestamp,
            },
        )
        return

    if message_type == "volume":
        # Volume is a host-local control event; accept it without changing
        # shared playback state so the server does not emit protocol errors.
        return

    if message_type == "chat":
        sequence_id = await room_manager.next_chat_sequence(room_id)
        if sequence_id is None:
            await send_error(websocket, "Room not found")
            return

        await broadcast_to_all(
            room_id,
            {
                "type": "chat",
                "user_id": user_id,
                "text": payload.get("text", ""),
                "server_timestamp": server_timestamp,
                "sequence_id": sequence_id,
            },
        )
        return

    await send_error(websocket, f"Unsupported message type: {message_type}")


@router.websocket("/ws/{room_id}/{user_id}")
async def room_websocket(websocket: WebSocket, room_id: str, user_id: str) -> None:
    # Normalize short room codes so URLs work regardless of typed casing.
    room_id = room_id.upper()

    room = await room_manager.get_room(room_id)
    if not room:
        await websocket.accept()
        await websocket.close(code=4004)
        return

    role = await room_manager.register_user(room_id, user_id)
    if role is None:
        await websocket.accept()
        await websocket.close(code=4004)
        return

    if role == "full":
        await websocket.accept()
        await websocket.close(code=4003)
        return

    await websocket.accept()
    await register_connection(room_id, user_id, websocket)

    # Hydrate only the connecting user with the full current room state.
    room = await room_manager.get_room(room_id)
    if room:
        await websocket.send_json(
            {
                "type": "snapshot",
                "role": role,
                "room": room_snapshot(room),
                "server_timestamp": time.time(),
            }
        )

    # Let the already-connected peer update their presence UI.
    await broadcast_to_others(
        room_id,
        user_id,
        {
            "type": "peer_joined",
            "user_id": user_id,
            "role": role,
            "server_timestamp": time.time(),
        },
    )

    try:
        while True:
            payload = await websocket.receive_json()
            await handle_room_message(room_id, user_id, websocket, payload)
    except WebSocketDisconnect:
        pass
    finally:
        removed_current_socket = await unregister_connection(room_id, user_id, websocket)
        if not removed_current_socket:
            return

        room = await room_manager.remove_user(room_id, user_id)

        if room:
            # Notify the remaining peer and include the updated host assignment.
            await broadcast_to_all(
                room_id,
                {
                    "type": "peer_left",
                    "user_id": user_id,
                    "host_id": room.host_id,
                    "guest_id": room.guest_id,
                    "server_timestamp": time.time(),
                },
            )
