import asyncio
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.cleanup import start_cleanup_loop
from core.room_manager import room_manager
from routers import rooms, ws

# Application entrypoint for the CoStream FastAPI backend.
# Run locally with: uvicorn main:app --reload --port 8000

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Starts the room cleanup loop when the API starts.
    cleanup_task = asyncio.create_task(start_cleanup_loop(room_manager))
    try:
        yield
    finally:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="CoStream API", lifespan=lifespan)

cors_origin = os.getenv("CORS_ORIGIN", "http://localhost:3000")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[cors_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registers HTTP room routes and synchronized watch WebSocket routes.
app.include_router(rooms.router)
app.include_router(ws.router)


@app.get("/")
async def root():
    # Minimal health-style response for quick local verification.
    return {"name": "CoStream API", "status": "ok"}
