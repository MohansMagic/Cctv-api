import os
import json
import time
import random
from typing import List, Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# Standard starting FEN constant
STARTING_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"

# ==========================================
# 1. DATABASE SETUP
# ==========================================
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./chess.db")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ChessGame(Base):
    __tablename__ = "chess_games"

    id = Column(Integer, primary_key=True, index=True)
    fen = Column(Text, default=STARTING_FEN)
    pgn = Column(Text, default="")
    white_player = Column(String, nullable=True)
    black_player = Column(String, nullable=True)

    # ⏱️ CLOCK FIELDS (Stored in seconds)
    white_time = Column(Float, default=300.0)      # Default: 5 minutes (300s)
    black_time = Column(Float, default=300.0)      # Default: 5 minutes (300s)
    increment = Column(Float, default=0.0)         # Increment per move in seconds
    last_move_time = Column(Float, nullable=True)  # Unix timestamp of last move

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ==========================================
# 2. FASTAPI APP & CORS CONFIGURATION
# ==========================================
app = FastAPI(title="Chess Magic Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================
# 3. WEBSOCKET CONNECTION MANAGER
# ==========================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, game_id: str, websocket: WebSocket):
        await websocket.accept()
        if game_id not in self.active_connections:
            self.active_connections[game_id] = []
        self.active_connections[game_id].append(websocket)

    def disconnect(self, game_id: str, websocket: WebSocket):
        if game_id in self.active_connections:
            if websocket in self.active_connections[game_id]:
                self.active_connections[game_id].remove(websocket)
            if not self.active_connections[game_id]:
                del self.active_connections[game_id]

    async def broadcast(self, game_id: str, message: dict):
        # 🔍 LOG OUTGOING BROADCASTS
        print(f"==================================================", flush=True)
        print(f"📢 [WS BROADCAST] Game: {game_id}", flush=True)
        print(f"Data: {json.dumps(message, indent=2)}", flush=True)
        print(f"==================================================", flush=True)

        if game_id in self.active_connections:
            for connection in self.active_connections[game_id]:
                await connection.send_json(message)

manager = ConnectionManager()


# ==========================================
# 4. MATCHMAKING & REST ENDPOINTS
# ==========================================
waiting_queue: List[Dict[str, str]] = []

class PlayerRequest(BaseModel):
    username: Optional[str] = None

class GuestRequest(BaseModel):
    username: Optional[str] = None

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Chess Magic Backend is running!"}

# 🟢 Queue Matchmaking (Registered users)
@app.post("/api/game/matchmake")
def matchmake(req: PlayerRequest, db: Session = Depends(get_db)):
    global waiting_queue

    username = req.username.strip() if (req.username and req.username.strip()) else f"Guest_{random.randint(1000, 9999)}"

    if any(p["username"] == username for p in waiting_queue):
        return {"status": "WAITING", "username": username, "message": "Already waiting in queue..."}

    if len(waiting_queue) > 0:
        player1 = waiting_queue.pop(0)
        player2 = username

        new_game = ChessGame(
            fen=STARTING_FEN,
            pgn="",  # Left empty; Angular will construct and send the PGN
            white_player=player1["username"],
            black_player=player2,
            white_time=300.0,
            black_time=300.0,
            increment=0.0,
            last_move_time=None
        )
        db.add(new_game)
        db.commit()
        db.refresh(new_game)

        return {
            "status": "MATCHED",
            "gameId": str(new_game.id),
            "color": "b",
            "username": player2,
            "whitePlayer": player1["username"],
            "blackPlayer": player2,
            "fen": new_game.fen,
            "whiteTime": new_game.white_time,
            "blackTime": new_game.black_time
        }

    waiting_queue.append({"username": username})
    return {
        "status": "WAITING",
        "username": username,
        "message": "Looking for opponent..."
    }

# 🟢 Queue Matchmaking (Guest players)
@app.post("/api/game/start-guest")
def start_guest_game(req: GuestRequest, db: Session = Depends(get_db)):
    global waiting_queue

    username = req.username.strip() if (req.username and req.username.strip()) else f"Guest_{random.randint(1000, 9999)}"

    if any(p["username"] == username for p in waiting_queue):
        return {
            "status": "WAITING",
            "username": username,
            "message": "Already waiting in queue..."
        }

    if len(waiting_queue) > 0:
        player1 = waiting_queue.pop(0)  # White
        player2 = username              # Black

        new_game = ChessGame(
            fen=STARTING_FEN,
            pgn="",  # Left empty; Angular will construct and send the PGN
            white_player=player1["username"],
            black_player=player2,
            white_time=300.0,
            black_time=300.0,
            increment=0.0,
            last_move_time=None
        )
        db.add(new_game)
        db.commit()
        db.refresh(new_game)

        return {
            "status": "MATCHED",
            "gameId": str(new_game.id),
            "color": "b",
            "username": player2,
            "whitePlayer": player1["username"],
            "blackPlayer": player2,
            "fen": new_game.fen,
            "whiteTime": new_game.white_time,
            "blackTime": new_game.black_time
        }

    waiting_queue.append({"username": username})
    return {
        "status": "WAITING",
        "username": username,
        "message": "Looking for opponent..."
    }

@app.get("/api/game/status/{username}")
def check_status(username: str, db: Session = Depends(get_db)):
    if any(p["username"] == username for p in waiting_queue):
        return {"status": "WAITING"}

    game = db.query(ChessGame).filter(
        (ChessGame.white_player == username) | (ChessGame.black_player == username)
    ).order_by(ChessGame.id.desc()).first()

    if game:
        color = "w" if game.white_player == username else "b"
        return {
            "status": "MATCHED",
            "gameId": str(game.id),
            "color": color,
            "whitePlayer": game.white_player,
            "blackPlayer": game.black_player
        }

    return {"status": "WAITING"}


# ==========================================
# 5. WEBSOCKET REAL-TIME GAMEPLAY
# ==========================================
@app.websocket("/ws/game/{game_id}/{color}/{username}")
async def websocket_endpoint(
    websocket: WebSocket,
    game_id: str,
    color: str,
    username: str
):
    await manager.connect(game_id, websocket)
    print(f"🟢 [WS CONNECTED] Game: {game_id} | User: {username} | Color: {color}", flush=True)

    # Initial state push on connect
    db_init = SessionLocal()
    try:
        game = db_init.query(ChessGame).filter(ChessGame.id == int(game_id)).first()
        if game:
            init_payload = {
                "type": "INIT",
                "fen": game.fen,
                "pgn": game.pgn or "",
                "whitePlayer": game.white_player,
                "blackPlayer": game.black_player,
                "whiteTime": game.white_time,
                "blackTime": game.black_time,
                "lastMoveTime": game.last_move_time
            }
            print(f"📤 [WS INIT SENT] To {username}:", json.dumps(init_payload, indent=2), flush=True)
            await websocket.send_json(init_payload)
    finally:
        db_init.close()

    try:
        while True:
            data = await websocket.receive_json()

            # 🔍 LOG INCOMING WEBSOCKET PAYLOAD FROM ANGULAR
            print(f"==================================================", flush=True)
            print(f"📥 [WS RECEIVED] Game: {game_id} | From: {username}", flush=True)
            print(f"Payload: {json.dumps(data, indent=2)}", flush=True)
            print(f"==================================================", flush=True)

            msg_type = data.get("type")
            incoming_fen = data.get("fen")
            incoming_pgn = data.get("pgn")
            move_played = data.get("move")

            now = time.time()

            # 💾 PERSISTENCE & TIMER CHECK:
            if (incoming_pgn and incoming_pgn.strip()) or incoming_fen:
                db = SessionLocal()
                try:
                    game = db.query(ChessGame).filter(ChessGame.id == int(game_id)).first()
                    if game:
                        # Process move time deduction
                        if msg_type == "MOVE" and game.last_move_time is not None:
                            elapsed = now - game.last_move_time
                            # If incoming_fen indicates Black's turn next (" b "), White made the move
                            if incoming_fen and " b " in incoming_fen:
                                game.white_time = max(0.0, game.white_time - elapsed + game.increment)
                            else:
                                game.black_time = max(0.0, game.black_time - elapsed + game.increment)

                        if msg_type == "MOVE":
                            game.last_move_time = now

                        if incoming_fen:
                            game.fen = incoming_fen
                        if incoming_pgn and incoming_pgn.strip():
                            game.pgn = incoming_pgn  # Save exact Angular PGN directly

                        db.commit()
                        print(f"💾 [DB SAVED] Game {game_id} updated with new PGN, FEN & Timers.", flush=True)

                        # Broadcast move back to room clients
                        if msg_type == "MOVE":
                            await manager.broadcast(game_id, {
                                "type": "MOVE",
                                "move": move_played,
                                "fen": incoming_fen,
                                "pgn": incoming_pgn,
                                "sender": username,
                                "whiteTime": game.white_time,
                                "blackTime": game.black_time,
                                "lastMoveTime": game.last_move_time
                            })
                except Exception as e:
                    db.rollback()
                    print(f"❌ [DB ERROR] Failed to save move: {e}", flush=True)
                finally:
                    db.close()

    except WebSocketDisconnect:
        manager.disconnect(game_id, websocket)
        print(f"🔴 [WS DISCONNECTED] Game: {game_id} | User: {username}", flush=True)
        await manager.broadcast(game_id, {
            "type": "SYSTEM",
            "message": f"Player {username} disconnected."
        })
