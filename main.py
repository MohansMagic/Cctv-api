import os
import json
import random
from typing import List, Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# Standard starting FEN constant (no need for external chess library)
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

# Database Model
class ChessGame(Base):
    __tablename__ = "chess_games"

    id = Column(Integer, primary_key=True, index=True)
    fen = Column(Text, default=STARTING_FEN)
    pgn = Column(Text, default="")  # Stores PGN string sent from Angular
    white_player = Column(String, nullable=True)
    black_player = Column(String, nullable=True)

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
    allow_credentials=True,
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
        if game_id in self.active_connections:
            for connection in self.active_connections[game_id]:
                await connection.send_json(message)

manager = ConnectionManager()


# ==========================================
# 4. MATCHMAKING & REST ENDPOINTS
# ==========================================
waiting_queue: List[Dict[str, str]] = []

class PlayerRequest(BaseModel):
    username: str

class GuestRequest(BaseModel):
    username: Optional[str] = None

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Chess Magic Backend is running!"}

# 🟢 NEW: Instant Guest Game Endpoint
@app.post("/api/game/start-guest")
def start_guest_game(req: GuestRequest, db: Session = Depends(get_db)):
    # Clean username or fallback to Guest_XXXX
    user_name = req.username.strip() if (req.username and req.username.strip()) else f"Guest_{random.randint(1000, 9999)}"
    
    new_game = ChessGame(
        fen=STARTING_FEN,
        pgn="",
        white_player=user_name,
        black_player="Guest Opponent"
    )
    db.add(new_game)
    db.commit()
    db.refresh(new_game)

    return {
        "status": "SUCCESS",
        "gameId": str(new_game.id),
        "color": "w",
        "whitePlayer": new_game.white_player,
        "blackPlayer": new_game.black_player,
        "fen": new_game.fen
    }

@app.post("/api/game/matchmake")
def matchmake(req: PlayerRequest, db: Session = Depends(get_db)):
    global waiting_queue

    if any(p["username"] == req.username for p in waiting_queue):
        return {"status": "WAITING", "message": "Already waiting in queue..."}

    if len(waiting_queue) > 0:
        player1 = waiting_queue.pop(0)  # White
        player2 = req.username          # Black

        new_game = ChessGame(
            fen=STARTING_FEN,
            pgn="",
            white_player=player1["username"],
            black_player=player2
        )
        db.add(new_game)
        db.commit()
        db.refresh(new_game)

        return {
            "status": "MATCHED",
            "gameId": str(new_game.id),
            "color": "b",
            "whitePlayer": player1["username"],
            "blackPlayer": player2
        }

    waiting_queue.append({"username": req.username})
    return {
        "status": "WAITING",
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
# 5. WEBSOCKET REAL-TIME GAMEPLAY (CLIENT-VALIDATED)
# ==========================================
@app.websocket("/ws/game/{game_id}/{color}/{username}")
async def websocket_endpoint(
    websocket: WebSocket,
    game_id: str,
    color: str,
    username: str,
    db: Session = Depends(get_db)
):
    await manager.connect(game_id, websocket)
    
    try:
        # 1. Send stored initial FEN & PGN to connecting player
        game = db.query(ChessGame).filter(ChessGame.id == int(game_id)).first()
        if game:
            await websocket.send_json({
                "type": "INIT",
                "fen": game.fen,
                "pgn": game.pgn or "",
                "whitePlayer": game.white_player,
                "blackPlayer": game.black_player
            })

        # 2. Receive and relay client-validated move payloads
        while True:
            data = await websocket.receive_json()

            if data.get("type") == "MOVE":
                incoming_fen = data.get("fen")
                incoming_pgn = data.get("pgn")
                move_played = data.get("move")

                game = db.query(ChessGame).filter(ChessGame.id == int(game_id)).first()
                if not game:
                    await websocket.send_json({"type": "ERROR", "message": "Game not found"})
                    continue

                # Store updated state directly to DB
                game.fen = incoming_fen
                game.pgn = incoming_pgn
                db.commit()

                # Broadcast updated FEN and PGN to all players connected to this game
                await manager.broadcast(game_id, {
                    "type": "MOVE",
                    "move": move_played,
                    "fen": incoming_fen,
                    "pgn": incoming_pgn,
                    "sender": username
                })

    except WebSocketDisconnect:
        manager.disconnect(game_id, websocket)
        await manager.broadcast(game_id, {
            "type": "SYSTEM",
            "message": f"Player {username} disconnected."
        })
