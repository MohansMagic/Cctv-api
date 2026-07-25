import os
import json
from typing import List, Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import chess

# ==========================================
# 1. DATABASE SETUP
# ==========================================
# Uses DATABASE_URL env var on Render (PostgreSQL), falls back to local SQLite
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./chess.db")

# Fix Render PostgreSQL URL compatibility if needed
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Connect to DB (SQLite requires check_same_thread=False)
connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database Model
class ChessGame(Base):
    __tablename__ = "chess_games"

    id = Column(Integer, primary_key=True, index=True)
    fen = Column(Text, default=chess.STARTING_FEN)
    white_player = Column(String, nullable=True)
    black_player = Column(String, nullable=True)

# Create tables automatically on startup
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

# Allow connections from Angular frontend, tester.html, or local browser
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
        # Maps game_id -> List of active WebSockets
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
# In-memory queue for simple matchmaking
waiting_queue: List[Dict[str, str]] = []

class PlayerRequest(BaseModel):
    username: str

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Chess Magic Backend is running!"}

@app.post("/api/game/matchmake")
def matchmake(req: PlayerRequest, db: Session = Depends(get_db)):
    global waiting_queue

    # 1. Prevent duplicate queue entries for the same user
    if any(p["username"] == req.username for p in waiting_queue):
        return {"status": "WAITING", "message": "Already waiting in queue..."}

    # 2. If another player is waiting, pair them up!
    if len(waiting_queue) > 0:
        player1 = waiting_queue.pop(0)  # Host (White)
        player2 = req.username          # Joiner (Black)

        # Create new game record in DB
        new_game = ChessGame(
            fen=chess.STARTING_FEN,
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

    # 3. If queue is empty, add this player to queue
    waiting_queue.append({"username": req.username})
    return {
        "status": "WAITING",
        "message": "Looking for opponent..."
    }

@app.get("/api/game/status/{username}")
def check_status(username: str, db: Session = Depends(get_db)):
    """
    Polling endpoint used by Player 1 while waiting in queue.
    Checks if a match was created for them by Player 2.
    """
    # If still sitting in waiting queue, stay in WAITING state
    if any(p["username"] == username for p in waiting_queue):
        return {"status": "WAITING"}

    # Find the latest game created involving this player
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
    username: str,
    db: Session = Depends(get_db)
):
    await manager.connect(game_id, websocket)
    
    try:
        # Send current FEN and board status upon initial connection
        game = db.query(ChessGame).filter(ChessGame.id == int(game_id)).first()
        if game:
            await websocket.send_json({
                "type": "INIT",
                "fen": game.fen,
                "whitePlayer": game.white_player,
                "blackPlayer": game.black_player
            })

        while True:
            data = await websocket.receive_json()

            # 1. Always re-fetch state from DB to prevent race conditions
            game = db.query(ChessGame).filter(ChessGame.id == int(game_id)).first()
            if not game:
                await websocket.send_json({"type": "ERROR", "message": "Game not found"})
                continue

            board = chess.Board(game.fen)
            current_turn_color = "w" if board.turn == chess.WHITE else "b"

            # 2. Security Check: Enforce player turn
            if color != current_turn_color:
                await websocket.send_json({
                    "type": "ERROR",
                    "message": f"It is not your turn! Current turn: {current_turn_color.upper()}"
                })
                continue

            # 3. Extract and validate move (expects UCI format like 'e2e4')
            move_uci = data.get("move")
            if not move_uci:
                await websocket.send_json({"type": "ERROR", "message": "Missing 'move' field in JSON payload"})
                continue

            try:
                move = chess.Move.from_uci(move_uci)
                if move in board.legal_moves:
                    board.push(move)
                    new_fen = board.fen()

                    # 4. Save updated board state to DB
                    game.fen = new_fen
                    db.commit()

                    # 5. Broadcast new move to both players
                    await manager.broadcast(game_id, {
                        "type": "MOVE",
                        "move": move_uci,
                        "fen": new_fen,
                        "sender": username,
                        "isCheckmate": board.is_checkmate(),
                        "isDraw": board.is_game_over() and not board.is_checkmate()
                    })
                else:
                    await websocket.send_json({"type": "ERROR", "message": f"Illegal move: {move_uci}"})

            except Exception as e:
                await websocket.send_json({"type": "ERROR", "message": f"Invalid move syntax: {str(e)}"})

    except WebSocketDisconnect:
        manager.disconnect(game_id, websocket)
        await manager.broadcast(game_id, {
            "type": "SYSTEM",
            "message": f"Player {username} disconnected."
        })
