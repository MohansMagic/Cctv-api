import json
import os
from typing import Dict, List
import chess

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# ----------------------------------------------------
# Database Setup (Aiven PostgreSQL / SQLite fallback)
# ----------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class ChessGame(Base):
    __tablename__ = "chess_games"

    id = Column(String, primary_key=True, index=True)
    pgn = Column(String, default="")
    fen = Column(String, default="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
    white_player = Column(String, nullable=True)
    black_player = Column(String, nullable=True)


# Auto-create tables if they don't exist
Base.metadata.create_all(bind=engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ----------------------------------------------------
# WebSocket Connection Manager
# ----------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, game_id: str):
        await websocket.accept()
        if game_id not in self.active_connections:
            self.active_connections[game_id] = []
        self.active_connections[game_id].append(websocket)

    def disconnect(self, websocket: WebSocket, game_id: str):
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


# ----------------------------------------------------
# WebSocket Endpoint
# ----------------------------------------------------
@app.websocket("/ws/game/{game_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    game_id: str,
    color: str = Query(None),
    username: str = Query(None),
    db: Session = Depends(get_db)
):
    await manager.connect(websocket, game_id)
    try:
        # Fetch existing game or create a new row on initial connection
        game = db.query(ChessGame).filter(ChessGame.id == game_id).first()
        if not game:
            game = ChessGame(id=game_id)
            db.add(game)

        # Assign player username based on chosen color
        if color == 'w' and username:
            game.white_player = username
        elif color == 'b' and username:
            game.black_player = username

        db.commit()
        db.refresh(game)

        # Broadcast initial state & player profiles to room
        await manager.broadcast(game_id, {
            "type": "PLAYER_JOINED",
            "fen": game.fen,
            "pgn": game.pgn,
            "white_player": game.white_player or "Waiting...",
            "black_player": game.black_player or "Waiting..."
        })

        # Keep listening for move messages
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            if message.get("type") == "MOVE":
                move_uci = message.get("move")
                
                # --- FIX: ALWAYS RE-FETCH LATEST GAME STATE FROM DB ---
                game = db.query(ChessGame).filter(ChessGame.id == game_id).first()
                
                # Apply move server-side using python-chess
                board = chess.Board(game.fen if (game and game.fen) else chess.STARTING_FEN)
                try:
                    move = chess.Move.from_uci(move_uci)
                    if move in board.legal_moves:
                        board.push(move)
                        game.fen = board.fen()
                        game.pgn = message.get("pgn", game.pgn)
                        db.commit()

                        # Broadcast updated board state to all connected clients
                        await manager.broadcast(game_id, {
                            "type": "UPDATE",
                            "fen": game.fen,
                            "pgn": game.pgn,
                            "lastMove": move_uci,
                            "white_player": game.white_player or "Waiting...",
                            "black_player": game.black_player or "Waiting..."
                        })
                except Exception as e:
                    print(f"Invalid move received: {e}")

    except WebSocketDisconnect:
        manager.disconnect(websocket, game_id)
    except Exception as e:
        print(f"Error handling socket: {e}")
        manager.disconnect(websocket, game_id)
