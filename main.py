import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import declarative_base, sessionmaker

# 1. Fetch Database URL from Render Environment Variables
DATABASE_URL = os.environ.get("DATABASE_URL")

# Automatically fix URL prefix for SQLAlchemy compatibility
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Fallback for local testing
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./test_chess.db"

# 2. Database Connection Setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 3. Database Model (Table Structure)
class Game(Base):
    __tablename__ = "chess_games"
    id = Column(Integer, primary_key=True, index=True)
    pgn = Column(Text, default="")         # Stores move history e.g. "1. e4 e5"
    fen = Column(String, default="start")  # Stores current board state

# AUTOMATIC TABLE CREATION (No manual SQL DDL required!)
Base.metadata.create_all(bind=engine)

app = FastAPI()

# 4. WebSocket Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active_games: dict[int, list[WebSocket]] = {}

    async def connect(self, game_id: int, websocket: WebSocket):
        await websocket.accept()
        if game_id not in self.active_games:
            self.active_games[game_id] = []
        self.active_games[game_id].append(websocket)

    def disconnect(self, game_id: int, websocket: WebSocket):
        if game_id in self.active_games:
            self.active_games[game_id].remove(websocket)

    async def broadcast(self, game_id: int, message: dict):
        if game_id in self.active_games:
            for connection in self.active_games[game_id]:
                await connection.send_json(message)

manager = ConnectionManager()

# 5. Status Endpoint
@app.get("/")
def home():
    return {"status": "Chess server online and connected to Aiven DB!"}

# 6. Real-time WebSocket Endpoint
@app.websocket("/ws/game/{game_id}")
async def game_websocket(websocket: WebSocket, game_id: int):
    await manager.connect(game_id, websocket)
    db = SessionLocal()

    try:
        # Load or create game record in Aiven DB
        game = db.query(Game).filter(Game.id == game_id).first()
        if not game:
            game = Game(id=game_id)
            db.add(game)
            db.commit()

        # Send latest saved FEN & PGN to sync UI on join/reconnect
        await websocket.send_json({
            "type": "SYNC",
            "pgn": game.pgn,
            "fen": game.fen
        })

        while True:
            # Receive move from frontend UI
            data = await websocket.receive_json()

            if data.get("type") == "MOVE":
                new_pgn = data["pgn"] # e.g., "1. e4 e5"
                new_fen = data["fen"]

                # Automatically update Aiven PostgreSQL
                game.pgn = new_pgn
                game.fen = new_fen
                db.commit()

                # Broadcast update to all connected screens
                await manager.broadcast(game_id, {
                    "type": "UPDATE",
                    "pgn": new_pgn,
                    "fen": new_fen
                })

    except WebSocketDisconnect:
        manager.disconnect(game_id, websocket)
    finally:
        db.close()
