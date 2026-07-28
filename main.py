import os
import json
import time
import random
import datetime
from typing import List, Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

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

    chat_messages = relationship("ChatMessage", back_populates="game", cascade="all, delete-orphan")


class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    game_id = Column(Integer, ForeignKey("chess_games.id", ondelete="CASCADE"), index=True, nullable=False)
    sender = Column(String, nullable=False)
    text = Column(Text, nullable=False)
    is_system = Column(Boolean, default=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

    game = relationship("ChessGame", back_populates="chat_messages")


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
        key = str(game_id)
        if key not in self.active_connections:
            self.active_connections[key] = []
        self.active_connections[key].append(websocket)
        print(f"🟢 [WS ROOM JOINED] Room: {key} | Connections in room: {len(self.active_connections[key])}", flush=True)

    def disconnect(self, game_id: str, websocket: WebSocket):
        key = str(game_id)
        if key in self.active_connections:
            if websocket in self.active_connections[key]:
                self.active_connections[key].remove(websocket)
            if not self.active_connections[key]:
                del self.active_connections[key]

    async def broadcast(self, game_id: str, message: dict):
        key = str(game_id)
        print(f"==================================================", flush=True)
        print(f"📢 [WS BROADCAST] Game Room: {key}", flush=True)
        print(f"Data: {json.dumps(message, indent=2)}", flush=True)
        print(f"==================================================", flush=True)

        if key in self.active_connections:
            # Iterate copy of list to prevent modification exceptions during loop
            for connection in list(self.active_connections[key]):
                try:
                    await connection.send_json(message)
                except Exception as e:
                    print(f"⚠️ Failed sending to socket: {e}", flush=True)

manager = ConnectionManager()


# ==========================================
# 4. MATCHMAKING & REST ENDPOINTS
# ==========================================
waiting_queue: List[Dict] = []

class MatchRequest(BaseModel):
    username: Optional[str] = None
    minutes: Optional[int] = 5
    increment: Optional[int] = 0

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Chess Magic Backend is running!"}

def create_game_from_queue(p1: Dict, p2_username: str, db: Session) -> ChessGame:
    minutes = p1.get("minutes", 5)
    increment = float(p1.get("increment", 0))
    initial_seconds = float(minutes * 60)

    new_game = ChessGame(
        fen=STARTING_FEN,
        pgn="",
        white_player=p1["username"],
        black_player=p2_username,
        white_time=initial_seconds,
        black_time=initial_seconds,
        increment=increment,
        last_move_time=None
    )
    db.add(new_game)
    db.commit()
    db.refresh(new_game)
    return new_game

@app.post("/api/game/matchmake")
def matchmake(req: MatchRequest, db: Session = Depends(get_db)):
    global waiting_queue

    username = req.username.strip() if (req.username and req.username.strip()) else f"Guest_{random.randint(1000, 9999)}"

    if any(p["username"] == username for p in waiting_queue):
        return {"status": "WAITING", "username": username, "message": "Already waiting in queue..."}

    if len(waiting_queue) > 0:
        player1 = waiting_queue.pop(0)
        player2 = username

        new_game = create_game_from_queue(player1, player2, db)

        return {
            "status": "MATCHED",
            "gameId": str(new_game.id),
            "color": "b",
            "username": player2,
            "whitePlayer": player1["username"],
            "blackPlayer": player2,
            "fen": new_game.fen,
            "whiteTime": new_game.white_time,
            "blackTime": new_game.black_time,
            "increment": new_game.increment
        }

    waiting_queue.append({
        "username": username,
        "minutes": req.minutes or 5,
        "increment": req.increment or 0
    })
    return {
        "status": "WAITING",
        "username": username,
        "message": "Looking for opponent..."
    }

@app.post("/api/game/start-guest")
def start_guest_game(req: MatchRequest, db: Session = Depends(get_db)):
    global waiting_queue

    username = req.username.strip() if (req.username and req.username.strip()) else f"Guest_{random.randint(1000, 9999)}"

    if any(p["username"] == username for p in waiting_queue):
        return {
            "status": "WAITING",
            "username": username,
            "message": "Already waiting in queue..."
        }

    if len(waiting_queue) > 0:
        player1 = waiting_queue.pop(0)
        player2 = username

        new_game = create_game_from_queue(player1, player2, db)

        return {
            "status": "MATCHED",
            "gameId": str(new_game.id),
            "color": "b",
            "username": player2,
            "whitePlayer": player1["username"],
            "blackPlayer": player2,
            "fen": new_game.fen,
            "whiteTime": new_game.white_time,
            "blackTime": new_game.black_time,
            "increment": new_game.increment
        }

    waiting_queue.append({
        "username": username,
        "minutes": req.minutes or 5,
        "increment": req.increment or 0
    })
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
# 5. WEBSOCKET REAL-TIME GAMEPLAY & CHAT
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

    # Push initial state & recent chat history on connection
    db_init = SessionLocal()
    try:
        game = db_init.query(ChessGame).filter(ChessGame.id == int(game_id)).first()
        if game:
            chats = db_init.query(ChatMessage).filter(ChatMessage.game_id == int(game_id)).order_by(ChatMessage.timestamp.asc()).all()
            chat_history = [
                {
                    "sender": c.sender,
                    "text": c.text,
                    "isSystem": c.is_system,
                    "timestamp": c.timestamp.strftime("%H:%M")
                }
                for c in chats
            ]

            init_payload = {
                "type": "INIT",
                "fen": game.fen,
                "pgn": game.pgn or "",
                "whitePlayer": game.white_player,
                "blackPlayer": game.black_player,
                "whiteTime": game.white_time,
                "blackTime": game.black_time,
                "increment": game.increment,
                "lastMoveTime": game.last_move_time,
                "chatHistory": chat_history
            }
            print(f"📤 [WS INIT SENT] To {username}:", json.dumps(init_payload, indent=2), flush=True)
            await websocket.send_json(init_payload)
    finally:
        db_init.close()

    try:
        while True:
            data = await websocket.receive_json()

            print(f"==================================================", flush=True)
            print(f"📥 [WS RECEIVED] Game: {game_id} | From: {username}", flush=True)
            print(f"Payload: {json.dumps(data, indent=2)}", flush=True)
            print(f"==================================================", flush=True)

            msg_type = data.get("type")
            now = time.time()

            # --- EVENT 1: MOVE EXECUTION ---
            if msg_type == "MOVE":
                incoming_fen = data.get("fen")
                incoming_pgn = data.get("pgn")
                move_played = data.get("move")

                db = SessionLocal()
                try:
                    game = db.query(ChessGame).filter(ChessGame.id == int(game_id)).first()
                    if game:
                        if game.last_move_time is not None:
                            elapsed = now - game.last_move_time
                            if incoming_fen and " b " in incoming_fen:
                                game.white_time = max(0.0, game.white_time - elapsed + game.increment)
                            else:
                                game.black_time = max(0.0, game.black_time - elapsed + game.increment)

                        game.last_move_time = now
                        if incoming_fen:
                            game.fen = incoming_fen
                        if incoming_pgn and incoming_pgn.strip():
                            game.pgn = incoming_pgn

                        db.commit()

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
                    print(f"❌ [DB ERROR] Move save failed: {e}", flush=True)
                finally:
                    db.close()

            # --- EVENT 2: LIVE CHAT MESSAGE ---
            elif msg_type == "CHAT":
                text = data.get("text", "").strip()
                is_system = data.get("isSystem", False)

                if text:
                    sender_name = "System" if is_system else username
                    formatted_time = datetime.datetime.now().strftime("%H:%M")
                    
                    db = SessionLocal()
                    try:
                        chat_entry = ChatMessage(
                            game_id=int(game_id),
                            sender=sender_name,
                            text=text,
                            is_system=is_system,
                            timestamp=datetime.datetime.utcnow()
                        )
                        db.add(chat_entry)
                        db.commit()
                    except Exception as e:
                        db.rollback()
                        print(f"❌ [DB ERROR] Chat save failed: {e}", flush=True)
                    finally:
                        db.close()

                    # Always broadcast to active sockets in room
                    await manager.broadcast(game_id, {
                        "type": "CHAT",
                        "sender": sender_name,
                        "text": text,
                        "isSystem": is_system,
                        "timestamp": formatted_time
                    })

            # --- EVENT 3: TIME CONTROL ADJUSTMENT ---
            elif msg_type == "SET_TIME_CONTROL":
                minutes = data.get("minutes", 5)
                increment = data.get("increment", 0)
                initial_seconds = float(minutes * 60)

                db = SessionLocal()
                try:
                    game = db.query(ChessGame).filter(ChessGame.id == int(game_id)).first()
                    if game:
                        game.white_time = initial_seconds
                        game.black_time = initial_seconds
                        game.increment = float(increment)
                        db.commit()

                        await manager.broadcast(game_id, {
                            "type": "SET_TIME_CONTROL",
                            "minutes": minutes,
                            "increment": increment,
                            "whiteTime": game.white_time,
                            "blackTime": game.black_time
                        })
                except Exception as e:
                    db.rollback()
                    print(f"❌ [DB ERROR] Time Control save failed: {e}", flush=True)
                finally:
                    db.close()

    except WebSocketDisconnect:
        print(f"🔴 [WS DISCONNECTED] Game: {game_id} | User: {username}", flush=True)
    except Exception as e:
        print(f"⚠️ [WS ERROR] Unexpected error: {e}", flush=True)
    finally:
        manager.disconnect(game_id, websocket)
        await manager.broadcast(game_id, {
            "type": "SYSTEM",
            "message": f"Player {username} disconnected."
        })
