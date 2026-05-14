#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎡 LN Roulette Multiplayer Server v2.2
"""
import asyncio
import logging
import sys
import json
import time
import random
import secrets
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional, List

from aiohttp import web
from aiohttp.web import middleware
from aiohttp.web_request import Request

# Глобальная ссылка на БД
db = None

# LOGGING
log_handler = RotatingFileHandler('mp_server.log', maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger('mp_server')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)
logger.addHandler(logging.StreamHandler(sys.stdout))

# КОНФИГУРАЦИЯ
MAX_PLAYERS = 10
MIN_PLAYERS = 2
ROUND_TIMER = 30
TIMER_EXTENSION = 15
MAX_TIMER_EXTENSIONS = 3
COMMISSION = 0.10
MAX_BET_PERCENTAGE = 0.80
MAX_ROOMS_PER_USER = 3

PLAYER_COLORS = [
    "#FF6B6B", "#4ECDC4", "#FFEAA7", "#DDA0DD", "#45B7D1",
    "#96CEB4", "#FF8C00", "#F7DC6F", "#FF69B4", "#7B68EE",
    "#00CED1", "#FFD700"
]
TOP_BORDERS = ["#FFD700", "#C0C0C0", "#CD7F32"]
BOT_NAMES = ["LuckyBot", "RoulettePro", "CasinoKing", "FortuneAI", "SpinMaster"]


def set_db(sqlite_pool):
    global db
    db = sqlite_pool
    logger.info("✅ MP DB connected")


async def get_user(user_id: int) -> Optional[Dict]:
    if not db:
        return None
    try:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (int(user_id),)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"get_user: {e}")
        return None


async def update_balance(user_id: int, new_balance: float):
    if not db:
        return
    try:
        await db.execute("UPDATE users SET balance=?, updated_at=strftime('%s','now') WHERE user_id=?", (new_balance, user_id))
        await db.commit()
    except Exception as e:
        logger.error(f"update_balance: {e}")


class Player:
    def __init__(self, user_id: int, nickname: str):
        self.user_id = user_id
        self.nickname = nickname[:15]
        self.bet = 0.0
        self.color = ""
        self.border = ""

    @property
    def avatar_url(self) -> str:
        safe = (self.nickname or "??")[:2].upper()
        return f"https://ui-avatars.com/api/?name={safe}&background=555&color=fff&size=64&bold=true&format=svg"


class MPRoom:
    def __init__(self, room_id: str, room_name: str):
        self.room_id = room_id
        self.room_name = room_name[:30]
        self.players: Dict[int, Player] = {}
        self.total_bank = 0.0
        self.timer = ROUND_TIMER
        self.timer_extensions = 0
        self.status = "waiting"
        self.timer_task: Optional[asyncio.Task] = None
        self.winner_id: Optional[int] = None
        self.winner_angle: float = 0.0

    def get_players_list(self) -> List[Dict]:
        players = []
        sorted_players = sorted(
            [(uid, p) for uid, p in self.players.items() if p.bet > 0],
            key=lambda x: x[1].bet, reverse=True
        )
        for rank, (uid, player) in enumerate(sorted_players):
            pct = (player.bet / self.total_bank * 100) if self.total_bank > 0 else 0
            angle = (player.bet / self.total_bank * 360) if self.total_bank > 0 else 0
            border = TOP_BORDERS[rank] if rank < 3 else ""
            players.append({
                "user_id": uid, "nickname": player.nickname, "avatar": player.avatar_url,
                "color": player.color, "bet": player.bet,
                "percentage": round(pct, 2), "angle": round(angle, 2),
                "border": border, "rank": rank + 1,
            })
        return players

    def get_wheel_data(self) -> Dict:
        return {"sectors": self.get_players_list(), "bank": self.total_bank}

    def get_state(self, my_user_id: int = None) -> Dict:
        my_bet = self.players[my_user_id].bet if my_user_id and my_user_id in self.players else 0.0
        return {
            "type": "mp_state", "room_id": self.room_id, "room_name": self.room_name,
            "players": self.get_players_list(), "bank": self.total_bank,
            "timer": self.timer, "my_bet": my_bet,
            "round_active": self.status == "waiting", "wheel": self.get_wheel_data(),
        }


ws_connections: Dict[int, web.WebSocketResponse] = {}
rooms: Dict[str, MPRoom] = {}
user_rooms: Dict[int, str] = {}


async def add_bots(room_id: str):
    room = rooms.get(room_id)
    if not room or room.status != "waiting":
        return
    for i in range(5):
        bot_id = 900000 + i
        if bot_id in room.players:
            continue
        bet_amount = round(random.uniform(1, 50), 2)
        player = Player(bot_id, BOT_NAMES[i])
        player.color = PLAYER_COLORS[len(room.players) % len(PLAYER_COLORS)]
        player.bet = bet_amount
        room.players[bot_id] = player
        room.total_bank += bet_amount
    logger.info(f"🤖 5 bots added to {room_id}")


async def send_ws(ws: web.WebSocketResponse, data: Dict):
    try:
        if not ws.closed:
            await ws.send_json(data)
    except Exception:
        pass


async def broadcast(room_id: str, data: Dict, exclude: int = None):
    room = rooms.get(room_id)
    if not room:
        return
    tasks = [send_ws(ws_connections[uid], data) for uid in room.players if uid != exclude and uid < 900000 and uid in ws_connections]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def broadcast_state(room_id: str):
    room = rooms.get(room_id)
    if not room:
        return
    for uid in list(room.players.keys()):
        if uid < 900000 and uid in ws_connections:
            await send_ws(ws_connections[uid], room.get_state(uid))


async def start_timer(room_id: str):
    room = rooms.get(room_id)
    if not room:
        return
    room.status = "waiting"
    room.timer = ROUND_TIMER
    room.timer_extensions = 0
    await add_bots(room_id)
    await broadcast_state(room_id)

    while room.timer > 0 and room.status == "waiting":
        await asyncio.sleep(1)
        if room_id not in rooms:
            return
        room.timer -= 1
        await broadcast(room_id, {"type": "mp_timer", "time": room.timer})

    if room_id not in rooms:
        return
    active = [p for p in room.players.values() if p.bet > 0]
    if room.status == "waiting" and len(active) >= MIN_PLAYERS:
        await start_spin(room_id)
    else:
        await broadcast(room_id, {"type": "mp_round_cancelled", "reason": "Недостаточно игроков"})
        await reset_room(room_id)


async def start_spin(room_id: str):
    room = rooms.get(room_id)
    if not room:
        return
    room.status = "spinning"
    active = [(uid, p) for uid, p in room.players.items() if p.bet > 0]
    if not active:
        await reset_room(room_id)
        return

    total = sum(p.bet for _, p in active)
    weights = [p.bet / total for _, p in active]
    winner_id, winner_player = random.choices(active, weights=weights, k=1)[0]
    room.winner_id = winner_id

    cumulative = 0
    for uid, p in active:
        angle = (p.bet / total) * 360
        if uid == winner_id:
            room.winner_angle = cumulative + angle / 2 + random.uniform(-angle * 0.3, angle * 0.3)
            break
        cumulative += angle

    await broadcast(room_id, {
        "type": "mp_spin_start",
        "winner_angle": room.winner_angle,
        "total_rotation": 720 + random.randint(360, 1080),
    })

    await asyncio.sleep(5)

    if room_id not in rooms:
        return

    commission = room.total_bank * COMMISSION
    win_amount = room.total_bank - commission

    if winner_id < 900000:
        winner_user = await get_user(winner_id)
        if winner_user:
            await update_balance(winner_id, winner_user["balance"] + win_amount)
            logger.info(f"🏆 Winner {winner_id}: +{win_amount}$")

    await broadcast(room_id, {
        "type": "mp_result", "winner_id": winner_id,
        "winner_nickname": winner_player.nickname, "winner_color": winner_player.color,
        "winner_avatar": winner_player.avatar_url, "win_amount": win_amount,
        "bank": room.total_bank, "commission": commission, "players_count": len(active),
    })

    await asyncio.sleep(5)
    await reset_room(room_id)


async def reset_room(room_id: str):
    room = rooms.get(room_id)
    if not room:
        return
    for uid, player in list(room.players.items()):
        if uid >= 900000:
            del room.players[uid]
        else:
            if player.bet > 0:
                user = await get_user(uid)
                if user:
                    await update_balance(uid, user["balance"] + player.bet)
            player.bet = 0
            player.color = ""
            player.border = ""
    room.total_bank = 0
    room.winner_id = None
    room.winner_angle = 0
    room.status = "waiting"
    room.timer_task = asyncio.create_task(start_timer(room_id))
    await broadcast(room_id, {"type": "mp_new_round", "message": "Новый раунд!"})


async def handle_ws(request: Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    user_id = None
    current_room = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    action = data.get("action")

                    if action == "connect":
                        uid = int(data.get("user_id", 0))
                        if uid <= 0:
                            await ws.send_json({"type": "error", "message": "Invalid user"})
                            continue
                        user_id = uid
                        user = await get_user(user_id)
                        if not user:
                            await ws.send_json({"type": "error", "message": "User not found"})
                            continue
                        if user_id in ws_connections and not ws_connections[user_id].closed:
                            try:
                                await ws_connections[user_id].close(code=1000)
                            except Exception:
                                pass
                        ws_connections[user_id] = ws
                        await ws.send_json({"type": "connected", "user_id": user_id, "balance": user["balance"]})
                        logger.info(f"🔌 Connected: user={user_id}")

                    elif action == "mp_get_rooms":
                        rooms_list = [{
                            "room_id": rid, "name": r.room_name,
                            "players_count": len([p for p in r.players.values() if p.bet > 0]),
                            "bank": r.total_bank, "timer": r.timer,
                        } for rid, r in rooms.items() if r.status == "waiting"]
                        await ws.send_json({"type": "mp_rooms_list", "rooms": rooms_list})

                    elif action == "mp_create_room":
                        if not user_id:
                            continue
                        amount = float(data.get("amount", 0))
                        nickname = str(data.get("nickname", f"Player_{user_id}"))[:15]
                        room_name = str(data.get("room_name", f"Room #{user_id}"))[:30]

                        user_room_count = sum(1 for uid, rid in user_rooms.items() if uid == user_id and rid in rooms)
                        if user_room_count >= MAX_ROOMS_PER_USER:
                            await ws.send_json({"type": "error", "message": f"Максимум {MAX_ROOMS_PER_USER} комнат"})
                            continue
                        if amount < 1:
                            await ws.send_json({"type": "error", "message": "Минимум 1$"})
                            continue
                        user = await get_user(user_id)
                        if not user or user["balance"] < amount:
                            await ws.send_json({"type": "error", "message": "Недостаточно средств"})
                            continue

                        await update_balance(user_id, user["balance"] - amount)
                        room_id = secrets.token_hex(4).upper()
                        room = MPRoom(room_id, room_name)
                        player = Player(user_id, nickname)
                        player.color = PLAYER_COLORS[0]
                        player.bet = amount
                        room.players[user_id] = player
                        room.total_bank = amount
                        rooms[room_id] = room
                        user_rooms[user_id] = room_id
                        current_room = room_id
                        room.timer_task = asyncio.create_task(start_timer(room_id))
                        await ws.send_json(room.get_state(user_id))
                        logger.info(f"🎯 Room {room_id} created by {user_id}")

                    elif action == "mp_join_room":
                        if not user_id:
                            continue
                        room_id = data.get("room_id")
                        amount = float(data.get("amount", 0))
                        nickname = str(data.get("nickname", f"Player_{user_id}"))[:15]

                        if user_id in user_rooms and user_rooms[user_id] in rooms:
                            await ws.send_json({"type": "error", "message": "Вы уже в комнате"})
                            continue
                        room = rooms.get(room_id)
                        if not room:
                            await ws.send_json({"type": "error", "message": "Комната не найдена"})
                            continue
                        if room.status != "waiting":
                            await ws.send_json({"type": "error", "message": "Игра уже идёт"})
                            continue
                        if len([p for p in room.players.values() if p.bet > 0]) >= MAX_PLAYERS:
                            await ws.send_json({"type": "error", "message": "Комната заполнена"})
                            continue
                        if amount < 1:
                            await ws.send_json({"type": "error", "message": "Минимум 1$"})
                            continue
                        if room.total_bank > 0 and amount > room.total_bank * MAX_BET_PERCENTAGE:
                            await ws.send_json({"type": "error", "message": f"Максимум {(room.total_bank * MAX_BET_PERCENTAGE):.2f}$"})
                            continue
                        user = await get_user(user_id)
                        if not user or user["balance"] < amount:
                            await ws.send_json({"type": "error", "message": "Недостаточно средств"})
                            continue

                        await update_balance(user_id, user["balance"] - amount)
                        player = Player(user_id, nickname)
                        player.color = PLAYER_COLORS[len(room.players) % len(PLAYER_COLORS)]
                        player.bet = amount
                        room.players[user_id] = player
                        room.total_bank += amount
                        user_rooms[user_id] = room_id
                        current_room = room_id
                        await broadcast_state(room_id)
                        logger.info(f"👤 {user_id} joined room {room_id}")

                    elif action == "mp_raise_bet":
                        if not user_id:
                            continue
                        room_id = data.get("room_id") or user_rooms.get(user_id)
                        if not room_id or room_id not in rooms:
                            await ws.send_json({"type": "error", "message": "Вы не в комнате"})
                            continue
                        room = rooms[room_id]
                        if room.status != "waiting":
                            await ws.send_json({"type": "error", "message": "Игра идёт"})
                            continue
                        if user_id not in room.players:
                            await ws.send_json({"type": "error", "message": "Вы не в этой комнате"})
                            continue
                        extra = float(data.get("amount", 0))
                        if extra < 0.1:
                            await ws.send_json({"type": "error", "message": "Минимум 0.1$"})
                            continue
                        player = room.players[user_id]
                        new_bet = player.bet + extra
                        if new_bet > (room.total_bank + extra) * MAX_BET_PERCENTAGE:
                            await ws.send_json({"type": "error", "message": f"Максимум {((room.total_bank + extra) * MAX_BET_PERCENTAGE):.2f}$"})
                            continue
                        user = await get_user(user_id)
                        if not user or user["balance"] < extra:
                            await ws.send_json({"type": "error", "message": "Недостаточно средств"})
                            continue
                        await update_balance(user_id, user["balance"] - extra)
                        player.bet = new_bet
                        room.total_bank += extra
                        if room.timer < 5 and room.timer_extensions < MAX_TIMER_EXTENSIONS:
                            room.timer += TIMER_EXTENSION
                            room.timer_extensions += 1
                            await broadcast(room_id, {"type": "mp_timer_extended", "time": room.timer})
                        await broadcast_state(room_id)
                        logger.info(f"⬆️ {user_id} raised by {extra}$ in {room_id}")

                    elif action == "mp_leave_room":
                        if not user_id:
                            continue
                        room_id = data.get("room_id") or user_rooms.get(user_id)
                        room = rooms.get(room_id)
                        if room and user_id in room.players:
                            player = room.players[user_id]
                            if room.status == "waiting" and player.bet > 0:
                                user = await get_user(user_id)
                                if user:
                                    await update_balance(user_id, user["balance"] + player.bet)
                            room.total_bank -= player.bet
                            del room.players[user_id]
                            if user_id in user_rooms:
                                del user_rooms[user_id]
                            real = [uid for uid in room.players if uid < 900000]
                            if not real:
                                if room.timer_task:
                                    room.timer_task.cancel()
                                del rooms[room_id]
                            else:
                                await broadcast_state(room_id)
                        current_room = None
                        await ws.send_json({"type": "mp_left_room"})

                    elif action == "ping":
                        await ws.send_json({"type": "pong"})

                except json.JSONDecodeError:
                    await ws.send_json({"type": "error", "message": "Invalid JSON"})
                except Exception as e:
                    logger.error(f"WS error: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"WS connection error: {e}")

    finally:
        if user_id and user_id in ws_connections:
            del ws_connections[user_id]
        if current_room and user_id:
            room = rooms.get(current_room)
            if room and user_id in room.players and room.status == "waiting":
                player = room.players[user_id]
                if player.bet > 0:
                    user = await get_user(user_id)
                    if user:
                        await update_balance(user_id, user["balance"] + player.bet)
                room.total_bank -= player.bet
                del room.players[user_id]
                if user_id in user_rooms:
                    del user_rooms[user_id]
                real = [uid for uid in room.players if uid < 900000]
                if not real:
                    if room.timer_task:
                        room.timer_task.cancel()
                    if current_room in rooms:
                        del rooms[current_room]
                else:
                    await broadcast_state(current_room)
        logger.info(f"🔌 Disconnected: user={user_id}")

    return ws


def create_app() -> web.Application:
    app = web.Application()

    @middleware
    async def cors(request: Request, handler):
        resp = web.Response(status=204) if request.method == "OPTIONS" else await handler(request)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp

    app.middlewares.append(cors)
    app.router.add_get("/health", lambda r: web.json_response({"status": "ok", "server": "mp_server"}))
    app.router.add_get("/ws", handle_ws)

    return app