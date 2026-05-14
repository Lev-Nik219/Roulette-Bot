#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎡 LN Roulette Multiplayer Server v2.0
Отдельный WebSocket-сервер для мультиплеера
Canvas wheel, live sectors, 100ms state sync
"""
import asyncio
import logging
import sys
import os
import json
import time
import random
import secrets
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict
import aiosqlite

from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import Response, json_response

# ═══════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════

log_handler = RotatingFileHandler(
    'mp_server.log',
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding='utf-8',
)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout), log_handler],
)
logger = logging.getLogger('mp_server')

# ═══════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════

HOST = "0.0.0.0"
PORT = int(os.getenv("MP_PORT", 10001))
SQLITE_DB_PATH = "database/mini_app.db"
API_URL = "https://roulette-bot-8i8t.onrender.com"

# Игровые настройки
MAX_PLAYERS = 10
MIN_PLAYERS = 2
ROUND_TIMER = 30
TIMER_EXTENSION = 15
MAX_TIMER_EXTENSIONS = 3
COMMISSION = 0.10
MAX_BET_PERCENTAGE = 0.80
MAX_ROOMS_PER_USER = 3

# Палитра цветов (12 цветов)
PLAYER_COLORS = [
    "#FF6B6B", "#4ECDC4", "#FFEAA7", "#DDA0DD", "#45B7D1",
    "#96CEB4", "#FF8C00", "#F7DC6F", "#FF69B4", "#7B68EE",
    "#00CED1", "#FFD700"
]

# ТОП-3 рамки
TOP_BORDERS = ["#FFD700", "#C0C0C0", "#CD7F32"]  # золото, серебро, бронза

# ═══════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════

db: Optional[aiosqlite.Connection] = None


async def init_db():
    global db
    os.makedirs("database", exist_ok=True)
    db = await aiosqlite.connect(SQLITE_DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=5000")
    logger.info("✅ MP DB ready")


async def get_user(user_id: int) -> Optional[Dict]:
    try:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"get_user: {e}")
        return None


async def update_balance(user_id: int, new_balance: float):
    try:
        await db.execute(
            "UPDATE users SET balance=?, updated_at=strftime('%s','now') WHERE user_id=?",
            (new_balance, user_id),
        )
        await db.commit()
    except Exception as e:
        logger.error(f"update_balance: {e}")


# ═══════════════════════════════════════
# ИГРОВОЕ СОСТОЯНИЕ
# ═══════════════════════════════════════


class Player:
    def __init__(self, user_id: int, nickname: str, avatar_url: str = None):
        self.user_id = user_id
        self.nickname = nickname[:15]
        self.avatar_url = avatar_url or self._generate_avatar(nickname)
        self.bet = 0.0
        self.color = ""
        self.border = ""

    def _generate_avatar(self, nickname: str) -> str:
        safe = (nickname or "??")[:2].upper()
        return f"https://ui-avatars.com/api/?name={safe}&background=555&color=fff&size=64&bold=true&format=svg"


class MPRoom:
    def __init__(self, room_id: str, room_name: str, creator_id: int):
        self.room_id = room_id
        self.room_name = room_name[:30]
        self.creator_id = creator_id
        self.players: Dict[int, Player] = {}
        self.total_bank = 0.0
        self.timer = ROUND_TIMER
        self.timer_extensions = 0
        self.status = "waiting"  # waiting, spinning, finished
        self.timer_task: Optional[asyncio.Task] = None
        self.spin_task: Optional[asyncio.Task] = None
        self.winner_id: Optional[int] = None
        self.winner_angle: float = 0.0
        self.last_activity = time.time()

    def get_players_list(self) -> List[Dict]:
        players = []
        sorted_players = sorted(self.players.items(), key=lambda x: x[1].bet, reverse=True)

        for rank, (uid, player) in enumerate(sorted_players):
            if player.bet == 0:
                continue
            pct = (player.bet / self.total_bank * 100) if self.total_bank > 0 else 0
            angle = (player.bet / self.total_bank * 360) if self.total_bank > 0 else 0

            # Присваиваем рамку ТОП-3
            border = ""
            if rank < 3 and player.bet > 0:
                border = TOP_BORDERS[rank]

            player.border = border

            players.append({
                "user_id": uid,
                "nickname": player.nickname,
                "avatar": player.avatar_url,
                "color": player.color,
                "bet": player.bet,
                "percentage": round(pct, 2),
                "angle": round(angle, 2),
                "border": border,
                "rank": rank + 1,
            })

        return players

    def get_wheel_data(self) -> Dict:
        return {
            "sectors": self.get_players_list(),
            "bank": self.total_bank,
            "status": self.status,
        }

    def get_state(self, my_user_id: int = None) -> Dict:
        my_bet = self.players[my_user_id].bet if my_user_id and my_user_id in self.players else 0.0
        return {
            "type": "mp_state",
            "room_id": self.room_id,
            "room_name": self.room_name,
            "players": self.get_players_list(),
            "bank": self.total_bank,
            "timer": self.timer,
            "my_bet": my_bet,
            "round_active": self.status == "waiting",
            "wheel": self.get_wheel_data(),
        }


# ═══════════════════════════════════════
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# ═══════════════════════════════════════

ws_connections: Dict[int, web.WebSocketResponse] = {}
rooms: Dict[str, MPRoom] = {}
user_rooms: Dict[int, str] = {}
rate_limits: Dict[str, List[float]] = defaultdict(list)

# Боты для отладки
BOT_NAMES = ["LuckyBot", "RoulettePro", "CasinoKing", "FortuneAI", "SpinMaster"]
BOT_EMOJIS = ["🤖", "🎰", "👑", "🍀", "🎯"]


async def add_bots_to_room(room_id: str):
    """Добавляем 5 ботов со случайными ставками"""
    room = rooms.get(room_id)
    if not room or room.status != "waiting":
        return

    for i in range(5):
        bot_id = 900000 + i
        bot_name = BOT_NAMES[i]
        bot_emoji = BOT_EMOJIS[i]

        # Проверяем, нет ли уже бота
        if bot_id in room.players:
            continue

        # Случайная ставка от 1 до 50
        bet_amount = round(random.uniform(1, 50), 2)

        player = Player(bot_id, bot_name)
        player.color = PLAYER_COLORS[len(room.players) % len(PLAYER_COLORS)]
        player.bet = bet_amount

        room.players[bot_id] = player
        room.total_bank += bet_amount

    logger.info(f"🤖 Added {5} bots to room {room_id}")


# ═══════════════════════════════════════
# ИГРОВАЯ ЛОГИКА
# ═══════════════════════════════════════


async def start_timer(room_id: str):
    """Таймер раунда"""
    room = rooms.get(room_id)
    if not room:
        return

    room.status = "waiting"
    room.timer = ROUND_TIMER
    room.timer_extensions = 0

    # Добавляем ботов
    await add_bots_to_room(room_id)

    # Рассылаем начальное состояние
    await broadcast_state(room_id)

    while room.timer > 0 and room.status == "waiting":
        await asyncio.sleep(1)
        if room_id not in rooms:
            return

        room.timer -= 1

        # Отправляем таймер каждую секунду
        await broadcast_to_room(room_id, {"type": "mp_timer", "time": room.timer})

    if room_id not in rooms:
        return

    if room.status == "waiting" and len([p for p in room.players.values() if p.bet > 0]) >= MIN_PLAYERS:
        await start_spin(room_id)
    else:
        await broadcast_to_room(room_id, {
            "type": "mp_round_cancelled",
            "reason": "Недостаточно игроков"
        })
        await reset_room(room_id)


async def start_spin(room_id: str):
    """Запуск вращения"""
    room = rooms.get(room_id)
    if not room:
        return

    room.status = "spinning"

    # Выбор победителя
    active_players = [(uid, p) for uid, p in room.players.items() if p.bet > 0]

    if not active_players:
        await reset_room(room_id)
        return

    total = sum(p.bet for _, p in active_players)
    weights = [p.bet / total for _, p in active_players]
    winner_id, winner_player = random.choices(active_players, weights=weights, k=1)[0]

    room.winner_id = winner_id

    # Расчёт угла победителя
    cumulative = 0
    for uid, p in active_players:
        angle = (p.bet / total) * 360
        if uid == winner_id:
            room.winner_angle = cumulative + angle / 2
            room.winner_angle += random.uniform(-angle * 0.3, angle * 0.3)
            break
        cumulative += angle

    total_rotation = 720 + random.randint(360, 1080)

    # Отправляем событие вращения
    await broadcast_to_room(room_id, {
        "type": "mp_spin_start",
        "winner_angle": room.winner_angle,
        "total_rotation": total_rotation,
    })

    # Ждём анимацию (5 секунд)
    await asyncio.sleep(5)

    if room_id not in rooms:
        return

    # Расчёт выплат
    commission = room.total_bank * COMMISSION
    win_amount = room.total_bank - commission

    # Начисляем выигрыш (если не бот)
    if winner_id < 900000:
        winner_user = await get_user(winner_id)
        if winner_user:
            await update_balance(winner_id, winner_user["balance"] + win_amount)
            logger.info(f"🏆 Winner {winner_id}: +{win_amount}$")

    # Результат
    await broadcast_to_room(room_id, {
        "type": "mp_result",
        "winner_id": winner_id,
        "winner_nickname": winner_player.nickname,
        "winner_color": winner_player.color,
        "winner_avatar": winner_player.avatar_url,
        "win_amount": win_amount,
        "bank": room.total_bank,
        "commission": commission,
        "players_count": len(active_players),
    })

    # Ждём показ результатов
    await asyncio.sleep(5)

    # Новый раунд
    await reset_room(room_id)


async def reset_room(room_id: str):
    """Сброс комнаты для нового раунда"""
    room = rooms.get(room_id)
    if not room:
        return

    # Возвращаем ставки реальным игрокам (не ботам)
    for uid, player in list(room.players.items()):
        if uid >= 900000:
            # Ботов удаляем
            del room.players[uid]
        else:
            # Игрокам возвращаем ставки
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

    # Запускаем новый раунд
    room.timer_task = asyncio.create_task(start_timer(room_id))

    await broadcast_to_room(room_id, {
        "type": "mp_new_round",
        "message": "Новый раунд начинается!"
    })


# ═══════════════════════════════════════
# РАССЫЛКА СОСТОЯНИЙ
# ═══════════════════════════════════════


async def send_ws(ws: web.WebSocketResponse, data: Dict):
    try:
        if not ws.closed:
            await ws.send_json(data)
    except Exception as e:
        logger.debug(f"send_ws error: {e}")


async def broadcast_to_room(room_id: str, data: Dict, exclude: int = None):
    room = rooms.get(room_id)
    if not room:
        return

    tasks = []
    for uid in room.players:
        if uid != exclude and uid in ws_connections and uid < 900000:
            tasks.append(send_ws(ws_connections[uid], data))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def broadcast_state(room_id: str):
    """Отправка полного состояния всем игрокам"""
    room = rooms.get(room_id)
    if not room:
        return

    for uid in room.players:
        if uid in ws_connections and uid < 900000:
            state = room.get_state(uid)
            await send_ws(ws_connections[uid], state)


# ═══════════════════════════════════════
# WEBSOCKET ОБРАБОТЧИК
# ═══════════════════════════════════════


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

                    # --- CONNECT ---
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

                        # Закрываем старое соединение
                        if user_id in ws_connections and not ws_connections[user_id].closed:
                            try:
                                await ws_connections[user_id].close(code=1000)
                            except:
                                pass

                        ws_connections[user_id] = ws
                        await ws.send_json({
                            "type": "connected",
                            "user_id": user_id,
                            "balance": user["balance"],
                        })
                        logger.info(f"🔌 Connected: user={user_id}")

                    # --- GET ROOMS ---
                    elif action == "mp_get_rooms":
                        rooms_list = []
                        for rid, r in rooms.items():
                            if r.status in ("waiting", "spinning"):
                                rooms_list.append({
                                    "room_id": rid,
                                    "name": r.room_name,
                                    "players_count": len([p for p in r.players.values() if p.bet > 0]),
                                    "bank": r.total_bank,
                                    "timer": r.timer,
                                    "status": r.status,
                                })

                        await ws.send_json({"type": "mp_rooms_list", "rooms": rooms_list})

                    # --- CREATE ROOM ---
                    elif action == "mp_create_room":
                        if not user_id:
                            continue

                        amount = float(data.get("amount", 0))
                        nickname = str(data.get("nickname", f"Player_{user_id}"))[:15]
                        room_name = str(data.get("room_name", f"Room #{user_id}"))[:30]

                        # Проверка лимита комнат
                        user_room_count = sum(1 for uid, rid in user_rooms.items()
                                              if uid == user_id and rid in rooms)
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
                        room = MPRoom(room_id, room_name, user_id)

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

                    # --- JOIN ROOM ---
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
                        if room.status not in ("waiting",):
                            await ws.send_json({"type": "error", "message": "Игра уже идёт"})
                            continue
                        if len([p for p in room.players.values() if p.bet > 0]) >= MAX_PLAYERS:
                            await ws.send_json({"type": "error", "message": "Комната заполнена"})
                            continue
                        if amount < 1:
                            await ws.send_json({"type": "error", "message": "Минимум 1$"})
                            continue

                        # Проверка максимальной ставки
                        if room.total_bank > 0 and amount > room.total_bank * MAX_BET_PERCENTAGE:
                            await ws.send_json({
                                "type": "error",
                                "message": f"Максимальная ставка: {(room.total_bank * MAX_BET_PERCENTAGE):.2f}$"
                            })
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

                    # --- RAISE BET ---
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

                        # Проверка максимальной ставки
                        if new_bet > (room.total_bank + extra) * MAX_BET_PERCENTAGE:
                            await ws.send_json({
                                "type": "error",
                                "message": f"Максимальная ставка: {((room.total_bank + extra) * MAX_BET_PERCENTAGE):.2f}$"
                            })
                            continue

                        user = await get_user(user_id)
                        if not user or user["balance"] < extra:
                            await ws.send_json({"type": "error", "message": "Недостаточно средств"})
                            continue

                        await update_balance(user_id, user["balance"] - extra)

                        player.bet = new_bet
                        room.total_bank += extra

                        # Продление таймера
                        if room.timer < 5 and room.timer_extensions < MAX_TIMER_EXTENSIONS:
                            room.timer += TIMER_EXTENSION
                            room.timer_extensions += 1
                            await broadcast_to_room(room_id, {
                                "type": "mp_timer_extended",
                                "time": room.timer,
                                "extensions": room.timer_extensions,
                            })

                        await broadcast_state(room_id)
                        logger.info(f"⬆️ {user_id} raised bet by {extra}$ in {room_id}")

                    # --- LEAVE ROOM ---
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

                            if not [p for p in room.players.values() if p.user_id < 900000]:
                                # Только боты остались — удаляем комнату
                                if room.timer_task:
                                    room.timer_task.cancel()
                                del rooms[room_id]
                            else:
                                await broadcast_state(room_id)

                        current_room = None
                        await ws.send_json({"type": "mp_left_room"})

                    # --- PING ---
                    elif action == "ping":
                        await ws.send_json({"type": "pong"})

                except json.JSONDecodeError:
                    await ws.send_json({"type": "error", "message": "Invalid JSON"})
                except Exception as e:
                    logger.error(f"WS error: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"WS connection error: {e}")

    finally:
        # Очистка при отключении
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

                if not [p for p in room.players.values() if p.user_id < 900000]:
                    if room.timer_task:
                        room.timer_task.cancel()
                    if current_room in rooms:
                        del rooms[current_room]
                else:
                    await broadcast_state(current_room)

        logger.info(f"🔌 Disconnected: user={user_id}")

    return ws


# ═══════════════════════════════════════
# ОЧИСТКА СТАРЫХ КОМНАТ
# ═══════════════════════════════════════


async def cleanup_inactive_rooms():
    """Периодическая очистка неактивных комнат"""
    while True:
        await asyncio.sleep(300)  # Каждые 5 минут
        now = time.time()
        inactive = []

        for rid, room in rooms.items():
            if now - room.last_activity > 600:  # 10 минут неактивности
                inactive.append(rid)

        for rid in inactive:
            room = rooms.get(rid)
            if room:
                for uid, player in list(room.players.items()):
                    if uid < 900000 and player.bet > 0:
                        user = await get_user(uid)
                        if user:
                            await update_balance(uid, user["balance"] + player.bet)
                if room.timer_task:
                    room.timer_task.cancel()
                del rooms[rid]
                logger.info(f"🧹 Cleaned room {rid}")


# ═══════════════════════════════════════
# ЗАПУСК СЕРВЕРА
# ═══════════════════════════════════════


def create_app() -> web.Application:
    app = web.Application()

    # CORS
    from aiohttp.web import middleware

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


if __name__ == "__main__":
    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())
    loop.create_task(cleanup_inactive_rooms())

    app = create_app()
    logger.info(f"🚀 MP Server starting on port {PORT}")
    web.run_app(app, host=HOST, port=PORT)