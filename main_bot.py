#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎡 LN Roulette Bot - Main Application
Mini App с европейской рулеткой и мультиплеерным режимом
"""
import asyncio
import logging
import sys
import os
import json
import time
import random
import hashlib
import hmac
import secrets
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import sqlite3
import aiosqlite
from contextlib import asynccontextmanager

# Aiogram imports
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    WebAppInfo, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InputFile, FSInputFile
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# Aiohttp imports
from aiohttp import web
from aiohttp.web import middleware
from aiohttp.web_request import Request
from aiohttp.web_response import Response, json_response

# PostgreSQL
import asyncpg

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════

class Config:
    # Bot
    BOT_TOKEN = "8756148710:AAHAF6f4fa9v9IruYkXq0_rshv-d65h7Yqg"
    BOT_USERNAME = "@lnRoulette_bot"

    # CryptoPay
    CRYPTO_PAY_TOKEN = "581586:AAmQppk9XEGf4EKxd8fpj0fReHZsIRietdW"

    # Admins
    ADMIN_IDS = [1167503795, 1670366784]

    # Database
    SQLITE_DB_PATH = "database/mini_app.db"
    POSTGRES_DSN = os.getenv(
        "DATABASE_URL",
        "postgresql://user:password@localhost:5432/roulette_db"
    )

    # API
    API_URL = "https://roulette-bot-8i8t.onrender.com"
    FRONTEND_URL = "https://roulette-bot-six.vercel.app"

    # Webhook
    WEBHOOK_HOST = "0.0.0.0"
    WEBHOOK_PORT = int(os.getenv("PORT", 8000))
    WEBHOOK_PATH = "/webhook"
    WEBHOOK_URL = f"{API_URL}{WEBHOOK_PATH}"

    # Game settings
    MAX_PLAYERS_MULTIPLAYER = 6
    MIN_PLAYERS_MULTIPLAYER = 2
    MULTIPLAYER_JOIN_TIMEOUT = 30  # seconds
    PLATFORM_COMMISSION = 0.10  # 10%
    MAX_WIN_PERCENTAGE = 0.47  # 47%
    FREE_SPIN_EVERY = 10  # games
    MIN_GAMES_FOR_WITHDRAWAL = 2

    # Roulette
    ROULETTE_NUMBERS = list(range(37))  # 0-36
    RED_NUMBERS = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}
    BLACK_NUMBERS = {2, 4, 6, 8, 10, 11, 13, 15, 17, 20, 22, 24, 26, 28, 29, 31, 33, 35}
    GREEN_NUMBERS = {0}


config = Config()

# ═══════════════════════════════════════
# DATABASE POOLS & CONNECTIONS
# ═══════════════════════════════════════

# SQLite connection pool (for Mini App API)
sqlite_pool: Optional[aiosqlite.Connection] = None

# PostgreSQL pool (for bot)
pg_pool: Optional[asyncpg.Pool] = None

# SQLite retry decorator
def execute_sqlite_with_retry(max_retries=5, delay=0.1):
    """Декоратор для повторных попыток операций с SQLite"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (sqlite3.OperationalError, aiosqlite.OperationalError) as e:
                    last_error = e
                    if "database is locked" in str(e).lower():
                        logger.warning(
                            f"SQLite locked (attempt {attempt + 1}/{max_retries}): {e}"
                        )
                        await asyncio.sleep(delay * (attempt + 1))
                    else:
                        raise
            raise last_error
        return wrapper
    return decorator


async def init_sqlite():
    """Initialize SQLite database for Mini App API"""
    global sqlite_pool
    os.makedirs("database", exist_ok=True)

    sqlite_pool = await aiosqlite.connect(config.SQLITE_DB_PATH)
    sqlite_pool.row_factory = aiosqlite.Row
    await sqlite_pool.execute("PRAGMA journal_mode=WAL")
    await sqlite_pool.execute("PRAGMA busy_timeout=5000")
    await sqlite_pool.execute("PRAGMA foreign_keys=ON")

    # Create tables
    await sqlite_pool.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            nickname TEXT DEFAULT '',
            balance REAL DEFAULT 0.0,
            total_games INTEGER DEFAULT 0,
            total_wins INTEGER DEFAULT 0,
            total_bet REAL DEFAULT 0.0,
            total_win_amount REAL DEFAULT 0.0,
            free_spins INTEGER DEFAULT 0,
            games_since_withdrawal INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS game_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            game_type TEXT NOT NULL,
            bet_type TEXT,
            bet_amount REAL DEFAULT 0,
            win_amount REAL DEFAULT 0,
            result TEXT,
            number INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS multiplayer_rooms (
            room_id TEXT PRIMARY KEY,
            status TEXT DEFAULT 'waiting',
            bank REAL DEFAULT 0.0,
            winner_id INTEGER,
            commission REAL DEFAULT 0.0,
            players_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS multiplayer_players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            bet_amount REAL DEFAULT 0,
            color TEXT,
            won INTEGER DEFAULT 0,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (room_id) REFERENCES multiplayer_rooms(room_id)
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            amount REAL DEFAULT 0,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS ads_watched (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            reward_type TEXT,
            reward_amount REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_users_id ON users(user_id);
        CREATE INDEX IF NOT EXISTS idx_game_history_user ON game_history(user_id);
        CREATE INDEX IF NOT EXISTS idx_rooms_status ON multiplayer_rooms(status);
        CREATE INDEX IF NOT EXISTS idx_mp_players_room ON multiplayer_players(room_id);
    """)

    await sqlite_pool.commit()
    logger.info("✅ SQLite initialized successfully")


async def init_postgres():
    """Initialize PostgreSQL pool for bot"""
    global pg_pool
    try:
        pg_pool = await asyncpg.create_pool(
            dsn=config.POSTGRES_DSN,
            min_size=2,
            max_size=10,
            command_timeout=60
        )

        async with pg_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    nickname VARCHAR(30) DEFAULT '',
                    balance DECIMAL(20,8) DEFAULT 0.0,
                    total_games INTEGER DEFAULT 0,
                    total_wins INTEGER DEFAULT 0,
                    total_bet DECIMAL(20,8) DEFAULT 0.0,
                    total_win_amount DECIMAL(20,8) DEFAULT 0.0,
                    free_spins INTEGER DEFAULT 0,
                    games_since_withdrawal INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS support_messages (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    admin_id BIGINT,
                    message TEXT NOT NULL,
                    is_admin_reply BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_pg_users_id ON users(user_id);
                CREATE INDEX IF NOT EXISTS idx_support_user ON support_messages(user_id);
            """)

        logger.info("✅ PostgreSQL initialized successfully")
    except Exception as e:
        logger.warning(f"⚠️ PostgreSQL connection failed: {e}")
        logger.info("Bot will use SQLite as fallback")


async def close_databases():
    """Close all database connections"""
    global sqlite_pool, pg_pool
    if sqlite_pool:
        await sqlite_pool.close()
    if pg_pool:
        await pg_pool.close()
    logger.info("Databases closed")


# ═══════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════

async def get_user(user_id: int) -> Optional[Dict]:
    """Get user from SQLite"""
    try:
        async with sqlite_pool.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}")
    return None


async def create_user_if_not_exists(user_id: int, username: str = None) -> Dict:
    """Create user if not exists in both databases"""
    user = await get_user(user_id)
    if not user:
        now = datetime.now().isoformat()
        await sqlite_pool.execute(
            """INSERT INTO users (user_id, username, nickname, balance, created_at, updated_at)
               VALUES (?, ?, ?, 0.0, ?, ?)""",
            (user_id, username or '', '', now, now)
        )
        await sqlite_pool.commit()

        # Also insert into PostgreSQL
        if pg_pool:
            try:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        """INSERT INTO users (user_id, username, nickname, balance)
                           VALUES ($1, $2, '', 0.0)
                           ON CONFLICT (user_id) DO NOTHING""",
                        user_id, username or ''
                    )
            except Exception as e:
                logger.error(f"PostgreSQL insert error: {e}")

        user = await get_user(user_id)
    return user


@execute_sqlite_with_retry()
async def update_balance_both(user_id: int, new_balance: float):
    """Update balance in both databases"""
    now = datetime.now().isoformat()
    await sqlite_pool.execute(
        "UPDATE users SET balance = ?, updated_at = ? WHERE user_id = ?",
        (new_balance, now, user_id)
    )
    await sqlite_pool.commit()

    if pg_pool:
        try:
            async with pg_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE users SET balance = $1, updated_at = NOW() WHERE user_id = $2",
                    new_balance, user_id
                )
        except Exception as e:
            logger.error(f"PostgreSQL update error: {e}")


def get_number_color(number: int) -> str:
    """Get color of roulette number"""
    if number in config.RED_NUMBERS:
        return "red"
    elif number in config.BLACK_NUMBERS:
        return "black"
    else:
        return "green"


def generate_roulette_result(user_bet_type: str, force_win: bool = False) -> Tuple[int, bool]:
    """
    Generate roulette result with house edge
    Returns (number, is_win)
    Max player win rate: 47%
    """
    if force_win:
        # Force a win for the player
        if user_bet_type == "red":
            number = random.choice(list(config.RED_NUMBERS))
        elif user_bet_type == "black":
            number = random.choice(list(config.BLACK_NUMBERS))
        elif user_bet_type == "zero":
            number = 0
        elif user_bet_type == "even":
            even_numbers = [n for n in range(1, 37) if n % 2 == 0]
            number = random.choice(even_numbers)
        elif user_bet_type == "odd":
            odd_numbers = [n for n in range(1, 37) if n % 2 == 1]
            number = random.choice(odd_numbers)
        else:
            # Specific number
            try:
                number = int(user_bet_type)
            except ValueError:
                number = random.randint(0, 36)
        return number, True

    # Normal game with house edge (47% max win rate)
    win_chance = random.random()

    if win_chance <= config.MAX_WIN_PERCENTAGE:
        # Player wins
        return generate_roulette_result(user_bet_type, force_win=True)
    else:
        # Player loses
        if user_bet_type in ["red", "black"]:
            # Return opposite color
            if user_bet_type == "red":
                losing_numbers = list(config.BLACK_NUMBERS) + [0]
            else:
                losing_numbers = list(config.RED_NUMBERS) + [0]
            number = random.choice(losing_numbers)
        elif user_bet_type == "zero":
            number = random.choice(list(config.RED_NUMBERS) + list(config.BLACK_NUMBERS))
        elif user_bet_type == "even":
            odd_numbers = [n for n in range(1, 37) if n % 2 == 1]
            number = random.choice(odd_numbers + [0])
        elif user_bet_type == "odd":
            even_numbers = [n for n in range(1, 37) if n % 2 == 0]
            number = random.choice(even_numbers + [0])
        else:
            # Specific number - return any other number
            try:
                target = int(user_bet_type)
                all_numbers = list(range(37))
                all_numbers.remove(target)
                number = random.choice(all_numbers)
            except ValueError:
                number = random.randint(0, 36)
        return number, False


def check_win(bet_type: str, number: int) -> bool:
    """Check if bet wins"""
    color = get_number_color(number)
    if bet_type == "red":
        return color == "red"
    elif bet_type == "black":
        return color == "black"
    elif bet_type == "zero":
        return number == 0
    elif bet_type == "even":
        return number > 0 and number % 2 == 0
    elif bet_type == "odd":
        return number > 0 and number % 2 == 1
    else:
        try:
            return int(bet_type) == number
        except ValueError:
            return False


def calculate_win_amount(bet_amount: float, bet_type: str) -> float:
    """Calculate win amount based on bet type"""
    if bet_type == "zero" or (bet_type.isdigit() and 0 <= int(bet_type) <= 36):
        return bet_amount * 36
    else:
        return bet_amount * 2


# ═══════════════════════════════════════
# CORS MIDDLEWARE
# ═══════════════════════════════════════

@middleware
async def cors_middleware(request: Request, handler):
    """CORS middleware for all API requests"""
    if request.method == "OPTIONS":
        response = Response(status=204)
    else:
        response = await handler(request)

    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Max-Age"] = "3600"

    return response


# ═══════════════════════════════════════
# WEB APPLICATION (API)
# ═══════════════════════════════════════

api_app = web.Application(middlewares=[cors_middleware])


async def api_health_check(request: Request) -> Response:
    """Health check endpoint"""
    return json_response({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "service": "LN Roulette API"
    })


async def api_get_user(request: Request) -> Response:
    """Get user data"""
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))

        if not user_id:
            return json_response({"error": "user_id required"}, status=400)

        user = await get_user(user_id)
        if not user:
            return json_response({"error": "User not found"}, status=404)

        return json_response({
            "user_id": user["user_id"],
            "username": user["username"],
            "nickname": user["nickname"],
            "balance": user["balance"],
            "total_games": user["total_games"],
            "total_wins": user["total_wins"],
            "free_spins": user["free_spins"],
            "games_since_withdrawal": user["games_since_withdrawal"]
        })
    except Exception as e:
        logger.error(f"API get_user error: {e}")
        return json_response({"error": str(e)}, status=500)


async def api_update_nickname(request: Request) -> Response:
    """Update user nickname"""
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        nickname = str(data.get("nickname", ""))[:30]

        if not user_id:
            return json_response({"error": "user_id required"}, status=400)

        await create_user_if_not_exists(user_id)
        await sqlite_pool.execute(
            "UPDATE users SET nickname = ? WHERE user_id = ?",
            (nickname, user_id)
        )
        await sqlite_pool.commit()

        return json_response({"success": True, "nickname": nickname})
    except Exception as e:
        logger.error(f"API update_nickname error: {e}")
        return json_response({"error": str(e)}, status=500)


async def api_place_bet(request: Request) -> Response:
    """Place a bet in single roulette"""
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        bet_type = str(data.get("bet_type", ""))
        bet_amount = float(data.get("bet_amount", 0))
        use_free_spin = data.get("use_free_spin", False)

        if not user_id or not bet_type or bet_amount <= 0:
            return json_response({"error": "Invalid parameters"}, status=400)

        user = await get_user(user_id)
        if not user:
            return json_response({"error": "User not found"}, status=404)

        # Check free spins or admin
        is_admin = user_id in config.ADMIN_IDS
        
        if is_admin:
            # Админы играют бесплатно
            actual_bet = 0
        elif use_free_spin:
            if user["free_spins"] <= 0:
                return json_response({"error": "No free spins available"}, status=400)
            actual_bet = 0
        else:
            if user["balance"] < bet_amount:
                return json_response({"error": "Insufficient balance"}, status=400)
            actual_bet = bet_amount

        # Check withdrawal condition
        can_withdraw = user["games_since_withdrawal"] >= config.MIN_GAMES_FOR_WITHDRAWAL

        # Generate result
        number, is_win = generate_roulette_result(bet_type)
        color = get_number_color(number)

        if is_win:
            win_amount = calculate_win_amount(actual_bet, bet_type)
            new_balance = user["balance"] - actual_bet + win_amount
        else:
            win_amount = 0
            new_balance = user["balance"] - actual_bet

        # Update user stats
        new_games = user["total_games"] + 1
        new_wins = user["total_wins"] + (1 if is_win else 0)
        new_free_spins = user["free_spins"] - (1 if use_free_spin else 0)

        # Add free spin every 10 games
        if new_games % config.FREE_SPIN_EVERY == 0 and not use_free_spin:
            new_free_spins += 1

        games_since = (user["games_since_withdrawal"] + 1) if not is_win else user[
            "games_since_withdrawal"
        ]

        # Update database
        await update_balance_both(user_id, new_balance)
        await sqlite_pool.execute(
            """UPDATE users SET
               total_games = ?, total_wins = ?, free_spins = ?,
               games_since_withdrawal = ?, total_bet = total_bet + ?,
               total_win_amount = total_win_amount + ?
               WHERE user_id = ?""",
            (new_games, new_wins, new_free_spins, games_since,
             actual_bet, win_amount, user_id)
        )
        await sqlite_pool.commit()

        # Save game history
        await sqlite_pool.execute(
            """INSERT INTO game_history (user_id, game_type, bet_type, bet_amount, win_amount, result, number)
               VALUES (?, 'single', ?, ?, ?, ?, ?)""",
            (user_id, bet_type, actual_bet, win_amount,
             'win' if is_win else 'loss', number)
        )
        await sqlite_pool.commit()

        return json_response({
            "success": True,
            "number": number,
            "color": color,
            "is_win": is_win,
            "win_amount": win_amount,
            "new_balance": new_balance,
            "free_spins": new_free_spins,
            "can_withdraw": can_withdraw,
            "games_played": new_games
        })
    except Exception as e:
        logger.error(f"API place_bet error: {e}")
        return json_response({"error": str(e)}, status=500)
    # ═══════════════════════════════════════
# FSM STATES
# ═══════════════════════════════════════

class SupportStates(StatesGroup):
    """Состояния для поддержки"""
    waiting_for_message = State()
    waiting_for_reply = State()


class AdminStates(StatesGroup):
    """Состояния для админ-панели"""
    waiting_for_add_balance_user = State()
    waiting_for_add_balance_amount = State()
    waiting_for_sub_balance_user = State()
    waiting_for_sub_balance_amount = State()
    waiting_for_broadcast_message = State()
    waiting_for_reply_target = State()
    waiting_for_reply_message = State()
    waiting_for_clear_confirm = State()


class UserStates(StatesGroup):
    """Состояния для пользователя"""
    waiting_for_nickname = State()


# ═══════════════════════════════════════
# KEYBOARDS
# ═══════════════════════════════════════

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главная клавиатура"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(
                    text="🎡 Играть в рулетку",
                    web_app=WebAppInfo(url=f"{config.FRONTEND_URL}?mode=single")
                )
            ]
        ],
        resize_keyboard=True,
        persistent=True
    )


def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Админ-клавиатура"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💵 Начислить", callback_data="admin_add_balance"),
            InlineKeyboardButton(text="💸 Списать", callback_data="admin_sub_balance")
        ],
        [
            InlineKeyboardButton(text="👥 Список игроков", callback_data="admin_players_list"),
            InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")
        ],
        [
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
            InlineKeyboardButton(text="📩 Ответить", callback_data="admin_reply")
        ],
        [
            InlineKeyboardButton(text="🗑 Очистка БД", callback_data="admin_clear_db"),
            InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_refresh")
        ]
    ])


def get_back_to_admin_keyboard() -> InlineKeyboardMarkup:
    """Кнопка возврата в админ-панель"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_refresh")]
    ])


# ═══════════════════════════════════════
# SUPPORT ROUTER (ПЕРВЫЙ)
# ═══════════════════════════════════════

support_router = Router()


@support_router.message(F.text == "📩 Поддержка")
async def support_start(message: Message, state: FSMContext):
    """Начало обращения в поддержку"""
    await message.answer(
        "📩 *Поддержка*\n\n"
        "Опишите вашу проблему или вопрос, и мы ответим в ближайшее время.\n\n"
        "Для отмены нажмите /cancel",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(SupportStates.waiting_for_message)


@support_router.message(SupportStates.waiting_for_message)
async def support_receive_message(message: Message, state: FSMContext, bot: Bot):
    """Получение сообщения от пользователя"""
    user_id = message.from_user.id
    msg_text = message.text or message.caption or ""

    if msg_text.startswith("/"):
        await state.clear()
        await message.answer("❌ Обращение отменено", reply_markup=get_main_keyboard())
        return

    # Save to database
    try:
        await sqlite_pool.execute(
            "INSERT INTO support_messages (user_id, message) VALUES (?, ?)",
            (user_id, msg_text)
        )
        await sqlite_pool.commit()
    except Exception:
        # Try PostgreSQL
        if pg_pool:
            try:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO support_messages (user_id, message) VALUES ($1, $2)",
                        user_id, msg_text
                    )
            except Exception as e:
                logger.error(f"Save support message error: {e}")

    await message.answer(
        "✅ Ваше сообщение отправлено! Мы ответим вам в ближайшее время.",
        reply_markup=get_main_keyboard()
    )

    # Notify admins
    for admin_id in config.ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"📩 *Новое обращение*\n\n"
                f"От: `{user_id}`\n"
                f"Имя: {message.from_user.full_name}\n"
                f"Username: @{message.from_user.username or 'нет'}\n\n"
                f"Сообщение:\n{msg_text}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="📝 Ответить",
                        callback_data=f"reply_to_{user_id}"
                    )]
                ])
            )
        except Exception as e:
            logger.error(f"Notify admin {admin_id} error: {e}")

    await state.clear()


# ═══════════════════════════════════════
# ADMIN ROUTER (ВТОРОЙ)
# ═══════════════════════════════════════

admin_router = Router()


@admin_router.message(Command("admin"))
async def admin_panel(message: Message):
    """Админ-панель"""
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer("⛔ Доступ запрещён")
        return

    await message.answer(
        "🔧 *Админ-панель*\n\nВыберите действие:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_admin_keyboard()
    )


@admin_router.callback_query(F.data == "admin_refresh")
async def admin_refresh(callback: CallbackQuery):
    """Обновить админ-панель"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    await callback.message.edit_text(
        "🔧 *Админ-панель*\n\nВыберите действие:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_admin_keyboard()
    )
    await callback.answer()


# --- ADD BALANCE ---
@admin_router.callback_query(F.data == "admin_add_balance")
async def admin_add_balance_start(callback: CallbackQuery, state: FSMContext):
    """Начало начисления баланса"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    await callback.message.edit_text(
        "💵 *Начисление баланса*\n\nВведите ID пользователя:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_add_balance_user)
    await callback.answer()


@admin_router.message(AdminStates.waiting_for_add_balance_user)
async def admin_add_balance_get_user(message: Message, state: FSMContext):
    """Получение ID пользователя для начисления"""
    try:
        target_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Неверный ID. Введите число:")
        return

    await state.update_data(target_id=target_id)
    await message.answer(
        f"💵 *Начисление баланса*\n\n"
        f"Пользователь: `{target_id}`\n"
        f"Введите сумму для начисления:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_add_balance_amount)


@admin_router.message(AdminStates.waiting_for_add_balance_amount)
async def admin_add_balance_execute(message: Message, state: FSMContext):
    """Выполнение начисления"""
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Неверная сумма. Введите положительное число:")
        return

    data = await state.get_data()
    target_id = data["target_id"]

    user = await get_user(target_id)
    if not user:
        user = await create_user_if_not_exists(target_id)

    new_balance = user["balance"] + amount
    await update_balance_both(target_id, new_balance)

    # Save transaction
    await sqlite_pool.execute(
        "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'deposit', ?, 'Admin deposit')",
        (target_id, amount)
    )
    await sqlite_pool.commit()

    await message.answer(
        f"✅ Начислено {amount:.2f}$ пользователю `{target_id}`\n"
        f"Новый баланс: {new_balance:.2f}$",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard()
    )
    await state.clear()


# --- SUBTRACT BALANCE ---
@admin_router.callback_query(F.data == "admin_sub_balance")
async def admin_sub_balance_start(callback: CallbackQuery, state: FSMContext):
    """Начало списания баланса"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    await callback.message.edit_text(
        "💸 *Списание баланса*\n\nВведите ID пользователя:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_sub_balance_user)
    await callback.answer()


@admin_router.message(AdminStates.waiting_for_sub_balance_user)
async def admin_sub_balance_get_user(message: Message, state: FSMContext):
    """Получение ID пользователя для списания"""
    try:
        target_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Неверный ID. Введите число:")
        return

    user = await get_user(target_id)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return

    await state.update_data(target_id=target_id, current_balance=user["balance"])
    await message.answer(
        f"💸 *Списание баланса*\n\n"
        f"Пользователь: `{target_id}`\n"
        f"Текущий баланс: {user['balance']:.2f}$\n"
        f"Введите сумму для списания:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_sub_balance_amount)


@admin_router.message(AdminStates.waiting_for_sub_balance_amount)
async def admin_sub_balance_execute(message: Message, state: FSMContext):
    """Выполнение списания"""
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Неверная сумма. Введите положительное число:")
        return

    data = await state.get_data()
    target_id = data["target_id"]
    current_balance = data["current_balance"]

    if amount > current_balance:
        await message.answer(
            f"❌ Недостаточно средств. Баланс: {current_balance:.2f}$"
        )
        return

    new_balance = current_balance - amount
    await update_balance_both(target_id, new_balance)

    # Save transaction
    await sqlite_pool.execute(
        "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'withdraw', ?, 'Admin withdraw')",
        (target_id, amount)
    )
    await sqlite_pool.commit()

    await message.answer(
        f"✅ Списано {amount:.2f}$ у пользователя `{target_id}`\n"
        f"Новый баланс: {new_balance:.2f}$",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard()
    )
    await state.clear()


# --- PLAYERS LIST ---
@admin_router.callback_query(F.data == "admin_players_list")
async def admin_players_list(callback: CallbackQuery):
    """Список игроков с пагинацией"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    async with sqlite_pool.execute("SELECT COUNT(*) as cnt FROM users") as cursor:
        row = await cursor.fetchone()
        total = row["cnt"] if row else 0

    async with sqlite_pool.execute(
        "SELECT user_id, username, nickname, balance, total_games, total_wins "
        "FROM users ORDER BY balance DESC LIMIT 20"
    ) as cursor:
        players = await cursor.fetchall()

    if not players:
        await callback.message.edit_text(
            "👥 Список игроков пуст\nВсего игроков: 0",
            reply_markup=get_back_to_admin_keyboard()
        )
        return

    text = f"👥 Список игроков (Топ-20 из {total})\n\n"
    for p in players:
        nick = (p["nickname"] or p["username"] or str(p["user_id"]))[:20]
        # Экранируем специальные символы для Markdown
        nick = nick.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`').replace('[', '\\[')
        text += (
            f"• `{p['user_id']}` — {nick}\n"
            f"  💰 {p['balance']:.2f}$ | 🎮 {p['total_games']} | 🏆 {p['total_wins']}\n"
        )

    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await callback.answer()


# --- STATISTICS ---
@admin_router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    """Статистика"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    # Gather statistics
    async with sqlite_pool.execute("SELECT COUNT(*) as cnt FROM users") as cursor:
        row = await cursor.fetchone()
        total_users = row["cnt"] if row else 0

    async with sqlite_pool.execute("SELECT COUNT(*) as cnt FROM game_history") as cursor:
        row = await cursor.fetchone()
        total_games = row["cnt"] if row else 0

    async with sqlite_pool.execute(
        "SELECT COALESCE(SUM(bet_amount), 0) as total FROM game_history"
    ) as cursor:
        row = await cursor.fetchone()
        total_bets = row["total"] if row else 0

    async with sqlite_pool.execute(
        "SELECT COALESCE(SUM(win_amount), 0) as total FROM game_history"
    ) as cursor:
        row = await cursor.fetchone()
        total_wins = row["total"] if row else 0

    async with sqlite_pool.execute(
        "SELECT COALESCE(SUM(commission), 0) as total FROM multiplayer_rooms"
    ) as cursor:
        row = await cursor.fetchone()
        total_commission = row["total"] if row else 0

    text = (
        "📊 *Статистика*\n\n"
        f"👤 Всего игроков: {total_users}\n"
        f"🎮 Всего игр: {total_games}\n"
        f"💵 Сумма ставок: {total_bets:.2f}$\n"
        f"🏆 Сумма выигрышей: {total_wins:.2f}$\n"
        f"🔧 Комиссия платформы: {total_commission:.2f}$\n"
        f"📈 Профит: {(total_bets - total_wins + total_commission):.2f}$"
    )

    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await callback.answer()


# --- BROADCAST ---
@admin_router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    """Начало рассылки"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    await callback.message.edit_text(
        "📢 *Массовая рассылка*\n\n"
        "Отправьте сообщение для рассылки всем пользователям.\n"
        "Поддерживается форматирование Markdown.\n\n"
        "Для отмены: /cancel",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_broadcast_message)
    await callback.answer()


@admin_router.message(AdminStates.waiting_for_broadcast_message)
async def admin_broadcast_execute(message: Message, state: FSMContext, bot: Bot):
    """Выполнение рассылки"""
    if message.text and message.text.startswith("/"):
        await state.clear()
        await message.answer("❌ Рассылка отменена", reply_markup=get_main_keyboard())
        return

    # Get all users
    async with sqlite_pool.execute("SELECT user_id FROM users") as cursor:
        users = await cursor.fetchall()

    success = 0
    failed = 0

    await message.answer(f"📤 Начинаю рассылку для {len(users)} пользователей...")

    for user in users:
        try:
            await bot.copy_message(
                chat_id=user["user_id"],
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                reply_markup=get_main_keyboard()
            )
            success += 1
            await asyncio.sleep(0.05)  # Rate limit
        except Exception as e:
            failed += 1
            logger.error(f"Broadcast to {user['user_id']} failed: {e}")

    await message.answer(
        f"✅ Рассылка завершена!\n\n"
        f"Успешно: {success}\n"
        f"Не удалось: {failed}",
        reply_markup=get_main_keyboard()
    )
    await state.clear()


# --- REPLY TO USER ---
@admin_router.callback_query(F.data.startswith("reply_to_"))
async def admin_reply_start(callback: CallbackQuery, state: FSMContext):
    """Начало ответа пользователю"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    target_id = int(callback.data.split("_")[-1])
    await state.update_data(reply_target=target_id)

    await callback.message.edit_text(
        f"📝 *Ответ пользователю* `{target_id}`\n\n"
        f"Напишите сообщение для отправки:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_reply_message)
    await callback.answer()


@admin_router.callback_query(F.data == "admin_reply")
async def admin_reply_manual(callback: CallbackQuery, state: FSMContext):
    """Ручной ввод ID для ответа"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    await callback.message.edit_text(
        "📝 *Ответ пользователю*\n\nВведите ID пользователя:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_reply_target)
    await callback.answer()


@admin_router.message(AdminStates.waiting_for_reply_target)
async def admin_reply_get_target(message: Message, state: FSMContext):
    """Получение ID для ответа"""
    try:
        target_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Неверный ID. Введите число:")
        return

    await state.update_data(reply_target=target_id)
    await message.answer(
        f"📝 *Ответ пользователю* `{target_id}`\n\nНапишите сообщение:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_reply_message)


@admin_router.message(AdminStates.waiting_for_reply_message)
async def admin_reply_execute(message: Message, state: FSMContext, bot: Bot):
    """Отправка ответа"""
    data = await state.get_data()
    target_id = data["reply_target"]

    try:
        await bot.send_message(
            target_id,
            f"📩 *Ответ от поддержки:*\n\n{message.text or ''}",
            parse_mode=ParseMode.MARKDOWN
        )
        await message.answer(
            f"✅ Ответ отправлен пользователю `{target_id}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_main_keyboard()
        )

        # Save to DB
        if pg_pool:
            try:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO support_messages (user_id, admin_id, message, is_admin_reply) "
                        "VALUES ($1, $2, $3, TRUE)",
                        target_id, message.from_user.id, message.text or ''
                    )
            except Exception as e:
                logger.error(f"Save reply error: {e}")
    except Exception as e:
        await message.answer(
            f"❌ Ошибка отправки: {e}",
            reply_markup=get_main_keyboard()
        )

    await state.clear()


# --- CLEAR DATABASE ---
@admin_router.callback_query(F.data == "admin_clear_db")
async def admin_clear_db_start(callback: CallbackQuery, state: FSMContext):
    """Подтверждение очистки БД"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    await callback.message.edit_text(
        "⚠️ *Очистка базы данных*\n\n"
        "Вы уверены? Это действие удалит ВСЕ данные!\n"
        "Будет создан бэкап перед очисткой.\n\n"
        "Введите `CONFIRM` для подтверждения:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_to_admin_keyboard()
    )
    await state.set_state(AdminStates.waiting_for_clear_confirm)
    await callback.answer()


@admin_router.message(AdminStates.waiting_for_clear_confirm)
async def admin_clear_db_execute(message: Message, state: FSMContext):
    """Выполнение очистки с бэкапом"""
    if message.text != "CONFIRM":
        await message.answer("❌ Очистка отменена", reply_markup=get_main_keyboard())
        await state.clear()
        return

    # Create backup
    backup_path = f"database/backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    try:
        import shutil
        shutil.copy2(config.SQLITE_DB_PATH, backup_path)
        await message.answer(f"📦 Бэкап сохранён: {backup_path}")
    except Exception as e:
        await message.answer(f"❌ Ошибка бэкапа: {e}")
        await state.clear()
        return

    # Clear all tables
    tables = ["game_history", "multiplayer_players", "multiplayer_rooms",
              "transactions", "ads_watched", "users"]
    for table in tables:
        await sqlite_pool.execute(f"DELETE FROM {table}")
    await sqlite_pool.commit()

    await message.answer(
        "✅ База данных очищена!\n"
        f"Бэкап: {backup_path}",
        reply_markup=get_main_keyboard()
    )
    await state.clear()


# --- /reply COMMAND ---
@admin_router.message(Command("reply"))
async def reply_command(message: Message, state: FSMContext):
    """Ответ админа через команду"""
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer("⛔ Доступ запрещён")
        return

    # Format: /reply user_id message
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer(
            "Использование: `/reply user_id сообщение`\n"
            "Пример: `/reply 123456 Привет! Как дела?`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    try:
        target_id = int(parts[1])
        reply_text = parts[2]
    except ValueError:
        await message.answer("❌ Неверный ID пользователя")
        return

    try:
        await message.bot.send_message(
            target_id,
            f"📩 *Ответ от поддержки:*\n\n{reply_text}",
            parse_mode=ParseMode.MARKDOWN
        )
        await message.answer(f"✅ Ответ отправлен пользователю `{target_id}`",
                             parse_mode=ParseMode.MARKDOWN)

        # Save to DB
        if pg_pool:
            try:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO support_messages (user_id, admin_id, message, is_admin_reply) "
                        "VALUES ($1, $2, $3, TRUE)",
                        target_id, message.from_user.id, reply_text
                    )
            except Exception as e:
                logger.error(f"Save reply error: {e}")
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки: {e}")
        # ═══════════════════════════════════════
# USER ROUTER (ТРЕТИЙ)
# ═══════════════════════════════════════

user_router = Router()


@user_router.message(Command("start"))
async def cmd_start(message: Message):
    """Команда /start"""
    user_id = message.from_user.id
    username = message.from_user.username or ''
    full_name = message.from_user.full_name

    # Create user if not exists
    user = await create_user_if_not_exists(user_id, username)

    # Приветственное сообщение
    welcome_text = (
        f"🎡 *Добро пожаловать в Roulette\\_Bot, {full_name}!*\n\n"
        f"🆔 Ваш ID: `{user_id}`\n"
        f"💰 Баланс: {user['balance']:.2f}$\n"
        f"🎁 Бесплатных спинов: {user['free_spins']}\n\n"
        f"🎯 *Режимы игры:*\n"
        f"• 🎡 Одиночная рулетка — ставки на цвет, число, чёт/нечет\n"
        f"• 👥 Мультиплеер — играйте против других игроков!\n\n"
        f"💎 Выигрывайте до ×36 от ставки!\n"
        f"📊 Играйте ответственно.\n\n"
        f"🎰 Удачной игры!"
    )

    await message.answer(
        welcome_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard()
    )


@user_router.message(Command("myid"))
async def cmd_myid(message: Message):
    """Показать ID пользователя"""
    await message.answer(
        f"🆔 Ваш ID: `{message.from_user.id}`\n"
        f"👤 Username: @{message.from_user.username or 'нет'}\n"
        f"📝 Имя: {message.from_user.full_name}",
        parse_mode=ParseMode.MARKDOWN
    )


@user_router.message(F.text == "💰 Баланс")
async def show_balance(message: Message):
    """Показать баланс"""
    user = await get_user(message.from_user.id)
    if not user:
        user = await create_user_if_not_exists(message.from_user.id,
                                                message.from_user.username)

    await message.answer(
        f"💰 *Ваш баланс*\n\n"
        f"💵 Баланс: {user['balance']:.2f}$\n"
        f"🎁 Бесплатных спинов: {user['free_spins']}\n"
        f"🏆 Побед: {user['total_wins']}\n\n"
        f"💳 Для пополнения используйте CryptoPay (USDT)\n"
        f"⚠️ Вывод доступен после {config.MIN_GAMES_FOR_WITHDRAWAL} игр",
        parse_mode=ParseMode.MARKDOWN
    )


@user_router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    """Отмена текущего действия"""
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer(
            "❌ Действие отменено",
            reply_markup=get_main_keyboard()
        )
    else:
        await message.answer("Нет активных действий для отмены")


@user_router.message(F.text == "🎡 Играть в рулетку")
async def play_roulette_button(message: Message):
    """Кнопка игры в рулетку (запасная, если WebApp не открылся)"""
    await message.answer(
        "🎡 *Рулетка открывается...*\n\n"
        "Если окно не открылось, проверьте:\n"
        "• Используете ли вы последнюю версию Telegram\n"
        "• Включена ли поддержка Mini Apps\n\n"
        "Или используйте кнопку ниже 👇",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="🎡 Открыть рулетку",
                web_app=WebAppInfo(url=f"{config.FRONTEND_URL}?mode=single")
            )]
        ])
    )


# ═══════════════════════════════════════
# WEBSOCKET SERVER FOR MULTIPLAYER
# ═══════════════════════════════════════

# Хранилище активных WebSocket соединений
ws_connections: Dict[int, web.WebSocketResponse] = {}

# Хранилище комнат (room_id -> room_data)
multiplayer_rooms: Dict[str, Dict[str, Any]] = {}

# Хранилище игроков в комнатах
room_players: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(dict)


def generate_room_id() -> str:
    """Генерация уникального ID комнаты"""
    return secrets.token_hex(4).upper()


def calculate_sector_percentage(bet_amount: float, total_bank: float) -> float:
    """Расчёт процента сектора от общего банка"""
    if total_bank <= 0:
        return 0
    return (bet_amount / total_bank) * 100


def get_player_color(player_index: int) -> str:
    """Получение цвета для игрока по индексу"""
    colors = [
        "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4",
        "#FFEAA7", "#DDA0DD", "#FF8C00", "#98D8C8",
        "#F7DC6F", "#BB8FCE"
    ]
    return colors[player_index % len(colors)]


async def broadcast_to_room(room_id: str, message: Dict[str, Any], exclude_user: int = None):
    """Отправка сообщения всем игрокам в комнате"""
    if room_id in room_players:
        for user_id in room_players[room_id]:
            if user_id != exclude_user and user_id in ws_connections:
                ws = ws_connections[user_id]
                if not ws.closed:
                    try:
                        await ws.send_json(message)
                    except Exception as e:
                        logger.error(f"Broadcast to {user_id} error: {e}")


async def handle_websocket(request: Request) -> web.WebSocketResponse:
    """Обработчик WebSocket соединений"""
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
                        # Подключение пользователя
                        user_id = int(data.get("user_id", 0))
                        if user_id:
                            # Закрываем старое соединение если есть
                            if user_id in ws_connections and not ws_connections[
                                user_id].closed:
                                try:
                                    await ws_connections[user_id].close(
                                        code=1000,
                                        message="New connection established"
                                    )
                                except:
                                    pass

                            ws_connections[user_id] = ws
                            await ws.send_json({
                                "type": "connected",
                                "user_id": user_id,
                                "message": "Connected to server"
                            })
                            logger.info(f"WebSocket user {user_id} connected")

                    elif action == "create_room":
                        # Создание комнаты
                        user_id = int(data.get("user_id", 0))
                        bet_amount = float(data.get("bet_amount", 0))

                        user = await get_user(user_id)
                        if not user or user["balance"] < bet_amount:
                            await ws.send_json({
                                "type": "error",
                                "message": "Недостаточно средств"
                            })
                            continue

                        room_id = generate_room_id()
                        color = get_player_color(len(multiplayer_rooms))

                        multiplayer_rooms[room_id] = {
                            "room_id": room_id,
                            "status": "waiting",
                            "bank": bet_amount,
                            "players": [],
                            "created_at": time.time(),
                            "timer_start": time.time()
                        }

                        room_players[room_id][user_id] = {
                            "user_id": user_id,
                            "username": user["username"] or str(user_id),
                            "nickname": user["nickname"] or user["username"] or f"Player_{user_id}",
                            "bet_amount": bet_amount,
                            "color": color,
                            "avatar": data.get("avatar", "🎲")
                        }

                        multiplayer_rooms[room_id]["players"].append(user_id)
                        current_room = room_id

                        # Списываем ставку
                        new_balance = user["balance"] - bet_amount
                        await update_balance_both(user_id, new_balance)

                        await ws.send_json({
                            "type": "room_created",
                            "room_id": room_id,
                            "players": get_room_players_list(room_id),
                            "bank": bet_amount,
                            "timer": config.MULTIPLAYER_JOIN_TIMEOUT
                        })

                        logger.info(f"Room {room_id} created by user {user_id}")

                    elif action == "join_room":
                        # Присоединение к комнате
                        user_id = int(data.get("user_id", 0))
                        room_id = data.get("room_id")
                        bet_amount = float(data.get("bet_amount", 0))

                        if room_id not in multiplayer_rooms:
                            await ws.send_json({
                                "type": "error",
                                "message": "Комната не найдена"
                            })
                            continue

                        room = multiplayer_rooms[room_id]

                        if room["status"] != "waiting":
                            await ws.send_json({
                                "type": "error",
                                "message": "Игра уже началась"
                            })
                            continue

                        if len(room["players"]) >= config.MAX_PLAYERS_MULTIPLAYER:
                            await ws.send_json({
                                "type": "error",
                                "message": "Комната заполнена"
                            })
                            continue

                        if user_id in room_players[room_id]:
                            await ws.send_json({
                                "type": "error",
                                "message": "Вы уже в комнате"
                            })
                            continue

                        user = await get_user(user_id)
                        if not user or user["balance"] < bet_amount:
                            await ws.send_json({
                                "type": "error",
                                "message": "Недостаточно средств"
                            })
                            continue

                        # Добавляем игрока
                        color = get_player_color(len(room["players"]))
                        room_players[room_id][user_id] = {
                            "user_id": user_id,
                            "username": user["username"] or str(user_id),
                            "nickname": user["nickname"] or user["username"] or f"Player_{user_id}",
                            "bet_amount": bet_amount,
                            "color": color,
                            "avatar": data.get("avatar", "🎲")
                        }

                        room["players"].append(user_id)
                        room["bank"] += bet_amount
                        current_room = room_id

                        # Списываем ставку
                        new_balance = user["balance"] - bet_amount
                        await update_balance_both(user_id, new_balance)

                        await ws.send_json({
                            "type": "joined_room",
                            "room_id": room_id,
                            "players": get_room_players_list(room_id),
                            "bank": room["bank"],
                            "timer": max(0, config.MULTIPLAYER_JOIN_TIMEOUT -
                                        int(time.time() - room["timer_start"]))
                        })

                        # Уведомляем других игроков
                        await broadcast_to_room(room_id, {
                            "type": "player_joined",
                            "player": {
                                "user_id": user_id,
                                "nickname": room_players[room_id][user_id]["nickname"],
                                "bet_amount": bet_amount,
                                "color": color
                            },
                            "players": get_room_players_list(room_id),
                            "bank": room["bank"],
                            "players_count": len(room["players"])
                        }, exclude_user=user_id)

                    elif action == "leave_room":
                        # Выход из комнаты
                        user_id = int(data.get("user_id", 0))
                        room_id = data.get("room_id")

                        if room_id in room_players and user_id in room_players[
                            room_id]:
                            await remove_player_from_room(room_id, user_id, "left")
                            current_room = None

                            await ws.send_json({
                                "type": "left_room",
                                "message": "Вы вышли из комнаты"
                            })

                    elif action == "start_game":
                        # Запуск игры (только создатель)
                        user_id = int(data.get("user_id", 0))
                        room_id = data.get("room_id")

                        if room_id not in multiplayer_rooms:
                            continue

                        room = multiplayer_rooms[room_id]
                        if room["players"][0] != user_id:
                            await ws.send_json({
                                "type": "error",
                                "message": "Только создатель может запустить игру"
                            })
                            continue

                        if len(room["players"]) < config.MIN_PLAYERS_MULTIPLAYER:
                            await ws.send_json({
                                "type": "error",
                                "message": f"Минимум {config.MIN_PLAYERS_MULTIPLAYER} игрока"
                            })
                            continue

                        # Запускаем игру
                        asyncio.create_task(run_multiplayer_game(room_id))

                    elif action == "get_rooms":
                        # Получить список активных комнат
                        rooms_list = []
                        for rid, room in multiplayer_rooms.items():
                            if room["status"] == "waiting":
                                rooms_list.append({
                                    "room_id": rid,
                                    "players_count": len(room["players"]),
                                    "bank": room["bank"],
                                    "time_left": max(0, config.MULTIPLAYER_JOIN_TIMEOUT -
                                                    int(time.time() - room["timer_start"]))
                                })

                        await ws.send_json({
                            "type": "rooms_list",
                            "rooms": rooms_list
                        })

                    elif action == "ping":
                        await ws.send_json({"type": "pong"})

                except json.JSONDecodeError:
                    await ws.send_json({"type": "error", "message": "Invalid JSON"})
                except Exception as e:
                    logger.error(f"WebSocket message error: {e}")
                    await ws.send_json({"type": "error", "message": str(e)})

            elif msg.type == web.WSMsgType.ERROR:
                logger.error(f"WebSocket error: {ws.exception()}")

    except Exception as e:
        logger.error(f"WebSocket handler error: {e}")
    finally:
        # Очистка при отключении
        if user_id and user_id in ws_connections:
            del ws_connections[user_id]

        if current_room and user_id:
            await remove_player_from_room(current_room, user_id, "disconnected")

        logger.info(f"WebSocket user {user_id} disconnected")

    return ws
    
def get_room_players_list(room_id: str) -> List[Dict]:
    """Получение списка игроков в комнате"""
    players = []
    if room_id in room_players:
        for uid, pdata in room_players[room_id].items():
            players.append({
                "user_id": uid,
                "nickname": pdata["nickname"],
                "bet_amount": pdata["bet_amount"],
                "color": pdata["color"],
                "avatar": pdata.get("avatar", "🎲"),
                "percentage": calculate_sector_percentage(
                    pdata["bet_amount"],
                    multiplayer_rooms.get(room_id, {}).get("bank", 1)
                )
            })
    return players


async def remove_player_from_room(room_id: str, user_id: int, reason: str):
    """Удаление игрока из комнаты"""
    if room_id not in room_players or user_id not in room_players[room_id]:
        return

    player_data = room_players[room_id][user_id]
    bet_amount = player_data["bet_amount"]

    # Возвращаем ставку (если игра не началась)
    room = multiplayer_rooms.get(room_id)
    if room and room["status"] == "waiting":
        user = await get_user(user_id)
        if user:
            new_balance = user["balance"] + bet_amount
            await update_balance_both(user_id, new_balance)

    # Удаляем игрока
    del room_players[room_id][user_id]

    if room:
        if user_id in room["players"]:
            room["players"].remove(user_id)
        room["bank"] -= bet_amount

        # Если комната пуста - удаляем
        if len(room["players"]) == 0:
            del multiplayer_rooms[room_id]
            del room_players[room_id]
        else:
            # Уведомляем оставшихся
            await broadcast_to_room(room_id, {
                "type": "player_left",
                "user_id": user_id,
                "nickname": player_data["nickname"],
                "reason": reason,
                "players": get_room_players_list(room_id),
                "bank": room["bank"],
                "players_count": len(room["players"])
            })


async def run_multiplayer_game(room_id: str):
    """Запуск мультиплеерной игры"""
    if room_id not in multiplayer_rooms:
        return

    room = multiplayer_rooms[room_id]
    room["status"] = "spinning"

    # Уведомляем всех о начале
    await broadcast_to_room(room_id, {
        "type": "game_starting",
        "message": "Игра начинается! Вращение колеса...",
        "players": get_room_players_list(room_id),
        "bank": room["bank"]
    })

    # Ждём анимацию (5 секунд)
    await asyncio.sleep(5)

    # Выбираем победителя (пропорционально ставкам)
    players_in_room = room["players"].copy()
    if not players_in_room:
        room["status"] = "finished"
        return

    # Взвешенный случайный выбор
    weights = []
    for uid in players_in_room:
        if uid in room_players[room_id]:
            weights.append(room_players[room_id][uid]["bet_amount"])

    total_weight = sum(weights)
    if total_weight <= 0:
        winner_id = random.choice(players_in_room)
    else:
        # Нормализуем веса
        normalized = [w / total_weight for w in weights]
        winner_id = random.choices(players_in_room, weights=normalized, k=1)[0]

    # Расчёт выигрыша
    commission = room["bank"] * config.PLATFORM_COMMISSION
    win_amount = room["bank"] - commission

    # Обновляем баланс победителя
    winner_user = await get_user(winner_id)
    if winner_user:
        new_balance = winner_user["balance"] + win_amount
        await update_balance_both(winner_id, new_balance)

    # Сохраняем в БД
    await sqlite_pool.execute(
        """UPDATE multiplayer_rooms SET status = 'finished', winner_id = ?,
           commission = ?, finished_at = CURRENT_TIMESTAMP WHERE room_id = ?""",
        (winner_id, commission, room_id)
    )
    await sqlite_pool.commit()

    # Обновляем статус
    room["status"] = "finished"
    room["winner_id"] = winner_id
    room["commission"] = commission

    winner_nickname = room_players[room_id].get(winner_id, {}).get("nickname", "Unknown")
    winner_color = room_players[room_id].get(winner_id, {}).get("color", "#FFD700")

    # Уведомляем всех о результате
    await broadcast_to_room(room_id, {
        "type": "game_result",
        "winner_id": winner_id,
        "winner_nickname": winner_nickname,
        "winner_color": winner_color,
        "win_amount": win_amount,
        "bank": room["bank"],
        "commission": commission,
        "players": get_room_players_list(room_id)
    })

    # Чистим комнату через 10 секунд
    await asyncio.sleep(10)
    if room_id in multiplayer_rooms:
        for uid in list(room_players.get(room_id, {}).keys()):
            if uid in ws_connections and not ws_connections[uid].closed:
                try:
                    await ws_connections[uid].send_json({
                        "type": "room_closed",
                        "message": "Игра завершена, комната закрыта"
                    })
                except:
                    pass
        del multiplayer_rooms[room_id]
        if room_id in room_players:
            del room_players[room_id]


# Таймер для авто-запуска комнат
async def multiplayer_timer_checker():
    """Проверка таймеров комнат"""
    while True:
        await asyncio.sleep(1)
        now = time.time()
        rooms_to_start = []

        for room_id, room in list(multiplayer_rooms.items()):
            if room["status"] != "waiting":
                continue

            time_left = config.MULTIPLAYER_JOIN_TIMEOUT - (now - room["timer_start"])

            # Отправляем обновление таймера
            if time_left > 0 and int(time_left) % 5 == 0:
                await broadcast_to_room(room_id, {
                    "type": "timer_update",
                    "time_left": int(time_left)
                })

            # Автозапуск по истечении таймера
            if time_left <= 0:
                if len(room["players"]) >= config.MIN_PLAYERS_MULTIPLAYER:
                    rooms_to_start.append(room_id)
                else:
                    # Недостаточно игроков - отменяем
                    await broadcast_to_room(room_id, {
                        "type": "game_cancelled",
                        "message": "Недостаточно игроков. Ставки возвращены."
                    })
                    # Возвращаем ставки
                    for uid in list(room_players.get(room_id, {}).keys()):
                        await remove_player_from_room(room_id, uid, "timeout")
                    if room_id in multiplayer_rooms:
                        del multiplayer_rooms[room_id]

        # Запускаем игры
        for room_id in rooms_to_start:
            asyncio.create_task(run_multiplayer_game(room_id))


# ═══════════════════════════════════════
# API ENDPOINTS (REGISTRATION)
# ═══════════════════════════════════════

# Multiplayer API endpoints
async def api_get_rooms(request: Request) -> Response:
    """Получение списка активных комнат"""
    rooms_list = []
    for room_id, room in multiplayer_rooms.items():
        if room["status"] == "waiting":
            rooms_list.append({
                "room_id": room_id,
                "players_count": len(room["players"]),
                "bank": room["bank"],
                "time_left": max(0, config.MULTIPLAYER_JOIN_TIMEOUT -
                                int(time.time() - room["timer_start"]))
            })
    return json_response({"rooms": rooms_list})


async def api_room_info(request: Request) -> Response:
    """Информация о комнате"""
    try:
        data = await request.json()
        room_id = data.get("room_id")

        if room_id not in multiplayer_rooms:
            return json_response({"error": "Room not found"}, status=404)

        room = multiplayer_rooms[room_id]
        return json_response({
            "room_id": room_id,
            "status": room["status"],
            "bank": room["bank"],
            "players": get_room_players_list(room_id),
            "players_count": len(room["players"]),
            "time_left": max(0, config.MULTIPLAYER_JOIN_TIMEOUT -
                            int(time.time() - room["timer_start"]))
        })
    except Exception as e:
        return json_response({"error": str(e)}, status=500)


async def api_transaction_history(request: Request) -> Response:
    """История транзакций пользователя"""
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        limit = int(data.get("limit", 20))

        async with sqlite_pool.execute(
            "SELECT * FROM game_history WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        ) as cursor:
            rows = await cursor.fetchall()

        history = []
        for row in rows:
            history.append({
                "id": row["id"],
                "game_type": row["game_type"],
                "bet_type": row["bet_type"],
                "bet_amount": row["bet_amount"],
                "win_amount": row["win_amount"],
                "result": row["result"],
                "number": row["number"],
                "created_at": row["created_at"]
            })

        return json_response({"history": history, "count": len(history)})
    except Exception as e:
        return json_response({"error": str(e)}, status=500)


async def api_withdraw_request(request: Request) -> Response:
    """Запрос на вывод средств"""
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        amount = float(data.get("amount", 0))

        if amount <= 0:
            return json_response({"error": "Invalid amount"}, status=400)

        user = await get_user(user_id)
        if not user:
            return json_response({"error": "User not found"}, status=404)

        if user["balance"] < amount:
            return json_response({"error": "Insufficient balance"}, status=400)

        if user["games_since_withdrawal"] < config.MIN_GAMES_FOR_WITHDRAWAL:
            return json_response({
                "error": f"Need {config.MIN_GAMES_FOR_WITHDRAWAL} games for withdrawal",
                "games_played": user["games_since_withdrawal"],
                "games_needed": config.MIN_GAMES_FOR_WITHDRAWAL - user[
                    "games_since_withdrawal"]
            }, status=400)

        # Списываем средства
        new_balance = user["balance"] - amount
        await update_balance_both(user_id, new_balance)

        # Сохраняем транзакцию
        await sqlite_pool.execute(
            "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'withdraw', ?, 'Withdrawal request')",
            (user_id, amount)
        )
        await sqlite_pool.commit()

        # Уведомляем админов
        for admin_id in config.ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"💸 *Запрос на вывод*\n\n"
                    f"Пользователь: `{user_id}`\n"
                    f"Сумма: {amount:.2f}$\n"
                    f"Остаток: {new_balance:.2f}$",
                    parse_mode=ParseMode.MARKDOWN
                )
            except:
                pass

        return json_response({
            "success": True,
            "new_balance": new_balance,
            "message": "Withdrawal request submitted"
        })
    except Exception as e:
        return json_response({"error": str(e)}, status=500)


async def api_crypto_pay_webhook(request: Request) -> Response:
    """Webhook для CryptoPay"""
    try:
        data = await request.json()
        # Верификация подписи CryptoPay
        # Здесь должна быть проверка подписи

        user_id = int(data.get("user_id", 0))
        amount = float(data.get("amount", 0))

        if user_id and amount > 0:
            user = await get_user(user_id)
            if not user:
                user = await create_user_if_not_exists(user_id)

            new_balance = user["balance"] + amount
            await update_balance_both(user_id, new_balance)

            await sqlite_pool.execute(
                "INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'deposit', ?, 'CryptoPay deposit')",
                (user_id, amount)
            )
            await sqlite_pool.commit()

            # Уведомляем пользователя
            try:
                await bot.send_message(
                    user_id,
                    f"✅ *Пополнение баланса*\n\n"
                    f"Сумма: {amount:.2f}$\n"
                    f"Новый баланс: {new_balance:.2f}$",
                    parse_mode=ParseMode.MARKDOWN
                )
            except:
                pass

        return json_response({"status": "ok"})
    except Exception as e:
        return json_response({"error": str(e)}, status=500)


# ═══════════════════════════════════════
# MAIN APPLICATION SETUP
# ═══════════════════════════════════════

bot = Bot(
    token=config.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
)
dp = Dispatcher(storage=MemoryStorage())

# Register routers in CORRECT ORDER
dp.include_router(support_router)   # ПЕРВЫЙ
dp.include_router(admin_router)     # ВТОРОЙ
dp.include_router(user_router)      # ТРЕТИЙ


async def on_startup():
    """Действия при запуске"""
    logger.info("🚀 Starting LN Roulette Bot...")

    # Initialize databases
    await init_sqlite()
    await init_postgres()

    # Start background tasks
    asyncio.create_task(multiplayer_timer_checker())

    # Set webhook
    await bot.set_webhook(
        url=config.WEBHOOK_URL,
        allowed_updates=["message", "callback_query", "inline_query"]
    )

    logger.info(f"✅ Webhook set to {config.WEBHOOK_URL}")
    logger.info("✅ Bot started successfully!")


async def on_shutdown():
    """Действия при остановке"""
    logger.info("🛑 Shutting down...")

    # Close WebSocket connections
    for user_id, ws in list(ws_connections.items()):
        if not ws.closed:
            await ws.close(code=1001, message="Server shutting down")
    ws_connections.clear()

    # Clear rooms
    multiplayer_rooms.clear()
    room_players.clear()

    # Delete webhook
    await bot.delete_webhook()
    logger.info("✅ Webhook deleted")

    # Close sessions
    await bot.session.close()
    await close_databases()

    logger.info("✅ Bot stopped")


def create_app() -> web.Application:
    """Создание aiohttp приложения"""
    app = web.Application()

    # CORS middleware для всего приложения
    @middleware
    async def global_cors(request: Request, handler):
        if request.method == "OPTIONS":
            response = Response(status=204)
        else:
            response = await handler(request)
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        response.headers["Access-Control-Max-Age"] = "3600"
        return response

    app.middlewares.append(global_cors)

    # Health check на корневом уровне
    async def health_handler(request):
        return web.json_response({"status": "ok", "timestamp": datetime.now().isoformat()})

    app.router.add_get("/health", health_handler)

    # API endpoints регистрируем НА САМОМ app, а не на subapp
    app.router.add_post("/api/user/get", api_get_user)
    app.router.add_post("/api/user/update_nickname", api_update_nickname)
    app.router.add_post("/api/user/history", api_transaction_history)
    app.router.add_post("/api/user/withdraw", api_withdraw_request)
    app.router.add_post("/api/game/place_bet", api_place_bet)
    app.router.add_post("/api/multiplayer/rooms", api_get_rooms)
    app.router.add_post("/api/multiplayer/room_info", api_room_info)
    app.router.add_post("/api/crypto/webhook", api_crypto_pay_webhook)

    # WebSocket
    app.router.add_get("/ws", handle_websocket)

    # Webhook для Telegram
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    webhook_requests_handler.register(app, path=config.WEBHOOK_PATH)

    setup_application(app, dp, bot=bot)

    return app


if __name__ == "__main__":
    app = create_app()
    logger.info(f"Starting web server on {config.WEBHOOK_HOST}:{config.WEBHOOK_PORT}")

    # Запускаем startup через on_startup в app
    app.on_startup.append(lambda app: on_startup())

    web.run_app(
        app,
        host=config.WEBHOOK_HOST,
        port=config.WEBHOOK_PORT,
        access_log=logger
    )

async def on_startup():
    """Действия при запуске"""
    logger.info("🚀 Starting LN Roulette Bot...")

    await init_sqlite()
    await init_postgres()

    asyncio.create_task(multiplayer_timer_checker())
    
    # Пинг самого себя каждые 4 минуты чтобы не засыпать
    async def keep_alive():
        while True:
            await asyncio.sleep(240)  # 4 минуты
            try:
                async with aiohttp.ClientSession() as session:
                    await session.get(f"{config.API_URL}/health")
            except:
                pass
    
    asyncio.create_task(keep_alive())

    await bot.set_webhook(
        url=config.WEBHOOK_URL,
        allowed_updates=["message", "callback_query", "inline_query"]
    )

    logger.info(f"✅ Webhook set to {config.WEBHOOK_URL}")
    logger.info("✅ Bot started successfully!")