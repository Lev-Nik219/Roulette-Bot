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
import secrets
import shutil
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict
import sqlite3
import aiosqlite

from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    WebAppInfo, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from aiohttp import web
from aiohttp.web import middleware
from aiohttp.web_request import Request
from aiohttp.web_response import Response, json_response

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler('bot.log', encoding='utf-8')]
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════

class Config:
    BOT_TOKEN = "8756148710:AAHAF6f4fa9v9IruYkXq0_rshv-d65h7Yqg"
    BOT_USERNAME = "@lnRoulette_bot"
    CRYPTO_PAY_TOKEN = "581586:AAmQppk9XEGf4EKxd8fpj0fReHZsIRietdW"
    ADMIN_IDS = [1167503795, 1670366784]
    SQLITE_DB_PATH = "database/mini_app.db"
    POSTGRES_DSN = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/roulette_db")
    API_URL = "https://roulette-bot-8i8t.onrender.com"
    FRONTEND_URL = "https://roulette-bot-six.vercel.app"
    WEBHOOK_HOST = "0.0.0.0"
    WEBHOOK_PORT = int(os.getenv("PORT", 8000))
    WEBHOOK_PATH = "/webhook"
    WEBHOOK_URL = f"{API_URL}{WEBHOOK_PATH}"
    MAX_PLAYERS_MULTIPLAYER = 6
    MIN_PLAYERS_MULTIPLAYER = 2
    MULTIPLAYER_JOIN_TIMEOUT = 30
    PLATFORM_COMMISSION = 0.10
    MAX_WIN_PERCENTAGE = 0.47
    FREE_SPIN_EVERY = 10
    MIN_GAMES_FOR_WITHDRAWAL = 2
    ROULETTE_NUMBERS = list(range(37))
    RED_NUMBERS = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}
    BLACK_NUMBERS = {2, 4, 6, 8, 10, 11, 13, 15, 17, 20, 22, 24, 26, 28, 29, 31, 33, 35}
    GREEN_NUMBERS = {0}

config = Config()

# ═══════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════

sqlite_pool: Optional[aiosqlite.Connection] = None
pg_pool: Optional[asyncpg.Pool] = None

def execute_sqlite_with_retry(max_retries=5, delay=0.1):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (sqlite3.OperationalError, aiosqlite.OperationalError) as e:
                    last_error = e
                    if "database is locked" in str(e).lower():
                        logger.warning(f"SQLite locked (attempt {attempt+1}/{max_retries})")
                        await asyncio.sleep(delay * (attempt + 1))
                    else:
                        raise
            raise last_error
        return wrapper
    return decorator

async def init_sqlite():
    global sqlite_pool
    os.makedirs("database", exist_ok=True)
    sqlite_pool = await aiosqlite.connect(config.SQLITE_DB_PATH)
    sqlite_pool.row_factory = aiosqlite.Row
    await sqlite_pool.execute("PRAGMA journal_mode=WAL")
    await sqlite_pool.execute("PRAGMA busy_timeout=5000")
    await sqlite_pool.execute("PRAGMA foreign_keys=ON")
    await sqlite_pool.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, nickname TEXT DEFAULT '',
            balance REAL DEFAULT 0.0, total_games INTEGER DEFAULT 0, total_wins INTEGER DEFAULT 0,
            total_bet REAL DEFAULT 0.0, total_win_amount REAL DEFAULT 0.0,
            free_spins INTEGER DEFAULT 0, games_since_withdrawal INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS game_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, game_type TEXT NOT NULL,
            bet_type TEXT, bet_amount REAL DEFAULT 0, win_amount REAL DEFAULT 0,
            result TEXT, number INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );
        CREATE TABLE IF NOT EXISTS multiplayer_rooms (
            room_id TEXT PRIMARY KEY, status TEXT DEFAULT 'waiting', bank REAL DEFAULT 0.0,
            winner_id INTEGER, commission REAL DEFAULT 0.0, players_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS multiplayer_players (
            id INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT NOT NULL, user_id INTEGER NOT NULL,
            bet_amount REAL DEFAULT 0, color TEXT, won INTEGER DEFAULT 0,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (room_id) REFERENCES multiplayer_rooms(room_id)
        );
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            type TEXT NOT NULL, amount REAL DEFAULT 0, description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_users_id ON users(user_id);
        CREATE INDEX IF NOT EXISTS idx_game_history_user ON game_history(user_id);
        CREATE INDEX IF NOT EXISTS idx_rooms_status ON multiplayer_rooms(status);
    """)
    await sqlite_pool.commit()
    
    # Восстанавливаем данные из PostgreSQL если есть
    if pg_pool:
        try:
            async with pg_pool.acquire() as conn:
                pg_users = await conn.fetch("SELECT * FROM users")
                for u in pg_users:
                    await sqlite_pool.execute(
                        "INSERT OR REPLACE INTO users (user_id, username, nickname, balance, total_games, total_wins, total_bet, total_win_amount, free_spins, games_since_withdrawal, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,NOW(),NOW())",
                        (u["user_id"], u["username"], u["nickname"], float(u["balance"]), u["total_games"], u["total_wins"], float(u["total_bet"]), float(u["total_win_amount"]), u["free_spins"], u["games_since_withdrawal"])
                    )
                await sqlite_pool.commit()
                logger.info(f"✅ Restored {len(pg_users)} users from PostgreSQL")
        except Exception as e:
            logger.warning(f"⚠️ Could not restore from PG: {e}")
    
    logger.info("✅ SQLite initialized")

async def init_postgres():
    global pg_pool
    try:
        pg_pool = await asyncpg.create_pool(dsn=config.POSTGRES_DSN, min_size=2, max_size=10, command_timeout=60)
        async with pg_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY, username VARCHAR(255), nickname VARCHAR(30) DEFAULT '',
                    balance DECIMAL(20,8) DEFAULT 0.0, total_games INTEGER DEFAULT 0, total_wins INTEGER DEFAULT 0,
                    total_bet DECIMAL(20,8) DEFAULT 0.0, total_win_amount DECIMAL(20,8) DEFAULT 0.0,
                    free_spins INTEGER DEFAULT 0, games_since_withdrawal INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS support_messages (
                    id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, admin_id BIGINT,
                    message TEXT NOT NULL, is_admin_reply BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_pg_users_id ON users(user_id);
            """)
        logger.info("✅ PostgreSQL initialized")
    except Exception as e:
        logger.warning(f"⚠️ PostgreSQL not available: {e}")

async def close_databases():
    global sqlite_pool, pg_pool
    if sqlite_pool: await sqlite_pool.close()
    if pg_pool: await pg_pool.close()

# ═══════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════

async def get_user(user_id: int) -> Optional[Dict]:
    try:
        async with sqlite_pool.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"get_user error: {e}")
        return None

async def create_user_if_not_exists(user_id: int, username: str = None) -> Dict:
    user = await get_user(user_id)
    if not user:
        now = datetime.now().isoformat()
        await sqlite_pool.execute(
            "INSERT INTO users (user_id, username, nickname, balance, created_at, updated_at) VALUES (?,?,?,0.0,?,?)",
            (user_id, username or '', '', now, now)
        )
        await sqlite_pool.commit()
        if pg_pool:
            try:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO users (user_id, username, nickname, balance) VALUES ($1,$2,'',0.0) ON CONFLICT (user_id) DO NOTHING",
                        user_id, username or ''
                    )
            except Exception as e:
                logger.error(f"PG insert error: {e}")
        user = await get_user(user_id)
    return user

@execute_sqlite_with_retry()
async def update_balance_both(user_id: int, new_balance: float):
    now = datetime.now().isoformat()
    await sqlite_pool.execute("UPDATE users SET balance = ?, updated_at = ? WHERE user_id = ?", (new_balance, now, user_id))
    await sqlite_pool.commit()
    if pg_pool:
        try:
            async with pg_pool.acquire() as conn:
                await conn.execute("UPDATE users SET balance = $1, updated_at = NOW() WHERE user_id = $2", new_balance, user_id)
        except Exception as e:
            logger.error(f"PG update error: {e}")

def get_number_color(number: int) -> str:
    if number in config.RED_NUMBERS: return "red"
    elif number in config.BLACK_NUMBERS: return "black"
    else: return "green"

def generate_roulette_result(user_bet_type: str, force_win: bool = False) -> Tuple[int, bool]:
    if force_win:
        if user_bet_type == "red": number = random.choice(list(config.RED_NUMBERS))
        elif user_bet_type == "black": number = random.choice(list(config.BLACK_NUMBERS))
        elif user_bet_type == "zero": number = 0
        elif user_bet_type == "even": number = random.choice([n for n in range(1,37) if n%2==0])
        elif user_bet_type == "odd": number = random.choice([n for n in range(1,37) if n%2==1])
        else:
            try: number = int(user_bet_type)
            except ValueError: number = random.randint(0,36)
        return number, True
    if random.random() <= config.MAX_WIN_PERCENTAGE:
        return generate_roulette_result(user_bet_type, force_win=True)
    else:
        if user_bet_type in ["red","black"]:
            losing = list(config.BLACK_NUMBERS)+[0] if user_bet_type=="red" else list(config.RED_NUMBERS)+[0]
            number = random.choice(losing)
        elif user_bet_type == "zero": number = random.choice(list(config.RED_NUMBERS)+list(config.BLACK_NUMBERS))
        elif user_bet_type == "even": number = random.choice([n for n in range(1,37) if n%2==1]+[0])
        elif user_bet_type == "odd": number = random.choice([n for n in range(1,37) if n%2==0]+[0])
        else:
            try:
                target = int(user_bet_type)
                all_nums = list(range(37)); all_nums.remove(target)
                number = random.choice(all_nums)
            except ValueError: number = random.randint(0,36)
        return number, False

def calculate_win_amount(bet_amount: float, bet_type: str) -> float:
    if bet_type == "zero" or (bet_type.isdigit() and 0 <= int(bet_type) <= 36): return bet_amount * 36
    return bet_amount * 2

# ═══════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════

async def api_get_user(request: Request) -> Response:
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        if not user_id: return json_response({"error": "user_id required"}, status=400)
        user = await get_user(user_id)
        if not user: return json_response({"error": "User not found"}, status=404)
        return json_response({
            "user_id": user["user_id"], "username": user["username"], "balance": user["balance"],
            "total_games": user["total_games"], "total_wins": user["total_wins"],
            "free_spins": user["free_spins"], "games_since_withdrawal": user["games_since_withdrawal"]
        })
    except Exception as e: return json_response({"error": str(e)}, status=500)

async def api_place_bet(request: Request) -> Response:
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        bet_type = str(data.get("bet_type", ""))
        bet_amount = float(data.get("bet_amount", 0))
        use_free_spin = data.get("use_free_spin", False)
        if not user_id or not bet_type: return json_response({"error": "Invalid parameters"}, status=400)

        user = await get_user(user_id)
        if not user: user = await create_user_if_not_exists(user_id)

        is_admin = user_id in config.ADMIN_IDS
        if is_admin: actual_bet = 0
        elif use_free_spin:
            if user["free_spins"] <= 0: return json_response({"error": "No free spins"}, status=400)
            actual_bet = 0
        else:
            if bet_amount <= 0: return json_response({"error": "Bet amount required"}, status=400)
            if user["balance"] < bet_amount: return json_response({"error": "Insufficient balance"}, status=400)
            actual_bet = bet_amount

        number, is_win = generate_roulette_result(bet_type)
        color = get_number_color(number)
        win_amount = calculate_win_amount(actual_bet, bet_type) if is_win else 0
        new_balance = user["balance"] - actual_bet + win_amount

        now = datetime.now().isoformat()
        add_free = 1 if (user["total_games"] + 1) % config.FREE_SPIN_EVERY == 0 and not use_free_spin else 0
        await sqlite_pool.execute(
            "UPDATE users SET balance=?, total_games=total_games+1, total_wins=total_wins+?, free_spins=free_spins+?, games_since_withdrawal=games_since_withdrawal+1, total_bet=total_bet+?, total_win_amount=total_win_amount+?, updated_at=? WHERE user_id=?",
            (new_balance, 1 if is_win else 0, add_free, actual_bet, win_amount, now, user_id)
        )
        await sqlite_pool.commit()

        if pg_pool:
            try:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE users SET balance=$1, total_games=total_games+1, total_wins=total_wins+$2, free_spins=free_spins+$3, games_since_withdrawal=games_since_withdrawal+1, total_bet=total_bet+$4, total_win_amount=total_win_amount+$5, updated_at=NOW() WHERE user_id=$6",
                        new_balance, 1 if is_win else 0, add_free, actual_bet, win_amount, user_id
                    )
            except Exception as e: logger.error(f"PG error: {e}")

        await sqlite_pool.execute(
            "INSERT INTO game_history (user_id, game_type, bet_type, bet_amount, win_amount, result, number) VALUES (?,'single',?,?,?,?,?)",
            (user_id, bet_type, actual_bet, win_amount, 'win' if is_win else 'loss', number)
        )
        await sqlite_pool.commit()

        updated_user = await get_user(user_id)
        return json_response({
            "success": True, "number": number, "color": color, "is_win": is_win,
            "win_amount": win_amount, "new_balance": new_balance,
            "free_spins": updated_user["free_spins"], "games_played": updated_user["total_games"],
            "games_since_withdrawal": updated_user["games_since_withdrawal"]
        })
    except Exception as e:
        logger.error(f"api_place_bet error: {e}")
        return json_response({"error": str(e)}, status=500)

async def api_withdraw_request(request: Request) -> Response:
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        amount = float(data.get("amount", 0))
        if amount <= 0: return json_response({"error": "Invalid amount"}, status=400)
        user = await get_user(user_id)
        if not user: return json_response({"error": "User not found"}, status=404)
        if user["balance"] < amount: return json_response({"error": "Insufficient balance"}, status=400)
        if user["games_since_withdrawal"] < config.MIN_GAMES_FOR_WITHDRAWAL:
            return json_response({"error": "Need more games", "games_needed": config.MIN_GAMES_FOR_WITHDRAWAL - user["games_since_withdrawal"]}, status=400)
        new_balance = user["balance"] - amount
        await update_balance_both(user_id, new_balance)
        await sqlite_pool.execute("INSERT INTO transactions (user_id, type, amount, description) VALUES (?,'withdraw',?,'Withdrawal')", (user_id, amount))
        await sqlite_pool.commit()
        for admin_id in config.ADMIN_IDS:
            try: await bot.send_message(admin_id, f"💸 Запрос на вывод\nПользователь: `{user_id}`\nСумма: {amount:.2f}$", parse_mode=ParseMode.MARKDOWN)
            except: pass
        return json_response({"success": True, "new_balance": new_balance})
    except Exception as e: return json_response({"error": str(e)}, status=500)

# ═══════════════════════════════════════
# FSM STATES
# ═══════════════════════════════════════

class SupportStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_reply = State()

class AdminStates(StatesGroup):
    waiting_for_add_balance_user = State()
    waiting_for_add_balance_amount = State()
    waiting_for_sub_balance_user = State()
    waiting_for_sub_balance_amount = State()
    waiting_for_broadcast_message = State()
    waiting_for_reply_target = State()
    waiting_for_reply_message = State()
    waiting_for_clear_confirm = State()

# ═══════════════════════════════════════
# KEYBOARDS
# ═══════════════════════════════════════

def get_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🎡 Играть в рулетку", web_app=WebAppInfo(url=f"{config.FRONTEND_URL}?mode=single"))]],
        resize_keyboard=True, persistent=True
    )

def get_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Начислить", callback_data="admin_add_balance"),
         InlineKeyboardButton(text="💸 Списать", callback_data="admin_sub_balance")],
        [InlineKeyboardButton(text="👥 Список игроков", callback_data="admin_players_list"),
         InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
         InlineKeyboardButton(text="📩 Ответить", callback_data="admin_reply")],
        [InlineKeyboardButton(text="🗑 Очистка БД", callback_data="admin_clear_db"),
         InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_refresh")]
    ])

def get_back_to_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_refresh")]])

# ═══════════════════════════════════════
# SUPPORT ROUTER (ПЕРВЫЙ)
# ═══════════════════════════════════════

support_router = Router()

@support_router.message(F.text == "📩 Поддержка")
async def support_start(message: Message, state: FSMContext):
    await message.answer(
        "📩 *Поддержка*\n\nОпишите вашу проблему или вопрос, и мы ответим в ближайшее время.\n\nДля отмены нажмите /cancel",
        parse_mode=ParseMode.MARKDOWN, reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(SupportStates.waiting_for_message)

@support_router.message(SupportStates.waiting_for_message)
async def support_receive_message(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    msg_text = message.text or message.caption or ""
    if msg_text.startswith("/"):
        await state.clear()
        await message.answer("❌ Обращение отменено", reply_markup=get_main_keyboard())
        return
    try:
        await sqlite_pool.execute("INSERT INTO support_messages (user_id, message) VALUES (?, ?)", (user_id, msg_text))
        await sqlite_pool.commit()
    except:
        if pg_pool:
            try:
                async with pg_pool.acquire() as conn:
                    await conn.execute("INSERT INTO support_messages (user_id, message) VALUES ($1, $2)", user_id, msg_text)
            except Exception as e: logger.error(f"Save support error: {e}")
    await message.answer("✅ Ваше сообщение отправлено! Мы ответим вам в ближайшее время.", reply_markup=get_main_keyboard())
    for admin_id in config.ADMIN_IDS:
        try:
            await bot.send_message(admin_id,
                f"📩 *Новое обращение*\n\nОт: `{user_id}`\nИмя: {message.from_user.full_name}\nUsername: @{message.from_user.username or 'нет'}\n\nСообщение:\n{msg_text}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📝 Ответить", callback_data=f"reply_to_{user_id}")]]))
        except Exception as e: logger.error(f"Notify admin error: {e}")
    await state.clear()

# ═══════════════════════════════════════
# ADMIN ROUTER (ВТОРОЙ)
# ═══════════════════════════════════════

admin_router = Router()

@admin_router.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer("⛔ Доступ запрещён"); return
    await message.answer("🔧 *Админ-панель*\n\nВыберите действие:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

@admin_router.callback_query(F.data == "admin_refresh")
async def admin_refresh(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True); return
    await callback.message.edit_text("🔧 *Админ-панель*\n\nВыберите действие:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())
    await callback.answer()

# ADD BALANCE
@admin_router.callback_query(F.data == "admin_add_balance")
async def admin_add_balance_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True); return
    await callback.message.edit_text("💵 *Начисление баланса*\n\nВведите ID пользователя:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await state.set_state(AdminStates.waiting_for_add_balance_user)
    await callback.answer()

@admin_router.message(AdminStates.waiting_for_add_balance_user)
async def admin_add_balance_get_user(message: Message, state: FSMContext):
    try: target_id = int(message.text.strip())
    except ValueError: await message.answer("❌ Неверный ID. Введите число:"); return
    await state.update_data(target_id=target_id)
    await message.answer(f"💵 *Начисление баланса*\n\nПользователь: `{target_id}`\nВведите сумму для начисления:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await state.set_state(AdminStates.waiting_for_add_balance_amount)

@admin_router.message(AdminStates.waiting_for_add_balance_amount)
async def admin_add_balance_execute(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0: raise ValueError
    except ValueError: await message.answer("❌ Неверная сумма. Введите положительное число:"); return
    data = await state.get_data()
    target_id = data["target_id"]
    user = await get_user(target_id)
    if not user: user = await create_user_if_not_exists(target_id)
    new_balance = user["balance"] + amount
    await update_balance_both(target_id, new_balance)
    await sqlite_pool.execute("INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'deposit', ?, 'Admin deposit')", (target_id, amount))
    await sqlite_pool.commit()
    await message.answer(f"✅ Начислено {amount:.2f}$ пользователю `{target_id}`\nНовый баланс: {new_balance:.2f}$", parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_keyboard())
    await state.clear()

# SUBTRACT BALANCE
@admin_router.callback_query(F.data == "admin_sub_balance")
async def admin_sub_balance_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True); return
    await callback.message.edit_text("💸 *Списание баланса*\n\nВведите ID пользователя:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await state.set_state(AdminStates.waiting_for_sub_balance_user)
    await callback.answer()

@admin_router.message(AdminStates.waiting_for_sub_balance_user)
async def admin_sub_balance_get_user(message: Message, state: FSMContext):
    try: target_id = int(message.text.strip())
    except ValueError: await message.answer("❌ Неверный ID. Введите число:"); return
    user = await get_user(target_id)
    if not user: await message.answer("❌ Пользователь не найден"); return
    await state.update_data(target_id=target_id, current_balance=user["balance"])
    await message.answer(f"💸 *Списание баланса*\n\nПользователь: `{target_id}`\nТекущий баланс: {user['balance']:.2f}$\nВведите сумму для списания:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await state.set_state(AdminStates.waiting_for_sub_balance_amount)

@admin_router.message(AdminStates.waiting_for_sub_balance_amount)
async def admin_sub_balance_execute(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0: raise ValueError
    except ValueError: await message.answer("❌ Неверная сумма. Введите положительное число:"); return
    data = await state.get_data()
    target_id = data["target_id"]
    current_balance = data["current_balance"]
    if amount > current_balance: await message.answer(f"❌ Недостаточно средств. Баланс: {current_balance:.2f}$"); return
    new_balance = current_balance - amount
    await update_balance_both(target_id, new_balance)
    await sqlite_pool.execute("INSERT INTO transactions (user_id, type, amount, description) VALUES (?, 'withdraw', ?, 'Admin withdraw')", (target_id, amount))
    await sqlite_pool.commit()
    await message.answer(f"✅ Списано {amount:.2f}$ у пользователя `{target_id}`\nНовый баланс: {new_balance:.2f}$", parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_keyboard())
    await state.clear()

# PLAYERS LIST
@admin_router.callback_query(F.data == "admin_players_list")
async def admin_players_list(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True); return
    async with sqlite_pool.execute("SELECT COUNT(*) as cnt FROM users") as cursor:
        row = await cursor.fetchone()
        total = row["cnt"] if row else 0
    async with sqlite_pool.execute("SELECT user_id, username, nickname, balance, total_games, total_wins FROM users ORDER BY balance DESC LIMIT 20") as cursor:
        players = await cursor.fetchall()
    if not players:
        await callback.message.edit_text("👥 Список игроков пуст\nВсего игроков: 0", reply_markup=get_back_to_admin_keyboard()); return
    text = f"👥 Список игроков (Топ-20 из {total})\n\n"
    for p in players:
        nick = (p["nickname"] or p["username"] or str(p["user_id"]))[:20]
        nick = nick.replace('_','\\_').replace('*','\\*').replace('`','\\`').replace('[','\\[')
        text += f"• `{p['user_id']}` — {nick}\n  💰 {p['balance']:.2f}$ | 🎮 {p['total_games']} | 🏆 {p['total_wins']}\n"
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await callback.answer()

# STATISTICS
@admin_router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True); return
    async with sqlite_pool.execute("SELECT COUNT(*) as cnt FROM users") as cursor:
        row = await cursor.fetchone(); total_users = row["cnt"] if row else 0
    async with sqlite_pool.execute("SELECT COUNT(*) as cnt FROM game_history") as cursor:
        row = await cursor.fetchone(); total_games = row["cnt"] if row else 0
    async with sqlite_pool.execute("SELECT COALESCE(SUM(bet_amount),0) as total FROM game_history") as cursor:
        row = await cursor.fetchone(); total_bets = row["total"] if row else 0
    async with sqlite_pool.execute("SELECT COALESCE(SUM(win_amount),0) as total FROM game_history") as cursor:
        row = await cursor.fetchone(); total_wins_sum = row["total"] if row else 0
    async with sqlite_pool.execute("SELECT COALESCE(SUM(commission),0) as total FROM multiplayer_rooms") as cursor:
        row = await cursor.fetchone(); total_commission = row["total"] if row else 0
    text = (f"📊 *Статистика*\n\n👤 Всего игроков: {total_users}\n🎮 Всего игр: {total_games}\n💵 Сумма ставок: {total_bets:.2f}$\n🏆 Сумма выигрышей: {total_wins_sum:.2f}$\n🔧 Комиссия: {total_commission:.2f}$\n📈 Профит: {(total_bets - total_wins_sum + total_commission):.2f}$")
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await callback.answer()

# BROADCAST
@admin_router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True); return
    await callback.message.edit_text("📢 *Массовая рассылка*\n\nОтправьте сообщение для рассылки.\nДля отмены: /cancel", parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await state.set_state(AdminStates.waiting_for_broadcast_message)
    await callback.answer()

@admin_router.message(AdminStates.waiting_for_broadcast_message)
async def admin_broadcast_execute(message: Message, state: FSMContext, bot: Bot):
    if message.text and message.text.startswith("/"):
        await state.clear(); await message.answer("❌ Рассылка отменена", reply_markup=get_main_keyboard()); return
    async with sqlite_pool.execute("SELECT user_id FROM users") as cursor:
        users = await cursor.fetchall()
    success = 0; failed = 0
    await message.answer(f"📤 Начинаю рассылку для {len(users)} пользователей...")
    for user in users:
        try:
            await bot.copy_message(chat_id=user["user_id"], from_chat_id=message.chat.id, message_id=message.message_id, reply_markup=get_main_keyboard())
            success += 1; await asyncio.sleep(0.05)
        except Exception as e: failed += 1; logger.error(f"Broadcast error: {e}")
    await message.answer(f"✅ Рассылка завершена!\n\nУспешно: {success}\nНе удалось: {failed}", reply_markup=get_main_keyboard())
    await state.clear()

# REPLY
@admin_router.callback_query(F.data.startswith("reply_to_"))
async def admin_reply_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True); return
    target_id = int(callback.data.split("_")[-1])
    await state.update_data(reply_target=target_id)
    await callback.message.edit_text(f"📝 *Ответ пользователю* `{target_id}`\n\nНапишите сообщение:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await state.set_state(AdminStates.waiting_for_reply_message)
    await callback.answer()

@admin_router.callback_query(F.data == "admin_reply")
async def admin_reply_manual(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True); return
    await callback.message.edit_text("📝 *Ответ пользователю*\n\nВведите ID пользователя:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await state.set_state(AdminStates.waiting_for_reply_target)
    await callback.answer()

@admin_router.message(AdminStates.waiting_for_reply_target)
async def admin_reply_get_target(message: Message, state: FSMContext):
    try: target_id = int(message.text.strip())
    except ValueError: await message.answer("❌ Неверный ID. Введите число:"); return
    await state.update_data(reply_target=target_id)
    await message.answer(f"📝 *Ответ пользователю* `{target_id}`\n\nНапишите сообщение:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await state.set_state(AdminStates.waiting_for_reply_message)

@admin_router.message(AdminStates.waiting_for_reply_message)
async def admin_reply_execute(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    target_id = data["reply_target"]
    try:
        await bot.send_message(target_id, f"📩 *Ответ от поддержки:*\n\n{message.text or ''}", parse_mode=ParseMode.MARKDOWN)
        await message.answer(f"✅ Ответ отправлен пользователю `{target_id}`", parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_keyboard())
        if pg_pool:
            try:
                async with pg_pool.acquire() as conn:
                    await conn.execute("INSERT INTO support_messages (user_id, admin_id, message, is_admin_reply) VALUES ($1,$2,$3,TRUE)", target_id, message.from_user.id, message.text or '')
            except Exception as e: logger.error(f"Save reply error: {e}")
    except Exception as e: await message.answer(f"❌ Ошибка отправки: {e}", reply_markup=get_main_keyboard())
    await state.clear()

# CLEAR DB
@admin_router.callback_query(F.data == "admin_clear_db")
async def admin_clear_db_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True); return
    await callback.message.edit_text("⚠️ *Очистка базы данных*\n\nВы уверены? Будет создан бэкап.\n\nВведите `CONFIRM` для подтверждения:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_back_to_admin_keyboard())
    await state.set_state(AdminStates.waiting_for_clear_confirm)
    await callback.answer()

@admin_router.message(AdminStates.waiting_for_clear_confirm)
async def admin_clear_db_execute(message: Message, state: FSMContext):
    if message.text != "CONFIRM":
        await message.answer("❌ Очистка отменена", reply_markup=get_main_keyboard()); await state.clear(); return
    backup_path = f"database/backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    try:
        shutil.copy2(config.SQLITE_DB_PATH, backup_path)
        await message.answer(f"📦 Бэкап сохранён: {backup_path}")
    except Exception as e: await message.answer(f"❌ Ошибка бэкапа: {e}"); await state.clear(); return
    for table in ["game_history", "multiplayer_players", "multiplayer_rooms", "transactions", "users"]:
        await sqlite_pool.execute(f"DELETE FROM {table}")
    await sqlite_pool.commit()
    await message.answer(f"✅ База данных очищена!\nБэкап: {backup_path}", reply_markup=get_main_keyboard())
    await state.clear()

@admin_router.message(Command("reply"))
async def reply_command(message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer("⛔ Доступ запрещён"); return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: `/reply user_id сообщение`", parse_mode=ParseMode.MARKDOWN); return
    try:
        target_id = int(parts[1]); reply_text = parts[2]
        await message.bot.send_message(target_id, f"📩 *Ответ от поддержки:*\n\n{reply_text}", parse_mode=ParseMode.MARKDOWN)
        await message.answer(f"✅ Ответ отправлен пользователю `{target_id}`", parse_mode=ParseMode.MARKDOWN)
    except ValueError: await message.answer("❌ Неверный ID пользователя")
    except Exception as e: await message.answer(f"❌ Ошибка отправки: {e}")

# ═══════════════════════════════════════
# USER ROUTER (ТРЕТИЙ)
# ═══════════════════════════════════════

user_router = Router()

@user_router.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or ''
    full_name = message.from_user.full_name
    user = await create_user_if_not_exists(user_id, username)

    welcome_text = (
        f"🎡 *Добро пожаловать в 𝑹𝒐𝒖𝒍𝒆𝒕𝒕𝒆, {full_name}!*\n\n"
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

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Пополнить", callback_data="deposit_info"),
         InlineKeyboardButton(text="💸 Вывести", callback_data="withdraw_info")],
        [InlineKeyboardButton(text="💰 Баланс", callback_data="check_balance"),
         InlineKeyboardButton(text="📩 Поддержка", callback_data="support_start")]
    ])

    await message.answer(welcome_text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

@user_router.callback_query(F.data == "deposit_info")
async def deposit_info_cb(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        f"💳 *Пополнение баланса*\n\nДля пополнения используйте CryptoPay (USDT):\n• Отправьте USDT через CryptoBot\n• Укажите ваш ID: `{callback.from_user.id}`\n• Баланс зачислится автоматически",
        parse_mode=ParseMode.MARKDOWN
    )

@user_router.callback_query(F.data == "withdraw_info")
async def withdraw_info_cb(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user: user = await create_user_if_not_exists(callback.from_user.id)
    games_needed = max(0, config.MIN_GAMES_FOR_WITHDRAWAL - user["games_since_withdrawal"])
    if games_needed > 0:
        await callback.message.answer(f"💸 *Вывод средств*\n\n⚠️ Нужно сыграть ещё {games_needed} игр\n💰 Баланс: {user['balance']:.2f}$", parse_mode=ParseMode.MARKDOWN)
    else:
        await callback.message.answer(f"💸 *Вывод средств*\n\n✅ Доступен!\n💰 Баланс: {user['balance']:.2f}$\nНапишите в поддержку для вывода.", parse_mode=ParseMode.MARKDOWN)

@user_router.callback_query(F.data == "check_balance")
async def check_balance_cb(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user: user = await create_user_if_not_exists(callback.from_user.id)
    await callback.message.answer(f"💰 Баланс: {user['balance']:.2f}$\n🎁 Спинов: {user['free_spins']}\n🎮 Игр: {user['total_games']}\n🏆 Побед: {user['total_wins']}")

@user_router.callback_query(F.data == "support_start")
async def support_start_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("📩 *Поддержка*\n\nОпишите вашу проблему, и мы ответим.\nДля отмены: /cancel", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(SupportStates.waiting_for_message)

@user_router.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"🆔 Ваш ID: `{message.from_user.id}`\n👤 Username: @{message.from_user.username or 'нет'}\n📝 Имя: {message.from_user.full_name}", parse_mode=ParseMode.MARKDOWN)

@user_router.message(F.text == "💰 Баланс")
async def show_balance(message: Message):
    user = await get_user(message.from_user.id)
    if not user: user = await create_user_if_not_exists(message.from_user.id, message.from_user.username)
    await message.answer(f"💰 *Ваш баланс*\n\n💵 Баланс: {user['balance']:.2f}$\n🎁 Бесплатных спинов: {user['free_spins']}\n🏆 Побед: {user['total_wins']}\n\n💳 Для пополнения используйте CryptoPay (USDT)\n⚠️ Вывод доступен после {config.MIN_GAMES_FOR_WITHDRAWAL} игр", parse_mode=ParseMode.MARKDOWN)

@user_router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer("❌ Действие отменено", reply_markup=get_main_keyboard())
    else:
        await message.answer("Нет активных действий для отмены")

@user_router.message(F.text == "🎡 Играть в рулетку")
async def play_roulette_button(message: Message):
    await message.answer("🎡 *Рулетка открывается...*", parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎡 Открыть рулетку", web_app=WebAppInfo(url=f"{config.FRONTEND_URL}?mode=single"))]]))

# ═══════════════════════════════════════
# WEBSOCKET SERVER FOR MULTIPLAYER
# ═══════════════════════════════════════

ws_connections: Dict[int, web.WebSocketResponse] = {}

# Комнаты: room_id -> {players, bank, timer, status, timer_task, round_start}
mp_rooms: Dict[str, Dict] = {}

MP_COLORS = ["#FF6B6B","#4ECDC4","#FFEAA7","#DDA0DD","#45B7D1","#96CEB4","#FF8C00","#F7DC6F"]

def get_mp_color(index: int) -> str:
    return MP_COLORS[index % len(MP_COLORS)]

async def broadcast_to_room(room_id: str, message: Dict, exclude_user: int = None):
    room = mp_rooms.get(room_id)
    if not room: return
    for uid in room["players"]:
        if uid != exclude_user and uid in ws_connections:
            ws = ws_connections[uid]
            if not ws.closed:
                try: await ws.send_json(message)
                except: pass

def build_room_state(room_id: str, my_user_id: int = None) -> Dict:
    room = mp_rooms.get(room_id)
    if not room: return {"type":"mp_state","players":[],"bank":0,"timer":0,"my_bet":0,"round_active":False}
    players_list = []
    my_bet = 0.0
    for uid, pdata in room["players"].items():
        players_list.append({
            "user_id": uid, "nickname": pdata["nickname"],
            "bet": pdata["bet"], "color": pdata["color"]
        })
        if my_user_id and uid == my_user_id:
            my_bet = pdata["bet"]
    return {
        "type": "mp_state",
        "room_id": room_id,
        "players": players_list,
        "bank": room["bank"],
        "timer": room["timer"],
        "my_bet": my_bet,
        "round_active": room["status"] == "waiting"
    }

async def save_mp_bet_to_db(room_id: str, user_id: int, bet: float):
    """Сохраняем ставку в БД на случай падения"""
    await sqlite_pool.execute(
        "INSERT INTO multiplayer_players (room_id, user_id, bet_amount, joined_at) VALUES (?,?,?,CURRENT_TIMESTAMP)",
        (room_id, user_id, bet)
    )
    await sqlite_pool.commit()

async def mp_timer_task(room_id: str):
    """Таймер для конкретной комнаты"""
    room = mp_rooms.get(room_id)
    if not room: return
    
    room["round_start"] = time.time()
    room["timer"] = config.MULTIPLAYER_JOIN_TIMEOUT

    while room["timer"] > 0 and room["status"] == "waiting":
        await asyncio.sleep(1)
        if room_id not in mp_rooms: return
        room["timer"] -= 1
        
        if room["timer"] % 5 == 0 or room["timer"] <= 5:
            await broadcast_to_room(room_id, {"type":"mp_timer","time":room["timer"]})

    if room_id not in mp_rooms: return
    if room["status"] == "waiting" and len(room["players"]) >= config.MIN_PLAYERS_MULTIPLAYER:
        await start_mp_game(room_id)
    elif len(room["players"]) < config.MIN_PLAYERS_MULTIPLAYER:
        # Возвращаем ставки
        for uid, pdata in room["players"].items():
            user = await get_user(uid)
            if user:
                await update_balance_both(uid, user["balance"] + pdata["bet"])
        await broadcast_to_room(room_id, {"type":"mp_round_reset"})
        del mp_rooms[room_id]

async def start_mp_game(room_id: str):
    """Запуск игры в комнате"""
    room = mp_rooms.get(room_id)
    if not room or len(room["players"]) < 2: return

    room["status"] = "spinning"
    await broadcast_to_room(room_id, {
        "type":"mp_state","room_id":room_id,
        "players":get_room_players_list(room_id),"bank":room["bank"],
        "timer":0,"round_active":False
    })

    # Выбор победителя
    players_list = list(room["players"].items())
    total = room["bank"]
    weights = [pdata["bet"]/total for _, pdata in players_list]
    winner_id, winner_data = random.choices(players_list, weights=weights, k=1)[0]
    winner_angle = random.random() * 360

    await broadcast_to_room(room_id, {"type":"mp_spinning","winner_angle":winner_angle})
    await asyncio.sleep(5)

    commission = room["bank"] * config.PLATFORM_COMMISSION
    win_amount = room["bank"] - commission

    # Начисление победителю
    winner_user = await get_user(winner_id)
    if winner_user:
        await update_balance_both(winner_id, winner_user["balance"] + win_amount)

    # Сохраняем в БД
    room_db_id = room_id
    await sqlite_pool.execute(
        "INSERT INTO multiplayer_rooms (room_id, status, bank, winner_id, commission, players_count) VALUES (?,'finished',?,?,?,?)",
        (room_db_id, room["bank"], winner_id, commission, len(room["players"]))
    )
    await sqlite_pool.commit()

    await broadcast_to_room(room_id, {
        "type":"mp_result","winner_id":winner_id,
        "winner_nickname":winner_data["nickname"],
        "win_amount":win_amount,"bank":room["bank"],"commission":commission
    })

    await asyncio.sleep(3)
    if room_id in mp_rooms:
        del mp_rooms[room_id]
    await broadcast_to_room(room_id, {"type":"mp_round_reset"})

def get_room_players_list(room_id: str) -> List[Dict]:
    room = mp_rooms.get(room_id)
    if not room: return []
    return [{"user_id":uid,"nickname":p["nickname"],"bet":p["bet"],"color":p["color"]}
            for uid, p in room["players"].items()]

async def handle_websocket(request: Request) -> web.WebSocketResponse:
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
                        user_id = int(data.get("user_id", 0))
                        if user_id:
                            if user_id in ws_connections and not ws_connections[user_id].closed:
                                try: await ws_connections[user_id].close(code=1000)
                                except: pass
                            ws_connections[user_id] = ws
                            await ws.send_json({"type":"connected","user_id":user_id})

                    elif action == "mp_get_rooms":
                        rooms_list = []
                        for rid, room in mp_rooms.items():
                            if room["status"] == "waiting":
                                rooms_list.append({
                                    "room_id": rid,
                                    "players_count": len(room["players"]),
                                    "bank": room["bank"],
                                    "timer": room["timer"]
                                })
                        await ws.send_json({"type":"mp_rooms_list","rooms":rooms_list})

                    elif action == "mp_create_room":
                        user_id = int(data.get("user_id", 0))
                        amount = float(data.get("amount", 0))
                        nickname = str(data.get("nickname", f"Player_{user_id}"))[:15]

                        if amount < 1:
                            await ws.send_json({"type":"error","message":"Минимум 1$"}); continue
                        user = await get_user(user_id)
                        if not user or user["balance"] < amount:
                            await ws.send_json({"type":"error","message":"Недостаточно средств"}); continue

                        # Списываем
                        await update_balance_both(user_id, user["balance"] - amount)

                        room_id = secrets.token_hex(4).upper()
                        mp_rooms[room_id] = {
                            "players": {user_id: {"bet":amount,"nickname":nickname,"color":get_mp_color(0)}},
                            "bank": amount, "timer": config.MULTIPLAYER_JOIN_TIMEOUT,
                            "status": "waiting", "timer_task": None, "round_start": time.time()
                        }
                        current_room = room_id
                        await save_mp_bet_to_db(room_id, user_id, amount)

                        mp_rooms[room_id]["timer_task"] = asyncio.create_task(mp_timer_task(room_id))
                        await ws.send_json(build_room_state(room_id, my_user_id=user_id))
                        logger.info(f"Room {room_id} created by {user_id}")

                    elif action == "mp_join_room":
                        user_id = int(data.get("user_id", 0))
                        room_id = data.get("room_id")
                        amount = float(data.get("amount", 0))
                        nickname = str(data.get("nickname", f"Player_{user_id}"))[:15]

                        room = mp_rooms.get(room_id)
                        if not room:
                            await ws.send_json({"type":"error","message":"Комната не найдена"}); continue
                        if room["status"] != "waiting":
                            await ws.send_json({"type":"error","message":"Раунд уже идёт"}); continue
                        if len(room["players"]) >= config.MAX_PLAYERS_MULTIPLAYER:
                            await ws.send_json({"type":"error","message":"Комната заполнена"}); continue
                        if amount < 1:
                            await ws.send_json({"type":"error","message":"Минимум 1$"}); continue

                        user = await get_user(user_id)
                        if not user or user["balance"] < amount:
                            await ws.send_json({"type":"error","message":"Недостаточно средств"}); continue

                        await update_balance_both(user_id, user["balance"] - amount)
                        color_idx = len(room["players"])
                        room["players"][user_id] = {"bet":amount,"nickname":nickname,"color":get_mp_color(color_idx)}
                        room["bank"] += amount
                        current_room = room_id
                        await save_mp_bet_to_db(room_id, user_id, amount)

                        await broadcast_to_room(room_id, build_room_state(room_id), exclude_user=user_id)
                        await ws.send_json(build_room_state(room_id, my_user_id=user_id))

                    elif action == "mp_raise_bet":
                        user_id = int(data.get("user_id", 0))
                        room_id = data.get("room_id")
                        extra = float(data.get("amount", 0))

                        room = mp_rooms.get(room_id)
                        if not room or user_id not in room["players"]:
                            await ws.send_json({"type":"error","message":"Вы не в игре"}); continue
                        if room["status"] != "waiting":
                            await ws.send_json({"type":"error","message":"Раунд уже идёт"}); continue

                        user = await get_user(user_id)
                        if not user or user["balance"] < extra:
                            await ws.send_json({"type":"error","message":"Недостаточно средств"}); continue

                        await update_balance_both(user_id, user["balance"] - extra)
                        room["players"][user_id]["bet"] += extra
                        room["bank"] += extra

                        if room["timer"] < 10:
                            room["timer"] += 15

                        await broadcast_to_room(room_id, build_room_state(room_id))
                        await ws.send_json(build_room_state(room_id, my_user_id=user_id))

                    elif action == "mp_leave_room":
                        user_id = int(data.get("user_id", 0))
                        room_id = data.get("room_id")
                        room = mp_rooms.get(room_id)
                        if room and user_id in room["players"]:
                            bet = room["players"][user_id]["bet"]
                            user = await get_user(user_id)
                            if user and room["status"] == "waiting":
                                await update_balance_both(user_id, user["balance"] + bet)
                            del room["players"][user_id]
                            room["bank"] -= bet
                            if not room["players"]:
                                if room["timer_task"]: room["timer_task"].cancel()
                                del mp_rooms[room_id]
                            else:
                                await broadcast_to_room(room_id, build_room_state(room_id))
                        current_room = None
                        await ws.send_json({"type":"mp_left_room"})

                    elif action == "ping":
                        await ws.send_json({"type":"pong"})

                except json.JSONDecodeError:
                    await ws.send_json({"type":"error","message":"Invalid JSON"})
                except Exception as e:
                    logger.error(f"WS error: {e}")

    except Exception as e:
        logger.error(f"WS handler error: {e}")
    finally:
        if user_id and user_id in ws_connections:
            del ws_connections[user_id]
        # Возврат ставки при дисконнекте
        if current_room and user_id:
            room = mp_rooms.get(current_room)
            if room and user_id in room["players"] and room["status"] == "waiting":
                bet = room["players"][user_id]["bet"]
                user = await get_user(user_id)
                if user:
                    await update_balance_both(user_id, user["balance"] + bet)
                del room["players"][user_id]
                room["bank"] -= bet
                if not room["players"]:
                    if room["timer_task"]: room["timer_task"].cancel()
                    del mp_rooms[current_room]
                else:
                    await broadcast_to_room(current_room, build_room_state(current_room))

    return ws

# ═══════════════════════════════════════
# BOT SETUP
# ═══════════════════════════════════════

bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher(storage=MemoryStorage())

dp.include_router(support_router)
dp.include_router(admin_router)
dp.include_router(user_router)

async def on_startup():
    logger.info("🚀 Starting LN Roulette Bot...")
    await init_sqlite()
    await init_postgres()

    async def keep_alive():
        while True:
            await asyncio.sleep(240)
            try:
                async with aiohttp.ClientSession() as session:
                    await session.get(f"{config.API_URL}/health")
            except: pass
    asyncio.create_task(keep_alive())

    await bot.set_webhook(url=config.WEBHOOK_URL, allowed_updates=["message","callback_query","inline_query"])
    logger.info(f"✅ Webhook set to {config.WEBHOOK_URL}")
    logger.info("✅ Bot started!")

async def on_shutdown():
    logger.info("🛑 Shutting down...")
    for uid, ws in list(ws_connections.items()):
        if not ws.closed:
            try: await ws.close(code=1001, message="Server shutting down")
            except: pass
    ws_connections.clear()
    mp_rooms.clear()
    await bot.delete_webhook()
    await bot.session.close()
    await close_databases()
    logger.info("✅ Bot stopped")

def create_app() -> web.Application:
    app = web.Application()

    @middleware
    async def global_cors(request: Request, handler):
        if request.method == "OPTIONS": response = Response(status=204)
        else: response = await handler(request)
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        response.headers["Access-Control-Max-Age"] = "3600"
        return response

    app.middlewares.append(global_cors)

    # Webhook ДО того как добавляем свои роуты
    webhook_requests_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_requests_handler.register(app, path=config.WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    # Свои роуты ПОСЛЕ webhook
    async def health_handler(request):
        return web.json_response({"status":"ok","timestamp":datetime.now().isoformat()})

    app.router.add_get("/health", health_handler)
    app.router.add_post("/api/user/get", api_get_user)
    app.router.add_post("/api/game/place_bet", api_place_bet)
    app.router.add_post("/api/user/withdraw", api_withdraw_request)
    app.router.add_get("/ws", handle_websocket)

    return app

if __name__ == "__main__":
    app = create_app()
    logger.info(f"Starting web server on {config.WEBHOOK_HOST}:{config.WEBHOOK_PORT}")
    app.on_startup.append(lambda app: on_startup())
    app.on_shutdown.append(lambda app: on_shutdown())
    web.run_app(app, host=config.WEBHOOK_HOST, port=config.WEBHOOK_PORT, access_log=logger)