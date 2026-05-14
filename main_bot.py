#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎡 LN Roulette Bot — Main Application
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
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
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
    ADMIN_IDS = [1167503795, 1670366784]
    SQLITE_DB_PATH = "database/mini_app.db"
    POSTGRES_DSN = os.getenv("DATABASE_URL", "")
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
    FREE_SPIN_EVERY = 10
    MIN_GAMES_FOR_WITHDRAWAL = 2
    ROULETTE_NUMBERS = list(range(37))
    RED_NUMBERS = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}
    BLACK_NUMBERS = {2, 4, 6, 8, 10, 11, 13, 15, 17, 20, 22, 24, 26, 28, 29, 31, 33, 35}
    GREEN_NUMBERS = {0}

config = Config()

# ═══════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════

sqlite_pool: Optional[aiosqlite.Connection] = None
pg_pool: Optional[asyncpg.Pool] = None

def execute_sqlite_sync(func, max_retries=5, delay=0.3):
    """Синхронные операции с SQLite с повторными попытками"""
    for attempt in range(max_retries):
        try:
            return func()
        except (sqlite3.OperationalError, aiosqlite.OperationalError) as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1))
            else:
                raise

async def execute_pg(query: str, *args, fetch_one=False, fetch_all=False, fetch_val=False):
    """Выполнение запроса к PostgreSQL"""
    if not pg_pool:
        return None
    try:
        async with pg_pool.acquire() as conn:
            if fetch_one:
                return await conn.fetchrow(query, *args)
            elif fetch_all:
                return await conn.fetch(query, *args)
            elif fetch_val:
                row = await conn.fetchrow(query, *args)
                return row[0] if row else None
            else:
                return await conn.execute(query, *args)
    except Exception as e:
        logger.error(f"PG error: {e}")
        return None

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
            user_id INTEGER PRIMARY KEY, username TEXT, balance REAL DEFAULT 0.0,
            total_games INTEGER DEFAULT 0, wins INTEGER DEFAULT 0,
            free_spins INTEGER DEFAULT 0, games_since_withdrawal INTEGER DEFAULT 0,
            created_at INTEGER DEFAULT (strftime('%s','now')), updated_at INTEGER DEFAULT (strftime('%s','now'))
        );
        CREATE TABLE IF NOT EXISTS game_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            game_type TEXT NOT NULL, bet_type TEXT, bet_amount REAL DEFAULT 0,
            win_amount REAL DEFAULT 0, result TEXT, number INTEGER,
            played_at INTEGER DEFAULT (strftime('%s','now'))
        );
        CREATE TABLE IF NOT EXISTS multiplayer_rooms (
            room_id TEXT PRIMARY KEY, status TEXT DEFAULT 'waiting', bank REAL DEFAULT 0.0,
            winner_id INTEGER, commission REAL DEFAULT 0.0, players_count INTEGER DEFAULT 0,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        );
        CREATE TABLE IF NOT EXISTS multiplayer_players (
            id INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT, user_id INTEGER,
            bet_amount REAL DEFAULT 0, color TEXT
        );
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, message TEXT,
            created_at INTEGER DEFAULT (strftime('%s','now')), is_read INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, type TEXT,
            amount REAL DEFAULT 0, description TEXT, created_at INTEGER DEFAULT (strftime('%s','now'))
        );
    """)
    await sqlite_pool.commit()
    logger.info("✅ SQLite ready")

async def init_postgres():
    global pg_pool
    if not config.POSTGRES_DSN:
        logger.warning("⚠️ No DATABASE_URL, skipping PostgreSQL")
        return
    try:
        pg_pool = await asyncpg.create_pool(dsn=config.POSTGRES_DSN, min_size=1, max_size=5, command_timeout=30)
        async with pg_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY, username TEXT, balance DECIMAL(20,2) DEFAULT 0,
                    total_games INTEGER DEFAULT 0, wins INTEGER DEFAULT 0,
                    free_spins INTEGER DEFAULT 0, games_since_withdrawal INTEGER DEFAULT 0,
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
                    updated_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
                );
                CREATE TABLE IF NOT EXISTS support_messages (
                    id SERIAL PRIMARY KEY, user_id BIGINT, message TEXT,
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT, is_read INTEGER DEFAULT 0
                );
            """)
        logger.info("✅ PostgreSQL ready")
    except Exception as e:
        logger.warning(f"⚠️ PostgreSQL unavailable: {e}")

async def restore_from_pg():
    """Восстановление данных из PostgreSQL в SQLite при старте"""
    if not pg_pool or not sqlite_pool:
        return
    try:
        rows = await execute_pg("SELECT user_id, username, balance, total_games, wins, free_spins, games_since_withdrawal FROM users", fetch_all=True)
        if rows:
            for r in rows:
                await sqlite_pool.execute(
                    "INSERT OR REPLACE INTO users (user_id, username, balance, total_games, wins, free_spins, games_since_withdrawal, updated_at) VALUES (?,?,?,?,?,?,?,strftime('%s','now'))",
                    (r["user_id"], r["username"], float(r["balance"]), r["total_games"] or 0, r["wins"] or 0, r["free_spins"] or 0, r["games_since_withdrawal"] or 0)
                )
            await sqlite_pool.commit()
            logger.info(f"✅ Restored {len(rows)} users from PostgreSQL")
    except Exception as e:
        logger.warning(f"⚠️ Restore failed: {e}")

async def close_databases():
    if sqlite_pool:
        await sqlite_pool.close()
    if pg_pool:
        await pg_pool.close()

# ═══════════════════════════════════════
# ХЕЛПЕРЫ
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
        now = int(time.time())
        await sqlite_pool.execute(
            "INSERT INTO users (user_id, username, balance, created_at, updated_at) VALUES (?,?,0.0,?,?)",
            (user_id, username or '', now, now)
        )
        await sqlite_pool.commit()
        # Синхронизация с PostgreSQL
        if pg_pool:
            try:
                await execute_pg(
                    "INSERT INTO users (user_id, username, balance, created_at, updated_at) VALUES ($1,$2,0,$3,$4) ON CONFLICT (user_id) DO NOTHING",
                    user_id, username or '', now, now
                )
            except Exception as e:
                logger.error(f"PG insert error: {e}")
        user = await get_user(user_id)
        logger.info(f"👤 New user: {user_id}")
    return user

async def update_balance(user_id: int, new_balance: float):
    now = int(time.time())
    await sqlite_pool.execute("UPDATE users SET balance=?, updated_at=? WHERE user_id=?", (new_balance, now, user_id))
    await sqlite_pool.commit()
    if pg_pool:
        try:
            await execute_pg("UPDATE users SET balance=$1, updated_at=$2 WHERE user_id=$3", new_balance, now, user_id)
        except Exception as e:
            logger.error(f"PG update error: {e}")

async def update_stats(user_id: int, win: bool):
    now = int(time.time())
    await sqlite_pool.execute(
        "UPDATE users SET total_games=total_games+1, wins=wins+?, updated_at=? WHERE user_id=?",
        (1 if win else 0, now, user_id)
    )
    await sqlite_pool.commit()

async def save_game_history(user_id: int, bet_type: str, bet: float, win_amount: float, result: str, number: int):
    now = int(time.time())
    await sqlite_pool.execute(
        "INSERT INTO game_history (user_id, game_type, bet_type, bet_amount, win_amount, result, number, played_at) VALUES (?,'roulette',?,?,?,?,?,?)",
        (user_id, bet_type, bet, win_amount, result, number, now)
    )
    await sqlite_pool.commit()

def get_number_color(number: int) -> str:
    if number in config.RED_NUMBERS: return "red"
    elif number in config.BLACK_NUMBERS: return "black"
    else: return "green"

def generate_roulette_result() -> Tuple[int, str]:
    """Реалистичная рулетка — случайное число 0-36"""
    number = random.randint(0, 36)
    color = get_number_color(number)
    return number, color

def check_win(bet_type: str, number: int, color: str) -> bool:
    if bet_type == "red": return color == "red"
    elif bet_type == "black": return color == "black"
    elif bet_type == "zero": return number == 0
    elif bet_type == "even": return number > 0 and number % 2 == 0
    elif bet_type == "odd": return number > 0 and number % 2 == 1
    elif bet_type.isdigit(): return int(bet_type) == number
    return False

def calculate_win_amount(bet_amount: float, bet_type: str) -> float:
    if bet_type == "zero" or (bet_type.isdigit() and 0 <= int(bet_type) <= 36):
        return bet_amount * 36
    return bet_amount * 2

# ═══════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════

async def api_get_balance(request: Request) -> Response:
    """Получить баланс пользователя"""
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        if not user_id:
            return json_response({"success": False, "error": "user_id required"}, status=400)
        
        user = await get_user(user_id)
        if not user:
            user = await create_user_if_not_exists(user_id)
        
        return json_response({
            "success": True,
            "balance": user["balance"],
            "user_id": user_id,
            "total_games": user.get("total_games", 0),
            "wins": user.get("wins", 0),
            "free_spins": user.get("free_spins", 0),
            "games_since_withdrawal": user.get("games_since_withdrawal", 0)
        })
    except Exception as e:
        logger.error(f"api_get_balance error: {e}")
        return json_response({"success": False, "error": str(e)}, status=500)

async def api_game_result(request: Request) -> Response:
    """
    Приём результата игры.
    Фронтенд отправляет: user_id, bet_type, bet_amount, use_free_spin
    Сервер генерирует число, считает выигрыш, обновляет баланс.
    """
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        bet_type = str(data.get("bet_type", ""))
        bet_amount = float(data.get("bet_amount", 0) or 0)
        use_free_spin = data.get("use_free_spin", False)
        
        if not user_id or not bet_type:
            return json_response({"success": False, "error": "Invalid parameters"}, status=400)
        
        user = await get_user(user_id)
        if not user:
            user = await create_user_if_not_exists(user_id)
        
        is_admin = user_id in config.ADMIN_IDS
        
        # Определяем реальную ставку
        if is_admin:
            actual_bet = bet_amount if bet_amount > 0 else 1
        elif use_free_spin:
            if user["free_spins"] <= 0:
                return json_response({"success": False, "error": "No free spins"}, status=400)
            actual_bet = 0
        else:
            if bet_amount <= 0:
                return json_response({"success": False, "error": "Bet amount required"}, status=400)
            if user["balance"] < bet_amount:
                return json_response({"success": False, "error": "Insufficient balance"}, status=400)
            actual_bet = bet_amount
        
        # Генерируем результат
        number, color = generate_roulette_result()
        is_win = check_win(bet_type, number, color)
        
        # Расчёт выигрыша
        if is_win:
            win_amount = calculate_win_amount(actual_bet, bet_type)
        else:
            win_amount = 0
        
        # Новый баланс
        if is_admin:
            new_balance = user["balance"] + win_amount  # Не списываем
        elif use_free_spin:
            new_balance = user["balance"] + win_amount  # Не списываем
        else:
            new_balance = user["balance"] - actual_bet + win_amount
        
        # Бесплатные спины
        add_free = 1 if (user["total_games"] + 1) % config.FREE_SPIN_EVERY == 0 and not use_free_spin else 0
        new_free_spins = user["free_spins"] + add_free - (1 if use_free_spin else 0)
        
        # Обновляем БД
        await update_balance(user_id, new_balance)
        await update_stats(user_id, is_win)
        await save_game_history(user_id, bet_type, actual_bet, win_amount, 'win' if is_win else 'loss', number)
        
        # Обновляем free_spins
        await sqlite_pool.execute("UPDATE users SET free_spins=? WHERE user_id=?", (new_free_spins, user_id))
        await sqlite_pool.commit()
        
        updated_user = await get_user(user_id)
        
        logger.info(f"🎰 User {user_id}: bet={actual_bet}, number={number}, win={is_win}, amount={win_amount}, balance={new_balance}")
        
        return json_response({
            "success": True,
            "number": number,
            "color": color,
            "is_win": is_win,
            "win_amount": win_amount,
            "new_balance": new_balance,
            "free_spins": new_free_spins,
            "games_played": updated_user["total_games"],
            "games_since_withdrawal": updated_user["games_since_withdrawal"]
        })
    except Exception as e:
        logger.error(f"api_game_result error: {e}")
        return json_response({"success": False, "error": str(e)}, status=500)
    # ═══════════════════════════════════════
# FSM STATES
# ═══════════════════════════════════════

class AdminStates(StatesGroup):
    waiting_for_add_user = State()
    waiting_for_add_amount = State()
    waiting_for_sub_user = State()
    waiting_for_sub_amount = State()
    waiting_for_broadcast = State()

# ═══════════════════════════════════════
# КЛАВИАТУРЫ
# ═══════════════════════════════════════

def get_main_keyboard(user_id: int = None) -> ReplyKeyboardMarkup:
    """Главная клавиатура с WebApp кнопкой"""
    if user_id:
        webapp_url = f"{config.FRONTEND_URL}?mode=single&user_id={user_id}"
    else:
        webapp_url = f"{config.FRONTEND_URL}?mode=single"
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🎡 Играть в рулетку", web_app=WebAppInfo(url=webapp_url))]],
        resize_keyboard=True, persistent=True
    )

def get_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Начислить", callback_data="admin_give"),
         InlineKeyboardButton(text="➖ Списать", callback_data="admin_take")],
        [InlineKeyboardButton(text="👥 Список игроков", callback_data="admin_list")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🗑 Очистить БД", callback_data="admin_clear_db")]
    ])

def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_cancel")]
    ])

def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
    ])

# ═══════════════════════════════════════
# ПОДДЕРЖКА (ПЕРВЫЙ РОУТЕР)
# ═══════════════════════════════════════

support_router = Router()

@support_router.message(F.text == "📩 Поддержка")
async def support_start(message: Message):
    await message.answer(
        "📩 *Поддержка*\n\nОпишите вашу проблему или вопрос одним сообщением.\nАдминистратор ответит вам в ближайшее время.\n\n✏️ Введите ваше сообщение:",
        parse_mode=ParseMode.MARKDOWN
    )

@support_router.message(F.text, ~F.text.startswith("/"))
async def support_receive(message: Message):
    user_id = message.from_user.id
    msg_text = message.text or message.caption or ""
    
    # Сохраняем в SQLite
    await sqlite_pool.execute(
        "INSERT INTO support_messages (user_id, message, created_at) VALUES (?,?,?)",
        (user_id, msg_text, int(time.time()))
    )
    await sqlite_pool.commit()
    
    await message.answer("✅ Ваше сообщение отправлено! Ответ придёт сюда.", reply_markup=get_main_keyboard(message.from_user.id))
    
    # Уведомляем админов
    for admin_id in config.ADMIN_IDS:
        try:
            await message.bot.send_message(
                admin_id,
                f"📩 *Новое обращение*\n\n👤 ID: `{user_id}`\n👤 Username: @{message.from_user.username or 'нет'}\n📝 Сообщение: {msg_text}\n\n💡 Ответить: `/reply {user_id} ваш текст`",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Notify admin error: {e}")

@support_router.message(Command("reply"))
async def reply_command(message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде.")
        return
    
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("❌ Использование: `/reply ID_пользователя текст_ответа`", parse_mode=ParseMode.MARKDOWN)
        return
    
    try:
        target_id = int(parts[1])
        reply_text = parts[2]
    except ValueError:
        await message.answer("❌ ID пользователя должен быть числом.")
        return
    
    try:
        await message.bot.send_message(target_id, f"📨 *Ответ от администратора:*\n\n{reply_text}", parse_mode=ParseMode.MARKDOWN)
        await message.answer(f"✅ Ответ отправлен пользователю `{target_id}`", parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Reply sent to {target_id}")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить ответ: {e}")

# ═══════════════════════════════════════
# АДМИН-ПАНЕЛЬ (ВТОРОЙ РОУТЕР)
# ═══════════════════════════════════════

admin_router = Router()

@admin_router.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer("❌ Доступ запрещён"); return
    await message.answer("👑 *Админ-панель*\n\nВыберите действие:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

@admin_router.callback_query(F.data == "admin_cancel")
async def admin_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.edit_text("👑 *Админ-панель*\n\nВыберите действие:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

@admin_router.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.edit_text("👑 *Админ-панель*\n\nВыберите действие:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

# НАЧИСЛЕНИЕ
@admin_router.callback_query(F.data == "admin_give")
async def admin_give_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    await state.set_state(AdminStates.waiting_for_add_user)
    await callback.message.edit_text("💵 *Начисление*\n\nВведите ID пользователя:", parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_keyboard())

@admin_router.message(AdminStates.waiting_for_add_user, F.text)
async def admin_give_user(message: Message, state: FSMContext):
    try:
        target_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ ID должен быть числом."); return
    await state.update_data(target_id=target_id)
    await state.set_state(AdminStates.waiting_for_add_amount)
    await message.answer(f"💵 *Начисление*\n\nПользователь: `{target_id}`\nВведите сумму:", parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_keyboard())

@admin_router.message(AdminStates.waiting_for_add_amount, F.text)
async def admin_give_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0: raise ValueError
    except ValueError:
        await message.answer("❌ Введите положительное число."); return
    
    data = await state.get_data()
    target_id = data["target_id"]
    user = await get_user(target_id)
    if not user:
        user = await create_user_if_not_exists(target_id)
    
    new_balance = user["balance"] + amount
    await update_balance(target_id, new_balance)
    await state.clear()
    await message.answer(f"✅ Начислено {amount:.2f}$ пользователю `{target_id}`\nНовый баланс: {new_balance:.2f}$", parse_mode=ParseMode.MARKDOWN)
    await message.answer("👑 *Админ-панель*", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

# СПИСАНИЕ
@admin_router.callback_query(F.data == "admin_take")
async def admin_take_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    await state.set_state(AdminStates.waiting_for_sub_user)
    await callback.message.edit_text("💸 *Списание*\n\nВведите ID пользователя:", parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_keyboard())

@admin_router.message(AdminStates.waiting_for_sub_user, F.text)
async def admin_take_user(message: Message, state: FSMContext):
    try:
        target_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ ID должен быть числом."); return
    user = await get_user(target_id)
    if not user:
        await message.answer("❌ Пользователь не найден"); return
    await state.update_data(target_id=target_id, current_balance=user["balance"])
    await state.set_state(AdminStates.waiting_for_sub_amount)
    await message.answer(f"💸 *Списание*\n\nПользователь: `{target_id}`\nТекущий баланс: {user['balance']:.2f}$\nВведите сумму:", parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_keyboard())

@admin_router.message(AdminStates.waiting_for_sub_amount, F.text)
async def admin_take_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0: raise ValueError
    except ValueError:
        await message.answer("❌ Введите положительное число."); return
    
    data = await state.get_data()
    target_id = data["target_id"]
    current_balance = data["current_balance"]
    
    if amount > current_balance:
        await message.answer(f"❌ Недостаточно средств. Баланс: {current_balance:.2f}$"); return
    
    new_balance = current_balance - amount
    await update_balance(target_id, new_balance)
    await state.clear()
    await message.answer(f"✅ Списано {amount:.2f}$ у пользователя `{target_id}`\nНовый баланс: {new_balance:.2f}$", parse_mode=ParseMode.MARKDOWN)
    await message.answer("👑 *Админ-панель*", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

# СПИСОК ИГРОКОВ
@admin_router.callback_query(F.data == "admin_list")
async def admin_list(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    
    cursor = await sqlite_pool.execute("SELECT COUNT(*) as cnt FROM users")
    row = await cursor.fetchone()
    total = row["cnt"] if row else 0
    
    cursor = await sqlite_pool.execute("SELECT user_id, username, balance, total_games, wins FROM users ORDER BY balance DESC LIMIT 20")
    players = await cursor.fetchall()
    
    if not players:
        await callback.message.edit_text(f"👥 Список игроков пуст\nВсего: {total}", reply_markup=back_keyboard())
        return
    
    text = f"👥 *Список игроков* (Всего: {total})\n\n"
    for p in players:
        nick = (p["username"] or str(p["user_id"]))[:20]
        nick = nick.replace('_', '\\_').replace('*', '\\*')
        text += f"• `{p['user_id']}` — {nick}\n  💰 {p['balance']:.2f}$ | 🎮 {p['total_games'] or 0} | 🏆 {p['wins'] or 0}\n"
    
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=back_keyboard())

# СТАТИСТИКА
@admin_router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    
    cursor = await sqlite_pool.execute("SELECT COUNT(*) as cnt FROM users")
    total_users = (await cursor.fetchone())["cnt"] or 0
    
    cursor = await sqlite_pool.execute("SELECT COUNT(*) as cnt FROM game_history")
    total_games = (await cursor.fetchone())["cnt"] or 0
    
    cursor = await sqlite_pool.execute("SELECT COALESCE(SUM(bet_amount),0) as total FROM game_history")
    total_bets = (await cursor.fetchone())["total"] or 0
    
    cursor = await sqlite_pool.execute("SELECT COALESCE(SUM(win_amount),0) as total FROM game_history")
    total_wins = (await cursor.fetchone())["total"] or 0
    
    text = (
        f"📊 *Статистика*\n\n"
        f"👤 Пользователей: {total_users}\n"
        f"🎮 Игр сыграно: {total_games}\n"
        f"💵 Сумма ставок: {total_bets:.2f}$\n"
        f"🏆 Сумма выигрышей: {total_wins:.2f}$\n"
        f"📈 Профит: {(total_bets - total_wins):.2f}$"
    )
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=back_keyboard())

# РАССЫЛКА
@admin_router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    await state.set_state(AdminStates.waiting_for_broadcast)
    await callback.message.edit_text("📢 *Рассылка*\n\nВведите сообщение для отправки всем пользователям:", parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_keyboard())

@admin_router.message(AdminStates.waiting_for_broadcast, F.text)
async def admin_broadcast_execute(message: Message, state: FSMContext):
    text = message.text
    await message.answer("⏳ Начинаю рассылку...")
    
    cursor = await sqlite_pool.execute("SELECT user_id FROM users")
    users = [row["user_id"] for row in await cursor.fetchall()]
    
    success, failed = 0, 0
    for uid in users:
        try:
            await message.bot.send_message(uid, f"📢 *Рассылка*\n\n{text}", parse_mode=ParseMode.MARKDOWN)
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
    
    await state.clear()
    await message.answer(f"✅ Рассылка завершена!\n📨 Успешно: {success}\n❌ Неудачно: {failed}")
    await message.answer("👑 *Админ-панель*", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

# ОЧИСТКА БД
@admin_router.callback_query(F.data == "admin_clear_db")
async def admin_clear_db(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    
    # Создаём бэкап
    backup_path = f"database/backup_{int(time.time())}.db"
    try:
        shutil.copy2(config.SQLITE_DB_PATH, backup_path)
        for table in ["game_history", "multiplayer_players", "multiplayer_rooms", "transactions", "users"]:
            await sqlite_pool.execute(f"DELETE FROM {table}")
        await sqlite_pool.commit()
        await callback.message.edit_text(f"✅ База данных очищена!\n📦 Бэкап: `{backup_path}`", parse_mode=ParseMode.MARKDOWN, reply_markup=back_keyboard())
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка: {e}", reply_markup=back_keyboard())

# ═══════════════════════════════════════
# ПОЛЬЗОВАТЕЛЬСКИЙ РОУТЕР (ТРЕТИЙ)
# ═══════════════════════════════════════

user_router = Router()

@user_router.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or ''
    full_name = message.from_user.full_name
    user = await create_user_if_not_exists(user_id, username)

    welcome_text = (
        f"🎡 *Добро пожаловать в Roulette, {full_name}!*\n\n"
        f"🆔 Ваш ID: `{user_id}`\n"
        f"💰 Баланс: {user['balance']:.2f}$\n"
        f"🎁 Бесплатных спинов: {user['free_spins']}\n\n"
        f"🎯 *Режимы игры:*\n"
        f"• 🎡 Одиночная рулетка — ставки на цвет, число, чёт/нечет\n"
        f"• 👥 Мультиплеер — играйте против других игроков!\n\n"
        f"💎 Выигрывайте до ×36 от ставки!\n"
        f"🎰 Удачной игры!"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Баланс", callback_data="check_balance"),
         InlineKeyboardButton(text="📩 Поддержка", callback_data="support_info")]
    ])

    await message.answer(welcome_text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    # Отправляем reply-клавиатуру отдельно
    await message.answer("🎡 Нажмите кнопку ниже чтобы играть:", reply_markup=get_main_keyboard(user_id))

@user_router.callback_query(F.data == "check_balance")
async def check_balance_cb(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user: user = await create_user_if_not_exists(callback.from_user.id)
    await callback.message.answer(
        f"💰 *Баланс*\n\n💵 {user['balance']:.2f}$\n🎁 Спинов: {user['free_spins']}\n🎮 Игр: {user['total_games']}\n🏆 Побед: {user['wins']}",
        parse_mode=ParseMode.MARKDOWN
    )

@user_router.callback_query(F.data == "support_info")
async def support_info_cb(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "📩 *Поддержка*\n\nОпишите вашу проблему одним сообщением.\nАдминистратор ответит вам в ближайшее время.\n\n✏️ Просто напишите сообщение:",
        parse_mode=ParseMode.MARKDOWN
    )

@user_router.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"🆔 Ваш ID: `{message.from_user.id}`\n👤 Username: @{message.from_user.username or 'нет'}", parse_mode=ParseMode.MARKDOWN)

@user_router.message(F.text == "💰 Баланс")
async def show_balance(message: Message):
    user = await get_user(message.from_user.id)
    if not user: user = await create_user_if_not_exists(message.from_user.id, message.from_user.username)
    await message.answer(f"💰 *Баланс*\n\n💵 {user['balance']:.2f}$\n🎁 Спинов: {user['free_spins']}\n🏆 Побед: {user['wins']}\n\n⚠️ Вывод доступен после {config.MIN_GAMES_FOR_WITHDRAWAL} игр", parse_mode=ParseMode.MARKDOWN)

@user_router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer("❌ Действие отменено", reply_markup=get_main_keyboard(message.from_user.id))
    else:
        await message.answer("Нет активных действий.")

@user_router.message(F.text == "🎡 Играть в рулетку")
async def play_roulette(message: Message):
    await message.answer("🎡 Нажмите кнопку ниже чтобы открыть рулетку:", reply_markup=get_main_keyboard(message.from_user.id))
    # ═══════════════════════════════════════
# WEBSOCKET ДЛЯ МУЛЬТИПЛЕЕРА
# ═══════════════════════════════════════

ws_connections: Dict[int, web.WebSocketResponse] = {}
mp_rooms: Dict[str, Dict] = {}
user_active_rooms: Dict[int, str] = {}

MP_COLORS = ["#FF6B6B","#4ECDC4","#FFEAA7","#DDA0DD","#45B7D1","#96CEB4","#FF8C00","#F7DC6F"]

def get_mp_color(index: int) -> str:
    return MP_COLORS[index % len(MP_COLORS)]

async def send_ws_safe(ws: web.WebSocketResponse, message: Dict):
    try:
        await ws.send_json(message)
    except Exception:
        pass

async def broadcast_to_room(room_id: str, message: Dict, exclude_user: int = None):
    room = mp_rooms.get(room_id)
    if not room: return
    tasks = []
    for uid in room["players"]:
        if uid != exclude_user and uid in ws_connections:
            ws = ws_connections[uid]
            if not ws.closed:
                tasks.append(send_ws_safe(ws, message))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

def build_room_state(room_id: str, my_user_id: int = None) -> Dict:
    room = mp_rooms.get(room_id)
    if not room:
        return {"type":"mp_state","players":[],"bank":0,"timer":0,"my_bet":0,"round_active":False}
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
        "type": "mp_state", "room_id": room_id,
        "players": players_list, "bank": room["bank"],
        "timer": room["timer"], "my_bet": my_bet,
        "round_active": room["status"] == "waiting"
    }

async def return_bet(user_id: int, bet: float):
    user = await get_user(user_id)
    if user:
        await update_balance(user_id, user["balance"] + bet)
        logger.info(f"↩️ Returned {bet}$ to {user_id}")

async def mp_timer_task(room_id: str):
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
        for uid, pdata in room["players"].items():
            await return_bet(uid, pdata["bet"])
        await broadcast_to_room(room_id, {"type":"mp_round_reset"})
        for uid in room["players"]:
            if uid in user_active_rooms: del user_active_rooms[uid]
        del mp_rooms[room_id]

async def start_mp_game(room_id: str):
    room = mp_rooms.get(room_id)
    if not room or len(room["players"]) < 2: return
    
    room["status"] = "spinning"
    await broadcast_to_room(room_id, {
        "type":"mp_state","room_id":room_id,
        "players":get_room_players(room_id),"bank":room["bank"],
        "timer":0,"round_active":False
    })
    
    players_list = list(room["players"].items())
    total = room["bank"]
    weights = [pdata["bet"]/total for _, pdata in players_list]
    winner_id, winner_data = random.choices(players_list, weights=weights, k=1)[0]
    winner_angle = random.random() * 360
    
    await broadcast_to_room(room_id, {"type":"mp_spinning","winner_angle":winner_angle})
    await asyncio.sleep(5)
    
    commission = room["bank"] * config.PLATFORM_COMMISSION
    win_amount = room["bank"] - commission
    
    winner_user = await get_user(winner_id)
    if winner_user:
        await update_balance(winner_id, winner_user["balance"] + win_amount)
    
    await sqlite_pool.execute(
        "INSERT INTO multiplayer_rooms (room_id, status, bank, winner_id, commission, players_count) VALUES (?,'finished',?,?,?,?)",
        (room_id, room["bank"], winner_id, commission, len(room["players"]))
    )
    await sqlite_pool.commit()
    
    logger.info(f"🏆 Room {room_id}: winner={winner_id}, amount={win_amount}")
    
    await broadcast_to_room(room_id, {
        "type":"mp_result","winner_id":winner_id,
        "winner_nickname":winner_data["nickname"],
        "win_amount":win_amount,"bank":room["bank"],"commission":commission
    })
    
    await asyncio.sleep(3)
    for uid in room["players"]:
        if uid in user_active_rooms: del user_active_rooms[uid]
    if room_id in mp_rooms: del mp_rooms[room_id]

async def cleanup_old_rooms():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        to_delete = []
        for room_id, room in mp_rooms.items():
            if now - room.get("round_start", now) > 600:
                to_delete.append(room_id)
        for room_id in to_delete:
            room = mp_rooms[room_id]
            for uid, pdata in room["players"].items():
                await return_bet(uid, pdata["bet"])
                if uid in user_active_rooms: del user_active_rooms[uid]
            if room["timer_task"]: room["timer_task"].cancel()
            del mp_rooms[room_id]
        if to_delete:
            logger.info(f"🗑 Cleaned {len(to_delete)} old rooms")

def get_room_players(room_id: str) -> List[Dict]:
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
                                    "room_id": rid, "players_count": len(room["players"]),
                                    "bank": room["bank"], "timer": room["timer"]
                                })
                        await ws.send_json({"type":"mp_rooms_list","rooms":rooms_list})

                    elif action == "mp_create_room":
                        user_id = int(data.get("user_id", 0))
                        amount = float(data.get("amount", 0))
                        nickname = str(data.get("nickname", f"Player_{user_id}"))[:15]
                        
                        if user_id in user_active_rooms:
                            old = user_active_rooms[user_id]
                            if old in mp_rooms and user_id in mp_rooms[old]["players"]:
                                await ws.send_json({"type":"error","message":"Вы уже в комнате #"+old}); continue
                        if amount < 1:
                            await ws.send_json({"type":"error","message":"Минимум 1$"}); continue
                        
                        user = await get_user(user_id)
                        if not user or user["balance"] < amount:
                            await ws.send_json({"type":"error","message":"Недостаточно средств"}); continue
                        
                        await update_balance(user_id, user["balance"] - amount)
                        room_id = secrets.token_hex(4).upper()
                        mp_rooms[room_id] = {
                            "players": {user_id: {"bet":amount,"nickname":nickname,"color":get_mp_color(0)}},
                            "bank": amount, "timer": config.MULTIPLAYER_JOIN_TIMEOUT,
                            "status": "waiting", "timer_task": None, "round_start": time.time()
                        }
                        current_room = room_id
                        user_active_rooms[user_id] = room_id
                        mp_rooms[room_id]["timer_task"] = asyncio.create_task(mp_timer_task(room_id))
                        await ws.send_json(build_room_state(room_id, my_user_id=user_id))
                        logger.info(f"Room {room_id} created by {user_id}")

                    elif action == "mp_join_room":
                        user_id = int(data.get("user_id", 0))
                        room_id = data.get("room_id")
                        amount = float(data.get("amount", 0))
                        nickname = str(data.get("nickname", f"Player_{user_id}"))[:15]
                        
                        if user_id in user_active_rooms:
                            old = user_active_rooms[user_id]
                            if old in mp_rooms and user_id in mp_rooms[old]["players"]:
                                if old == room_id:
                                    await ws.send_json({"type":"error","message":"Вы уже в этой комнате"}); continue
                                else:
                                    await ws.send_json({"type":"error","message":"Вы уже в комнате #"+old}); continue
                        
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
                        
                        await update_balance(user_id, user["balance"] - amount)
                        color_idx = len(room["players"])
                        room["players"][user_id] = {"bet":amount,"nickname":nickname,"color":get_mp_color(color_idx)}
                        room["bank"] += amount
                        current_room = room_id
                        user_active_rooms[user_id] = room_id
                        await broadcast_to_room(room_id, build_room_state(room_id), exclude_user=user_id)
                        await ws.send_json(build_room_state(room_id, my_user_id=user_id))

                    elif action == "mp_raise_bet":
                        user_id = int(data.get("user_id", 0))
                        room_id = data.get("room_id")
                        extra = float(data.get("amount", 0))
                        
                        room = mp_rooms.get(room_id)
                        if not room:
                            await ws.send_json({"type":"error","message":"Комната уже завершена"}); continue
                        if user_id not in room["players"]:
                            await ws.send_json({"type":"error","message":"Вы не в игре"}); continue
                        if room["status"] != "waiting":
                            await ws.send_json({"type":"error","message":"Раунд уже идёт"}); continue
                        
                        user = await get_user(user_id)
                        if not user or user["balance"] < extra:
                            await ws.send_json({"type":"error","message":"Недостаточно средств"}); continue
                        
                        await update_balance(user_id, user["balance"] - extra)
                        room["players"][user_id]["bet"] += extra
                        room["bank"] += extra
                        if room["timer"] < 10: room["timer"] += 15
                        
                        await broadcast_to_room(room_id, {
                            "type":"mp_player_raised","user_id":user_id,
                            "nickname":room["players"][user_id]["nickname"],
                            "new_bet":room["players"][user_id]["bet"]
                        }, exclude_user=user_id)
                        await broadcast_to_room(room_id, build_room_state(room_id))
                        await ws.send_json(build_room_state(room_id, my_user_id=user_id))

                    elif action == "mp_leave_room":
                        user_id = int(data.get("user_id", 0))
                        room_id = data.get("room_id")
                        room = mp_rooms.get(room_id)
                        if room and user_id in room["players"]:
                            bet = room["players"][user_id]["bet"]
                            if room["status"] == "waiting":
                                await return_bet(user_id, bet)
                            del room["players"][user_id]
                            room["bank"] -= bet
                            if user_id in user_active_rooms: del user_active_rooms[user_id]
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
        if current_room and user_id:
            room = mp_rooms.get(current_room)
            if room and user_id in room["players"] and room["status"] == "waiting":
                bet = room["players"][user_id]["bet"]
                await return_bet(user_id, bet)
                del room["players"][user_id]
                room["bank"] -= bet
                if user_id in user_active_rooms: del user_active_rooms[user_id]
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
    logger.info("🚀 Starting Roulette Bot...")
    await init_sqlite()
    await init_postgres()
    await restore_from_pg()
    asyncio.create_task(cleanup_old_rooms())
    
    async def keep_alive():
        while True:
            await asyncio.sleep(240)
            try:
                async with aiohttp.ClientSession() as session:
                    await session.get(f"{config.API_URL}/health")
            except: pass
    asyncio.create_task(keep_alive())
    
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(3)
    await bot.set_webhook(url=config.WEBHOOK_URL, allowed_updates=["message","callback_query"])
    logger.info(f"✅ Webhook: {config.WEBHOOK_URL}")

async def on_shutdown():
    logger.info("🛑 Shutting down...")
    for room_id, room in list(mp_rooms.items()):
        for uid, pdata in room["players"].items():
            if room["status"] == "waiting":
                await return_bet(uid, pdata["bet"])
    for uid, ws in list(ws_connections.items()):
        if not ws.closed:
            try: await ws.close(code=1001, message="Shutting down")
            except: pass
    ws_connections.clear()
    mp_rooms.clear()
    user_active_rooms.clear()
    await bot.session.close()
    await close_databases()
    logger.info("✅ Bot stopped")

def create_app() -> web.Application:
    app = web.Application()

    @middleware
    async def cors_middleware(request: Request, handler):
        if request.method == "OPTIONS":
            resp = web.Response(status=204)
        else:
            resp = await handler(request)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp
    
    app.middlewares.append(cors_middleware)

    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path=config.WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    async def health(request):
        return web.json_response({"status":"ok"})

    app.router.add_get("/health", health)
    app.router.add_post("/api/get_balance", api_get_balance)
    app.router.add_post("/api/game_result", api_game_result)
    app.router.add_get("/ws", handle_websocket)

    return app

if __name__ == "__main__":
    app = create_app()
    logger.info(f"Starting on {config.WEBHOOK_HOST}:{config.WEBHOOK_PORT}")
    app.on_startup.append(lambda app: on_startup())
    app.on_shutdown.append(lambda app: on_shutdown())
    web.run_app(app, host=config.WEBHOOK_HOST, port=config.WEBHOOK_PORT, access_log=logger)