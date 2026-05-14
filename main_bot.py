#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎡 LN Roulette Bot — Main Application
Mini App с европейской рулеткой и мультиплеерным режимом
v6.0 — All bugs fixed
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
import hmac
import hashlib
import aiohttp
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import sqlite3
import aiosqlite

from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
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
    CRYPTO_PAY_API = "https://pay.crypt.bot/api"
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
    for attempt in range(max_retries):
        try: return func()
        except (sqlite3.OperationalError, aiosqlite.OperationalError) as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1: time.sleep(delay * (attempt + 1))
            else: raise

async def execute_pg(query: str, *args, fetch_one=False, fetch_all=False, fetch_val=False):
    if not pg_pool: return None
    for attempt in range(3):
        try:
            async with pg_pool.acquire() as conn:
                if fetch_one: return await conn.fetchrow(query, *args)
                elif fetch_all: return await conn.fetch(query, *args)
                elif fetch_val:
                    row = await conn.fetchrow(query, *args)
                    return row[0] if row else None
                else: return await conn.execute(query, *args)
        except Exception as e:
            if attempt < 2: await asyncio.sleep(0.5)
            else: logger.error(f"PG error: {e}"); return None

async def init_sqlite():
    global sqlite_pool
    os.makedirs("database", exist_ok=True)
    sqlite_pool = await aiosqlite.connect(config.SQLITE_DB_PATH)
    sqlite_pool.row_factory = aiosqlite.Row
    await sqlite_pool.execute("PRAGMA journal_mode=WAL")
    await sqlite_pool.execute("PRAGMA busy_timeout=5000")
    await sqlite_pool.executescript("""
        CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, balance REAL DEFAULT 0.0, total_games INTEGER DEFAULT 0, wins INTEGER DEFAULT 0, free_spins INTEGER DEFAULT 0, games_since_withdrawal INTEGER DEFAULT 0, created_at INTEGER DEFAULT (strftime('%s','now')), updated_at INTEGER DEFAULT (strftime('%s','now')));
        CREATE TABLE IF NOT EXISTS game_history (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, game_type TEXT NOT NULL, bet_type TEXT, bet_amount REAL DEFAULT 0, win_amount REAL DEFAULT 0, result TEXT, number INTEGER, played_at INTEGER DEFAULT (strftime('%s','now')));
        CREATE TABLE IF NOT EXISTS multiplayer_rooms (room_id TEXT PRIMARY KEY, status TEXT DEFAULT 'waiting', bank REAL DEFAULT 0.0, winner_id INTEGER, commission REAL DEFAULT 0.0, players_count INTEGER DEFAULT 0, created_at INTEGER DEFAULT (strftime('%s','now')));
        CREATE TABLE IF NOT EXISTS multiplayer_players (id INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT, user_id INTEGER, bet_amount REAL DEFAULT 0, color TEXT, nickname TEXT, avatar TEXT);
        CREATE TABLE IF NOT EXISTS support_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, message TEXT, created_at INTEGER DEFAULT (strftime('%s','now')), is_read INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, type TEXT, amount REAL DEFAULT 0, description TEXT, created_at INTEGER DEFAULT (strftime('%s','now')));
        CREATE TABLE IF NOT EXISTS crypto_payments (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, payment_id TEXT UNIQUE, status TEXT DEFAULT 'pending', created_at INTEGER DEFAULT (strftime('%s','now')));
    """)
    await sqlite_pool.commit()
    logger.info("✅ SQLite ready")

async def init_postgres():
    global pg_pool
    if not config.POSTGRES_DSN: logger.warning("⚠️ No DATABASE_URL"); return
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
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
                );
                CREATE TABLE IF NOT EXISTS withdraw_requests (
                    id SERIAL PRIMARY KEY, user_id BIGINT, amount REAL,
                    wallet TEXT, status TEXT DEFAULT 'pending',
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
                );
            """)
            # Миграция: добавляем колонки если их нет
            try:
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS total_games INTEGER DEFAULT 0")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS wins INTEGER DEFAULT 0")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS free_spins INTEGER DEFAULT 0")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS games_since_withdrawal INTEGER DEFAULT 0")
            except Exception as e:
                logger.warning(f"Migration warning: {e}")
        logger.info("✅ PostgreSQL ready")
    except Exception as e: logger.warning(f"⚠️ PG unavailable: {e}")

async def restore_from_pg():
    if not pg_pool or not sqlite_pool: return
    try:
        rows = await execute_pg("SELECT user_id, username, balance, total_games, wins, free_spins, games_since_withdrawal FROM users", fetch_all=True)
        if rows:
            for r in rows:
                await sqlite_pool.execute("INSERT OR REPLACE INTO users (user_id, username, balance, total_games, wins, free_spins, games_since_withdrawal, updated_at) VALUES (?,?,?,?,?,?,?,strftime('%s','now'))", (r["user_id"], r["username"], float(r["balance"]), r["total_games"] or 0, r["wins"] or 0, r["free_spins"] or 0, r["games_since_withdrawal"] or 0))
            await sqlite_pool.commit()
            logger.info(f"✅ Restored {len(rows)} users")
    except Exception as e: logger.warning(f"Restore failed: {e}")

async def close_databases():
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
    except Exception as e: logger.error(f"get_user: {e}"); return None

async def create_user_if_not_exists(user_id: int, username: str = None) -> Dict:
    user = await get_user(user_id)
    if not user:
        now = int(time.time())
        await sqlite_pool.execute("INSERT INTO users (user_id, username, balance, created_at, updated_at) VALUES (?,?,0.0,?,?)", (user_id, username or '', now, now))
        await sqlite_pool.commit()
        if pg_pool:
            await execute_pg("INSERT INTO users (user_id, username, balance, created_at, updated_at) VALUES ($1,$2,0,$3,$4) ON CONFLICT (user_id) DO NOTHING", user_id, username or '', now, now)
        user = await get_user(user_id)
        logger.info(f"👤 New user: {user_id}")
        # Уведомляем админов
        for admin_id in config.ADMIN_IDS:
            try:
                await bot.send_message(admin_id, f"👤 *Новый пользователь!*\n🆔 `{user_id}`\n👤 @{username or 'нет'}", parse_mode=ParseMode.MARKDOWN)
            except: pass
    return user

async def update_balance(user_id: int, new_balance: float):
    now = int(time.time())
    await sqlite_pool.execute("UPDATE users SET balance=?, updated_at=? WHERE user_id=?", (new_balance, now, user_id))
    await sqlite_pool.commit()
    if pg_pool:
        try: await execute_pg("UPDATE users SET balance=$1, updated_at=$2 WHERE user_id=$3", new_balance, now, user_id)
        except Exception as e: logger.error(f"PG balance: {e}")

async def update_stats(user_id: int, win: bool):
    await sqlite_pool.execute("UPDATE users SET total_games=total_games+1, wins=wins+?, updated_at=strftime('%s','now') WHERE user_id=?", (1 if win else 0, user_id))
    await sqlite_pool.commit()

async def save_game_history(user_id: int, bet_type: str, bet: float, win_amount: float, result: str, number: int):
    await sqlite_pool.execute("INSERT INTO game_history (user_id, game_type, bet_type, bet_amount, win_amount, result, number, played_at) VALUES (?,'roulette',?,?,?,?,?,strftime('%s','now'))", (user_id, bet_type, bet, win_amount, result, number))
    await sqlite_pool.commit()

def get_number_color(number: int) -> str:
    if number in config.RED_NUMBERS: return "red"
    elif number in config.BLACK_NUMBERS: return "black"
    else: return "green"

def generate_roulette_result() -> Tuple[int, str]:
    number = random.randint(0, 36)
    return number, get_number_color(number)

def check_win(bet_type: str, number: int, color: str) -> bool:
    if bet_type == "red": return color == "red"
    elif bet_type == "black": return color == "black"
    elif bet_type == "zero": return number == 0
    elif bet_type == "even": return number > 0 and number % 2 == 0
    elif bet_type == "odd": return number > 0 and number % 2 == 1
    elif bet_type.isdigit(): return int(bet_type) == number
    return False

def calculate_win_amount(bet_amount: float, bet_type: str) -> float:
    if bet_type == "zero" or (bet_type.isdigit() and 0 <= int(bet_type) <= 36): return bet_amount * 36
    return bet_amount * 2

# ═══════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════

async def api_get_balance(request: Request) -> Response:
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        if not user_id: return json_response({"success": False, "error": "user_id required"}, status=400)
        user = await get_user(user_id)
        if not user: user = await create_user_if_not_exists(user_id)
        return json_response({"success": True, "balance": user["balance"], "user_id": user_id, "total_games": user.get("total_games", 0), "wins": user.get("wins", 0), "free_spins": user.get("free_spins", 0), "games_since_withdrawal": user.get("games_since_withdrawal", 0)})
    except Exception as e: return json_response({"success": False, "error": str(e)}, status=500)

async def api_game_result(request: Request) -> Response:
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        bet_type = str(data.get("bet_type", ""))
        bet_amount = float(data.get("bet_amount", 0) or 0)
        use_free_spin = data.get("use_free_spin", False)
        if not user_id or not bet_type: return json_response({"success": False, "error": "Invalid parameters"}, status=400)
        
        user = await get_user(user_id)
        if not user: user = await create_user_if_not_exists(user_id)
        
        is_admin = user_id in config.ADMIN_IDS
        if is_admin: actual_bet = bet_amount if bet_amount > 0 else 1
        elif use_free_spin:
            if user["free_spins"] <= 0: return json_response({"success": False, "error": "No free spins"}, status=400)
            actual_bet = 0
        else:
            if bet_amount <= 0: return json_response({"success": False, "error": "Bet amount required"}, status=400)
            if user["balance"] < bet_amount: return json_response({"success": False, "error": "Insufficient balance"}, status=400)
            actual_bet = bet_amount
        
        number, color = generate_roulette_result()
        is_win = check_win(bet_type, number, color)
        win_amount = calculate_win_amount(actual_bet, bet_type) if is_win else 0
        
        if is_admin: new_balance = user["balance"] + win_amount
        elif use_free_spin: new_balance = user["balance"] + win_amount
        else: new_balance = user["balance"] - actual_bet + win_amount
        
        add_free = 1 if (user["total_games"] + 1) % config.FREE_SPIN_EVERY == 0 and not use_free_spin else 0
        new_free_spins = user["free_spins"] + add_free - (1 if use_free_spin else 0)
        
        await update_balance(user_id, new_balance)
        await update_stats(user_id, is_win)
        await save_game_history(user_id, bet_type, actual_bet, win_amount, 'win' if is_win else 'loss', number)
        await sqlite_pool.execute("UPDATE users SET free_spins=? WHERE user_id=?", (new_free_spins, user_id))
        await sqlite_pool.commit()
        
        updated_user = await get_user(user_id)
        logger.info(f"🎰 User {user_id}: bet={actual_bet}, number={number}, win={is_win}, amount={win_amount}")
        return json_response({"success": True, "number": number, "color": color, "is_win": is_win, "win_amount": win_amount, "new_balance": new_balance, "free_spins": new_free_spins, "games_played": updated_user["total_games"], "games_since_withdrawal": updated_user["games_since_withdrawal"]})
    except Exception as e: logger.error(f"api_game_result: {e}"); return json_response({"success": False, "error": str(e)}, status=500)

async def api_create_invoice(request: Request) -> Response:
    """Создание счёта на пополнение через CryptoPay API"""
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        amount = float(data.get("amount", 0))
        
        if not user_id or amount <= 0:
            return json_response({"success": False, "error": "Invalid parameters"}, status=400)
        
        user = await get_user(user_id)
        if not user:
            user = await create_user_if_not_exists(user_id)
        
        # Создаём инвойс через CryptoPay API
        payment_id = secrets.token_hex(16)
        
        async with aiohttp.ClientSession() as session:
            headers = {
                "Crypto-Pay-API-Token": config.CRYPTO_PAY_TOKEN,
                "Content-Type": "application/json"
            }
            payload = {
                "asset": "USDT",
                "amount": str(amount),
                "description": f"Пополнение баланса для user {user_id}",
                "hidden_message": f"user_id:{user_id}",
                "paid_btn_name": "callback",
                "paid_btn_url": f"{config.API_URL}/api/crypto_callback?payment_id={payment_id}",
                "allow_comments": False,
                "allow_anonymous": False
            }
            
            try:
                async with session.post(f"{config.CRYPTO_PAY_API}/createInvoice", json=payload, headers=headers) as resp:
                    result = await resp.json()
                
                if result.get("ok"):
                    invoice = result["result"]
                    
                    # Сохраняем платёж
                    await sqlite_pool.execute(
                        "INSERT INTO crypto_payments (user_id, amount, payment_id, status, created_at) VALUES (?,?,?,'pending',strftime('%s','now'))",
                        (user_id, amount, str(invoice["invoice_id"]))
                    )
                    await sqlite_pool.commit()
                    
                    logger.info(f"💳 Invoice created: user={user_id}, amount={amount}$, invoice_id={invoice['invoice_id']}")
                    
                    return json_response({
                        "success": True,
                        "payment_id": str(invoice["invoice_id"]),
                        "invoice_url": invoice["pay_url"],
                        "amount": amount
                    })
                else:
                    logger.error(f"CryptoPay error: {result}")
                    return json_response({"success": False, "error": "CryptoPay API error"}, status=500)
                    
            except Exception as e:
                logger.error(f"CryptoPay request error: {e}")
                return json_response({"success": False, "error": "Payment service unavailable"}, status=500)
                
    except Exception as e:
        logger.error(f"create_invoice: {e}")
        return json_response({"success": False, "error": str(e)}, status=500)

async def api_check_payment(request: Request) -> Response:
    try:
        data = await request.json()
        payment_id = data.get("payment_id", "")
        if not payment_id: return json_response({"success": False, "error": "payment_id required"}, status=400)
        cursor = await sqlite_pool.execute("SELECT user_id, amount, status FROM crypto_payments WHERE payment_id=?", (payment_id,))
        row = await cursor.fetchone()
        if not row: return json_response({"success": False, "error": "Not found"}, status=404)
        if row["status"] == "paid":
            user = await get_user(row["user_id"])
            return json_response({"success": True, "status": "paid", "amount": row["amount"], "new_balance": user["balance"] if user else 0})
        try:
            async with aiohttp.ClientSession() as session:
                headers = {"Crypto-Pay-API-Token": config.CRYPTO_PAY_TOKEN}
                async with session.get(f"{config.CRYPTO_PAY_API}/getInvoices?invoice_ids={payment_id}", headers=headers) as resp:
                    result = await resp.json()
            if result.get("ok") and result["result"]["items"]:
                invoice = result["result"]["items"][0]
                if invoice["status"] == "paid":
                    paid_amount = float(invoice["amount"])
                    user = await get_user(row["user_id"])
                    if user: await update_balance(row["user_id"], user["balance"] + paid_amount)
                    await sqlite_pool.execute("UPDATE crypto_payments SET status='paid' WHERE payment_id=?", (payment_id,))
                    await sqlite_pool.commit()
                    user = await get_user(row["user_id"])
                    return json_response({"success": True, "status": "paid", "amount": paid_amount, "new_balance": user["balance"] if user else 0})
        except Exception as e: logger.error(f"CryptoPay API: {e}")
        return json_response({"success": True, "status": "pending"})
    except Exception as e: return json_response({"success": False, "error": str(e)}, status=500)

async def api_withdraw(request: Request) -> Response:
    try:
        data = await request.json()
        user_id = int(data.get("user_id", 0))
        amount = float(data.get("amount", 0))
        wallet = str(data.get("wallet", ""))
        if not user_id or amount <= 0: return json_response({"success": False, "error": "Invalid"}, status=400)
        user = await get_user(user_id)
        if not user: return json_response({"success": False, "error": "Not found"}, status=404)
        if user["balance"] < amount: return json_response({"success": False, "error": "Insufficient balance"}, status=400)
        if user["games_since_withdrawal"] < config.MIN_GAMES_FOR_WITHDRAWAL: return json_response({"success": False, "error": "Need more games", "games_needed": config.MIN_GAMES_FOR_WITHDRAWAL - user["games_since_withdrawal"]}, status=400)
        new_balance = user["balance"] - amount
        await update_balance(user_id, new_balance)
        await sqlite_pool.execute("INSERT INTO transactions (user_id, type, amount, description, created_at) VALUES (?,'withdraw',?,?,strftime('%s','now'))", (user_id, amount, f"To {wallet}"))
        await sqlite_pool.commit()
        if pg_pool: await execute_pg("INSERT INTO withdraw_requests (user_id, amount, wallet, status, created_at) VALUES ($1,$2,$3,'pending',EXTRACT(EPOCH FROM NOW())::BIGINT)", user_id, amount, wallet)
        for admin_id in config.ADMIN_IDS:
            try: await bot.send_message(admin_id, f"💸 *Запрос на вывод*\n👤 `{user_id}`\n💰 {amount:.2f}$\n📧 `{wallet}`", parse_mode=ParseMode.MARKDOWN)
            except: pass
        logger.info(f"💸 Withdraw: user={user_id}, amount={amount}")
        return json_response({"success": True, "message": "Withdrawal submitted", "new_balance": new_balance})
    except Exception as e: return json_response({"success": False, "error": str(e)}, status=500)
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
        [InlineKeyboardButton(text="📩 Сообщения", callback_data="admin_messages")],
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

@support_router.message(Command("reply"))
async def reply_command(message: Message):
    """Команда /reply — имеет ПРИОРИТЕТ над всем"""
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer("❌ Нет доступа."); return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("❌ Использование: `/reply ID текст`", parse_mode=ParseMode.MARKDOWN); return
    try:
        target_id = int(parts[1])
        reply_text = parts[2]
        await message.bot.send_message(target_id, f"📨 *Ответ администратора:*\n\n{reply_text}", parse_mode=ParseMode.MARKDOWN)
        await message.answer(f"✅ Отправлено пользователю `{target_id}`", parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Reply from {message.from_user.id} to {target_id}")
    except ValueError: await message.answer("❌ ID должен быть числом.")
    except Exception as e: await message.answer(f"❌ Ошибка: {e}")

@support_router.message(F.text, ~F.text.startswith("/"))
async def support_receive(message: Message, state: FSMContext):
    """Принимаем сообщение ТОЛЬКО если нет активного FSM состояния"""
    # Проверяем — если пользователь в процессе админки, не перехватываем
    current_state = await state.get_state()
    if current_state is not None:
        return  # Пропускаем, пусть обрабатывает админский роутер
    
    user_id = message.from_user.id
    msg_text = message.text or message.caption or ""
    
    # Игнорируем сообщения от админов (они используют /reply)
    if user_id in config.ADMIN_IDS:
        return
    
    await sqlite_pool.execute(
        "INSERT INTO support_messages (user_id, message, created_at) VALUES (?,?,?)",
        (user_id, msg_text, int(time.time()))
    )
    await sqlite_pool.commit()
    
    await message.answer("✅ Ваше сообщение отправлено! Администратор ответит вам в ближайшее время.", reply_markup=get_main_keyboard(user_id))
    
    for admin_id in config.ADMIN_IDS:
        try:
            await message.bot.send_message(
                admin_id,
                f"📩 *Обращение*\n👤 `{user_id}`\n📝 {msg_text[:200]}\n💡 `/reply {user_id} ответ`",
                parse_mode=ParseMode.MARKDOWN
            )
        except: pass

# ═══════════════════════════════════════
# АДМИН-ПАНЕЛЬ (ВТОРОЙ РОУТЕР)
# ═══════════════════════════════════════

admin_router = Router()

@admin_router.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer("❌ Доступ запрещён"); return
    await message.answer("👑 *Админ-панель*\nВыберите действие:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

@admin_router.callback_query(F.data == "admin_cancel")
async def admin_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer(); await state.clear()
    try: await callback.message.edit_text("👑 *Админ-панель*", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())
    except: pass

@admin_router.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery, state: FSMContext):
    await callback.answer(); await state.clear()
    try: await callback.message.edit_text("👑 *Админ-панель*", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())
    except: pass

# НАЧИСЛЕНИЕ
@admin_router.callback_query(F.data == "admin_give")
async def admin_give_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    await state.set_state(AdminStates.waiting_for_add_user)
    await callback.message.edit_text("💵 *Начисление*\nВведите ID пользователя:", parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_keyboard())

@admin_router.message(AdminStates.waiting_for_add_user)
async def admin_give_user(message: Message, state: FSMContext):
    try: target_id = int(message.text.strip())
    except ValueError: await message.answer("❌ Введите число:", reply_markup=cancel_keyboard()); return
    await state.update_data(target_id=target_id)
    await state.set_state(AdminStates.waiting_for_add_amount)
    await message.answer(f"💵 *Начисление*\nПользователь: `{target_id}`\nВведите сумму:", parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_keyboard())

@admin_router.message(AdminStates.waiting_for_add_amount)
async def admin_give_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0: raise ValueError
    except ValueError: await message.answer("❌ Введите положительное число:", reply_markup=cancel_keyboard()); return
    
    data = await state.get_data(); target_id = data["target_id"]
    user = await get_user(target_id)
    if not user: user = await create_user_if_not_exists(target_id)
    new_balance = user["balance"] + amount
    await update_balance(target_id, new_balance)
    
    await sqlite_pool.execute("INSERT INTO transactions (user_id, type, amount, description, created_at) VALUES (?,'deposit',?,'Admin deposit',strftime('%s','now'))", (target_id, amount))
    await sqlite_pool.commit()
    
    logger.info(f"💵 Admin {message.from_user.id} added {amount}$ to {target_id}")
    await state.clear()
    await message.answer(f"✅ Начислено {amount:.2f}$ пользователю `{target_id}`\nНовый баланс: {new_balance:.2f}$", parse_mode=ParseMode.MARKDOWN)
    await message.answer("👑 *Админ-панель*", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

# СПИСАНИЕ
@admin_router.callback_query(F.data == "admin_take")
async def admin_take_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    await state.set_state(AdminStates.waiting_for_sub_user)
    await callback.message.edit_text("💸 *Списание*\nВведите ID пользователя:", parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_keyboard())

@admin_router.message(AdminStates.waiting_for_sub_user)
async def admin_take_user(message: Message, state: FSMContext):
    try: target_id = int(message.text.strip())
    except ValueError: await message.answer("❌ Введите число:", reply_markup=cancel_keyboard()); return
    user = await get_user(target_id)
    if not user: await message.answer("❌ Пользователь не найден", reply_markup=cancel_keyboard()); return
    await state.update_data(target_id=target_id, current_balance=user["balance"])
    await state.set_state(AdminStates.waiting_for_sub_amount)
    await message.answer(f"💸 *Списание*\nПользователь: `{target_id}`\nБаланс: {user['balance']:.2f}$\nВведите сумму:", parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_keyboard())

@admin_router.message(AdminStates.waiting_for_sub_amount)
async def admin_take_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0: raise ValueError
    except ValueError: await message.answer("❌ Введите положительное число:", reply_markup=cancel_keyboard()); return
    
    data = await state.get_data(); target_id = data["target_id"]; cur = data["current_balance"]
    if amount > cur: await message.answer(f"❌ Недостаточно средств. Баланс: {cur:.2f}$", reply_markup=cancel_keyboard()); return
    
    new_balance = cur - amount
    await update_balance(target_id, new_balance)
    
    await sqlite_pool.execute("INSERT INTO transactions (user_id, type, amount, description, created_at) VALUES (?,'withdraw',?,'Admin withdraw',strftime('%s','now'))", (target_id, amount))
    await sqlite_pool.commit()
    
    logger.info(f"💸 Admin {message.from_user.id} removed {amount}$ from {target_id}")
    await state.clear()
    await message.answer(f"✅ Списано {amount:.2f}$ у `{target_id}`\nНовый баланс: {new_balance:.2f}$", parse_mode=ParseMode.MARKDOWN)
    await message.answer("👑 *Админ-панель*", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_keyboard())

# СПИСОК ИГРОКОВ
@admin_router.callback_query(F.data == "admin_list")
async def admin_list(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    
    cursor = await sqlite_pool.execute("SELECT COUNT(*) as cnt FROM users")
    total = (await cursor.fetchone())["cnt"] or 0
    
    cursor = await sqlite_pool.execute("SELECT user_id, username, balance, total_games, wins FROM users ORDER BY balance DESC LIMIT 20")
    players = await cursor.fetchall()
    
    if not players: await callback.message.edit_text(f"👥 Пусто (всего: {total})", reply_markup=back_keyboard()); return
    
    text = f"👥 *Игроки* (всего: {total})\n\n"
    for p in players:
        nick = (p["username"] or str(p["user_id"]))[:20].replace('_','\\_').replace('*','\\*')
        text += f"• `{p['user_id']}` — {nick}\n  💰 {p['balance']:.2f}$ | 🎮 {p['total_games'] or 0} | 🏆 {p['wins'] or 0}\n"
    
    try: await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=back_keyboard())
    except: pass

# СТАТИСТИКА
@admin_router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    
    c1 = await sqlite_pool.execute("SELECT COUNT(*) as cnt FROM users")
    total_users = (await c1.fetchone())["cnt"] or 0
    c2 = await sqlite_pool.execute("SELECT COUNT(*) as cnt FROM game_history")
    total_games = (await c2.fetchone())["cnt"] or 0
    c3 = await sqlite_pool.execute("SELECT COALESCE(SUM(bet_amount),0) as t FROM game_history")
    total_bets = (await c3.fetchone())["t"] or 0
    c4 = await sqlite_pool.execute("SELECT COALESCE(SUM(win_amount),0) as t FROM game_history")
    total_wins = (await c4.fetchone())["t"] or 0
    
    text = f"📊 *Статистика*\n\n👤 {total_users}\n🎮 {total_games}\n💵 Ставок: {total_bets:.2f}$\n🏆 Выигрышей: {total_wins:.2f}$\n📈 Профит: {(total_bets - total_wins):.2f}$"
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=back_keyboard())

@admin_router.callback_query(F.data == "admin_messages")
async def admin_messages(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    
    cursor = await sqlite_pool.execute(
        "SELECT user_id, message, created_at FROM support_messages WHERE is_read=0 ORDER BY created_at DESC LIMIT 15"
    )
    msgs = await cursor.fetchall()
    
    if not msgs:
        await callback.message.edit_text("📩 Нет новых сообщений", reply_markup=back_keyboard())
        return
    
    text = "📩 *Непрочитанные сообщения:*\n\n"
    for m in msgs:
        date_str = datetime.fromtimestamp(m["created_at"]).strftime('%d.%m %H:%M')
        text += f"👤 `{m['user_id']}` | {date_str}\n{m['message'][:100]}\n💡 `/reply {m['user_id']} ответ`\n\n"
    
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=back_keyboard())

# ОЧИСТКА БД
@admin_router.callback_query(F.data == "admin_clear_db")
async def admin_clear_db(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user.id not in config.ADMIN_IDS: return
    
    backup = f"database/backup_{int(time.time())}.db"
    shutil.copy2(config.SQLITE_DB_PATH, backup)
    for t in ["game_history","multiplayer_players","multiplayer_rooms","transactions","users","crypto_payments","support_messages"]:
        await sqlite_pool.execute(f"DELETE FROM {t}")
    await sqlite_pool.commit()
    await callback.message.edit_text(f"✅ Очищено\n📦 `{backup}`", parse_mode=ParseMode.MARKDOWN, reply_markup=back_keyboard())

# ═══════════════════════════════════════
# ПОЛЬЗОВАТЕЛЬСКИЙ РОУТЕР (ТРЕТИЙ)
# ═══════════════════════════════════════

user_router = Router()

@user_router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()  # Сбрасываем любые предыдущие состояния
    user_id = message.from_user.id
    username = message.from_user.username or ''
    full_name = message.from_user.full_name
    user = await create_user_if_not_exists(user_id, username)

    text = (
        f"🎡 *Добро пожаловать, {full_name}!*\n\n"
        f"🆔 `{user_id}`\n💰 {user['balance']:.2f}$\n🎁 Спинов: {user['free_spins']}\n\n"
        f"🎯 Одиночная рулетка\n👥 Мультиплеер\n💎 ×36 от ставки!"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Пополнить", callback_data="deposit_info"),
         InlineKeyboardButton(text="💸 Вывести", callback_data="withdraw_info")],
        [InlineKeyboardButton(text="💰 Баланс", callback_data="check_balance"),
         InlineKeyboardButton(text="📩 Поддержка", callback_data="support_info")]
    ])

    await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    # Отправляем reply-клавиатуру ОТДЕЛЬНЫМ сообщением
    await message.answer("🎡 Нажмите кнопку ниже чтобы играть:", reply_markup=get_main_keyboard(user_id))

@user_router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    if await state.get_state():
        await state.clear()
        await message.answer("❌ Действие отменено", reply_markup=get_main_keyboard(message.from_user.id))
    else:
        await message.answer("Нет активных действий.")

@user_router.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"🆔 `{message.from_user.id}`\n👤 @{message.from_user.username or 'нет'}", parse_mode=ParseMode.MARKDOWN)

@user_router.message(F.text == "💰 Баланс")
async def show_balance(message: Message):
    user = await get_user(message.from_user.id)
    if not user: user = await create_user_if_not_exists(message.from_user.id, message.from_user.username)
    await message.answer(f"💰 {user['balance']:.2f}$ | 🎁 {user['free_spins']} спинов | 🎮 {user['total_games']} игр | 🏆 {user['wins']} побед", parse_mode=ParseMode.MARKDOWN)

@user_router.message(F.text == "🎡 Играть в рулетку")
async def play_roulette(message: Message):
    await message.answer("🎡 Откройте Mini App:", reply_markup=get_main_keyboard(message.from_user.id))

@user_router.callback_query(F.data == "deposit_info")
async def deposit_info(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "💳 *Пополнение*\n\nОткройте Mini App → Профиль → Пополнить\nИли нажмите кнопку ниже:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎡 Открыть Mini App", web_app=WebAppInfo(url=f"{config.FRONTEND_URL}?mode=single&user_id={callback.from_user.id}"))]
        ])
    )

@user_router.callback_query(F.data == "withdraw_info")
async def withdraw_info(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user: user = await create_user_if_not_exists(callback.from_user.id)
    need = max(0, config.MIN_GAMES_FOR_WITHDRAWAL - user["games_since_withdrawal"])
    if need > 0:
        await callback.message.answer(f"💸 *Вывод*\n⚠️ Сыграйте ещё {need} игр\n💰 {user['balance']:.2f}$", parse_mode=ParseMode.MARKDOWN)
    else:
        await callback.message.answer(f"💸 *Вывод*\n✅ Доступен!\n💰 {user['balance']:.2f}$\nОткройте Mini App → Профиль → Вывести", parse_mode=ParseMode.MARKDOWN)

@user_router.callback_query(F.data == "check_balance")
async def check_balance(callback: CallbackQuery):
    await callback.answer()
    user = await get_user(callback.from_user.id)
    if not user: user = await create_user_if_not_exists(callback.from_user.id)
    await callback.message.answer(f"💰 {user['balance']:.2f}$ | 🎁 {user['free_spins']} спинов | 🎮 {user['total_games']} игр | 🏆 {user['wins']} побед")

@user_router.callback_query(F.data == "support_info")
async def support_info(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer("📩 *Поддержка*\n\nПросто напишите ваше сообщение прямо сюда, и администратор ответит вам.\n\nДля отмены: /cancel", parse_mode=ParseMode.MARKDOWN)

# ═══════════════════════════════════════
# WEBSOCKET МУЛЬТИПЛЕЕР
# ═══════════════════════════════════════

ws_connections: Dict[int, web.WebSocketResponse] = {}
mp_rooms: Dict[str, Dict] = {}
user_active_rooms: Dict[int, str] = {}
MP_COLORS = ["#FF6B6B","#4ECDC4","#FFEAA7","#DDA0DD","#45B7D1","#96CEB4","#FF8C00","#F7DC6F"]

def get_mp_color(index: int) -> str: return MP_COLORS[index % len(MP_COLORS)]

async def send_ws_safe(ws: web.WebSocketResponse, message: Dict):
    try: await ws.send_json(message)
    except: pass

async def broadcast_to_room(room_id: str, message: Dict, exclude_user: int = None):
    room = mp_rooms.get(room_id)
    if not room: return
    tasks = [send_ws_safe(ws_connections[uid], message) for uid in room["players"] if uid != exclude_user and uid in ws_connections and not ws_connections[uid].closed]
    if tasks: await asyncio.gather(*tasks, return_exceptions=True)

def build_room_state(room_id: str, my_user_id: int = None) -> Dict:
    room = mp_rooms.get(room_id)
    if not room: return {"type":"mp_state","players":[],"bank":0,"timer":0,"my_bet":0,"round_active":False}
    players_list = [{"user_id":uid,"nickname":p["nickname"],"bet":p["bet"],"color":p["color"],"avatar":p.get("avatar","🎲")} for uid,p in room["players"].items()]
    my_bet = room["players"][my_user_id]["bet"] if my_user_id and my_user_id in room["players"] else 0.0
    return {"type":"mp_state","room_id":room_id,"players":players_list,"bank":room["bank"],"timer":room["timer"],"my_bet":my_bet,"round_active":room["status"]=="waiting"}

async def save_mp_room_to_db(room_id: str):
    room = mp_rooms.get(room_id)
    if not room: return
    await sqlite_pool.execute("INSERT OR REPLACE INTO multiplayer_rooms (room_id, status, bank, players_count, created_at) VALUES (?,?,?,?,strftime('%s','now'))", (room_id, room["status"], room["bank"], len(room["players"])))
    for uid, pdata in room["players"].items():
        await sqlite_pool.execute("INSERT OR REPLACE INTO multiplayer_players (room_id, user_id, bet_amount, color, nickname, avatar) VALUES (?,?,?,?,?,?)", (room_id, uid, pdata["bet"], pdata["color"], pdata["nickname"], pdata.get("avatar","🎲")))
    await sqlite_pool.commit()

async def return_bet(user_id: int, bet: float):
    user = await get_user(user_id)
    if user: await update_balance(user_id, user["balance"] + bet)

async def mp_timer_task(room_id: str):
    room = mp_rooms.get(room_id)
    if not room: return
    room["round_start"] = time.time()
    room["timer"] = config.MULTIPLAYER_JOIN_TIMEOUT
    while room["timer"] > 0 and room["status"] == "waiting":
        await asyncio.sleep(1)
        if room_id not in mp_rooms: return
        room["timer"] -= 1
        if room["timer"] % 5 == 0 or room["timer"] <= 5: await broadcast_to_room(room_id, {"type":"mp_timer","time":room["timer"]})
    if room_id not in mp_rooms: return
    if room["status"] == "waiting" and len(room["players"]) >= config.MIN_PLAYERS_MULTIPLAYER: await start_mp_game(room_id)
    elif len(room["players"]) < config.MIN_PLAYERS_MULTIPLAYER:
        for uid, pdata in room["players"].items(): await return_bet(uid, pdata["bet"])
        await broadcast_to_room(room_id, {"type":"mp_round_reset"})
        for uid in room["players"]:
            if uid in user_active_rooms: del user_active_rooms[uid]
        del mp_rooms[room_id]

async def start_mp_game(room_id: str):
    room = mp_rooms.get(room_id)
    if not room or len(room["players"]) < 2: return
    room["status"] = "spinning"
    await broadcast_to_room(room_id, {"type":"mp_state","room_id":room_id,"players":get_room_players(room_id),"bank":room["bank"],"timer":0,"round_active":False})
    players_list = list(room["players"].items())
    total = room["bank"]
    weights = [pdata["bet"]/total for _, pdata in players_list]
    winner_id, winner_data = random.choices(players_list, weights=weights, k=1)[0]
    await broadcast_to_room(room_id, {"type":"mp_spinning","winner_angle":random.random()*360})
    await asyncio.sleep(5)
    commission = room["bank"] * config.PLATFORM_COMMISSION
    win_amount = room["bank"] - commission
    winner_user = await get_user(winner_id)
    if winner_user: await update_balance(winner_id, winner_user["balance"] + win_amount)
    await sqlite_pool.execute("UPDATE multiplayer_rooms SET status='finished', winner_id=?, commission=? WHERE room_id=?", (winner_id, commission, room_id))
    await sqlite_pool.commit()
    logger.info(f"🏆 Room {room_id}: winner={winner_id}, amount={win_amount}")
    await broadcast_to_room(room_id, {"type":"mp_result","winner_id":winner_id,"winner_nickname":winner_data["nickname"],"win_amount":win_amount,"bank":room["bank"],"commission":commission})
    await asyncio.sleep(3)
    for uid in room["players"]:
        if uid in user_active_rooms: del user_active_rooms[uid]
    if room_id in mp_rooms: del mp_rooms[room_id]

async def cleanup_old_rooms():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        for room_id in list(mp_rooms.keys()):
            room = mp_rooms.get(room_id)
            if room and now - room.get("round_start", now) > 600:
                for uid, pdata in room["players"].items(): await return_bet(uid, pdata["bet"])
                if room.get("timer_task"): room["timer_task"].cancel()
                del mp_rooms[room_id]

def get_room_players(room_id: str) -> List[Dict]:
    room = mp_rooms.get(room_id)
    if not room: return []
    return [{"user_id":uid,"nickname":p["nickname"],"bet":p["bet"],"color":p["color"],"avatar":p.get("avatar","🎲")} for uid,p in room["players"].items()]

async def handle_websocket(request: Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    user_id = None; current_room = None
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
                        rooms_list = [{"room_id":rid,"players_count":len(r["players"]),"bank":r["bank"],"timer":r["timer"]} for rid,r in mp_rooms.items() if r["status"]=="waiting"]
                        await ws.send_json({"type":"mp_rooms_list","rooms":rooms_list})
                    elif action == "mp_create_room":
                        user_id = int(data.get("user_id", 0)); amount = float(data.get("amount", 0))
                        nickname = str(data.get("nickname", f"P{user_id}"))[:15]; avatar = data.get("avatar", "🎲")
                        if user_id in user_active_rooms:
                            old = user_active_rooms[user_id]
                            if old in mp_rooms and user_id in mp_rooms[old]["players"]:
                                await ws.send_json({"type":"error","message":"Вы уже в комнате"}); continue
                        if amount < 1: await ws.send_json({"type":"error","message":"Минимум 1$"}); continue
                        user = await get_user(user_id)
                        if not user or user["balance"] < amount: await ws.send_json({"type":"error","message":"Недостаточно средств"}); continue
                        await update_balance(user_id, user["balance"] - amount)
                        room_id = secrets.token_hex(4).upper()
                        mp_rooms[room_id] = {"players": {user_id: {"bet":amount,"nickname":nickname,"color":get_mp_color(0),"avatar":avatar}}, "bank": amount, "timer": config.MULTIPLAYER_JOIN_TIMEOUT, "status": "waiting", "timer_task": None, "round_start": time.time()}
                        current_room = room_id; user_active_rooms[user_id] = room_id
                        await save_mp_room_to_db(room_id)
                        mp_rooms[room_id]["timer_task"] = asyncio.create_task(mp_timer_task(room_id))
                        await ws.send_json(build_room_state(room_id, user_id))
                        logger.info(f"Room {room_id} by {user_id}")
                    elif action == "mp_join_room":
                        user_id = int(data.get("user_id", 0)); room_id = data.get("room_id"); amount = float(data.get("amount", 0))
                        nickname = str(data.get("nickname", f"P{user_id}"))[:15]; avatar = data.get("avatar", "🎲")
                        if user_id in user_active_rooms: await ws.send_json({"type":"error","message":"Вы уже в комнате"}); continue
                        room = mp_rooms.get(room_id)
                        if not room: await ws.send_json({"type":"error","message":"Комната не найдена"}); continue
                        if room["status"]!="waiting": await ws.send_json({"type":"error","message":"Игра идёт"}); continue
                        if len(room["players"])>=config.MAX_PLAYERS_MULTIPLAYER: await ws.send_json({"type":"error","message":"Заполнена"}); continue
                        if amount<1: await ws.send_json({"type":"error","message":"Минимум 1$"}); continue
                        user = await get_user(user_id)
                        if not user or user["balance"]<amount: await ws.send_json({"type":"error","message":"Недостаточно средств"}); continue
                        await update_balance(user_id, user["balance"]-amount)
                        color_idx = len(room["players"])
                        room["players"][user_id] = {"bet":amount,"nickname":nickname,"color":get_mp_color(color_idx),"avatar":avatar}
                        room["bank"] += amount; current_room = room_id; user_active_rooms[user_id] = room_id
                        await save_mp_room_to_db(room_id)
                        await broadcast_to_room(room_id, {"type":"mp_player_joined","user_id":user_id,"nickname":nickname})
                        await broadcast_to_room(room_id, build_room_state(room_id), exclude_user=user_id)
                        await ws.send_json(build_room_state(room_id, user_id))
                    elif action == "mp_raise_bet":
                        user_id = int(data.get("user_id", 0)); room_id = data.get("room_id"); extra = float(data.get("amount", 0))
                        room = mp_rooms.get(room_id)
                        if not room: await ws.send_json({"type":"error","message":"Комната завершена"}); continue
                        if room["status"]!="waiting": await ws.send_json({"type":"error","message":"Игра идёт"}); continue
                        user = await get_user(user_id)
                        if not user or user["balance"]<extra: await ws.send_json({"type":"error","message":"Недостаточно средств"}); continue
                        await update_balance(user_id, user["balance"]-extra)
                        room["players"][user_id]["bet"] += extra; room["bank"] += extra
                        if room["timer"]<10: room["timer"] += 15
                        await broadcast_to_room(room_id, {"type":"mp_player_raised","user_id":user_id,"nickname":room["players"][user_id]["nickname"]}, exclude_user=user_id)
                        await broadcast_to_room(room_id, build_room_state(room_id))
                        await ws.send_json(build_room_state(room_id, user_id))
                    elif action == "mp_leave_room":
                        user_id = int(data.get("user_id", 0)); room_id = data.get("room_id")
                        room = mp_rooms.get(room_id)
                        if room and user_id in room["players"]:
                            if room["status"]=="waiting": await return_bet(user_id, room["players"][user_id]["bet"])
                            del room["players"][user_id]
                            room["bank"] -= sum(p["bet"] for p in room["players"].values()) if room["players"] else 0
                            if user_id in user_active_rooms: del user_active_rooms[user_id]
                            if not room["players"]:
                                if room.get("timer_task"): room["timer_task"].cancel()
                                del mp_rooms[room_id]
                            else: await broadcast_to_room(room_id, build_room_state(room_id))
                        current_room = None
                        await ws.send_json({"type":"mp_left_room"})
                    elif action == "ping": await ws.send_json({"type":"pong"})
                except Exception as e: logger.error(f"WS error: {e}")
    except Exception as e: logger.error(f"WS handler: {e}")
    finally:
        if user_id and user_id in ws_connections: del ws_connections[user_id]
        if current_room and user_id:
            room = mp_rooms.get(current_room)
            if room and user_id in room["players"] and room["status"]=="waiting":
                await return_bet(user_id, room["players"][user_id]["bet"])
                del room["players"][user_id]
                if not room["players"]:
                    if room.get("timer_task"): room["timer_task"].cancel()
                    del mp_rooms[current_room]
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
    logger.info("🚀 Starting...")
    await init_sqlite()
    await init_postgres()
    await restore_from_pg()
    asyncio.create_task(cleanup_old_rooms())
    async def keep_alive():
        while True:
            await asyncio.sleep(240)
            try:
                async with aiohttp.ClientSession() as session: await session.get(f"{config.API_URL}/health")
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
            if room["status"]=="waiting": await return_bet(uid, pdata["bet"])
    for ws in ws_connections.values():
        if not ws.closed:
            try: await ws.close(code=1001)
            except: pass
    ws_connections.clear(); mp_rooms.clear(); user_active_rooms.clear()
    await bot.session.close(); await close_databases()
    logger.info("✅ Stopped")

def create_app() -> web.Application:
    app = web.Application()
    @middleware
    async def cors_middleware(request: Request, handler):
        resp = web.Response(status=204) if request.method=="OPTIONS" else await handler(request)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp
    app.middlewares.append(cors_middleware)
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path=config.WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    app.router.add_get("/health", lambda r: web.json_response({"status":"ok"}))
    app.router.add_post("/api/get_balance", api_get_balance)
    app.router.add_post("/api/game_result", api_game_result)
    app.router.add_post("/api/create_invoice", api_create_invoice)
    app.router.add_post("/api/check_payment", api_check_payment)
    app.router.add_post("/api/withdraw", api_withdraw)
    app.router.add_get("/ws", handle_websocket)
    return app

if __name__ == "__main__":
    app = create_app()
    logger.info(f"Port: {config.WEBHOOK_PORT}")
    app.on_startup.append(lambda app: on_startup())
    app.on_shutdown.append(lambda app: on_shutdown())
    web.run_app(app, host=config.WEBHOOK_HOST, port=config.WEBHOOK_PORT, access_log=logger)