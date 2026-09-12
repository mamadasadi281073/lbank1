import os
import re
import json
import requests
import tempfile

COMMAND_STATE_FILE = "telegram_command_state.json"


def load_command_state():
    """Load only Telegram polling state, independent of trading state."""
    try:
        with open(COMMAND_STATE_FILE, "r", encoding="utf-8") as f:
            data = __import__("json").load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError, OSError):
        return {}


def save_command_state(state):
    """Persist Telegram polling state atomically so GitHub Actions never sees a partial file."""
    directory = os.path.dirname(os.path.abspath(COMMAND_STATE_FILE)) or "."
    fd, tmp_path = tempfile.mkstemp(prefix="telegram_command_state_", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state or {}, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, COMMAND_STATE_FILE)
    except OSError as error:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        print(f"Could not save Telegram command state: {error}")



import crypto_tradeiq as bot

STATE_FILE = bot.STATE_FILE


def send_reply(chat_id, text):
    return bot.send_telegram_to_target(str(chat_id), text)


def process_commands(command_state):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        print("TELEGRAM_BOT_TOKEN is missing.")
        return

    url = f"https://api.telegram.org/bot{token}/getUpdates"

    # IMPORTANT: on the very first run, do NOT process old Telegram messages.
    # Read only the newest update id and start from the next one. This prevents
    # an old /balance from being answered every 5 minutes after a fresh checkout.
    if "telegram_update_offset" not in command_state:
        try:
            response = requests.get(
                url,
                params={
                    "offset": -1,
                    "limit": 1,
                    "timeout": 1,
                    "allowed_updates": ["message", "callback_query"],
                },
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
            if not data.get("ok"):
                print(f"Telegram initial offset lookup failed: {data}")
                return
            latest = data.get("result", [])
            if latest:
                latest_id = int(latest[-1].get("update_id", 0))
                command_state["telegram_update_offset"] = latest_id + 1
            else:
                command_state["telegram_update_offset"] = 0
            save_command_state(command_state)
            print(f"Telegram command state initialized at offset {command_state['telegram_update_offset']}; old messages skipped.")
            return
        except Exception as error:
            print(f"Telegram initial offset lookup failed: {error}")
            return

    offset = int(command_state.get("telegram_update_offset", 0))

    try:
        response = requests.get(
            url,
            params={
                "offset": offset,
                "timeout": 1,
                "allowed_updates": ["message", "callback_query"],
            },
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            print(f"Telegram getUpdates failed: {data}")
            return
    except Exception as error:
        print(f"Telegram command polling failed: {error}")
        return

    updates = data.get("result", [])
    if not updates:
        return

    # Trading data comes from signal_state_crypto.json through bot.load_state().
    # Telegram polling data stays isolated in telegram_command_state.json.
    trading_state = bot.load_state()

    for update in updates:
        update_id = int(update.get("update_id", 0))
        command_state["telegram_update_offset"] = max(
            int(command_state.get("telegram_update_offset", 0)),
            update_id + 1,
        )
        # Persist immediately. If a later command fails, this update is still
        # acknowledged and will not be executed again on the next GitHub run.
        save_command_state(command_state)

        # Inline-button callback: clicking TP1 reveals TP3 through TP40.
        callback = update.get("callback_query") or {}
        if callback:
            callback_id = str(callback.get("id") or "")
            callback_message = callback.get("message") or {}
            callback_chat = callback_message.get("chat") or {}
            callback_chat_id = callback_chat.get("id")
            callback_data = str(callback.get("data") or "")

            if callback_chat_id is None or not bot.authorized_chat(callback_chat_id):
                continue

            if callback_id:
                try:
                    requests.post(
                        f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                        json={"callback_query_id": callback_id},
                        timeout=15,
                    )
                except Exception as error:
                    print(f"answerCallbackQuery failed: {error}")

            match_tp = re.fullmatch(r"SHOW_TPS:(\d+)", callback_data)
            if match_tp:
                number = int(match_tp.group(1))
                kind, trade = bot.find_trade(trading_state, number)
                if trade is None:
                    send_reply(callback_chat_id, f"❌ معامله شماره #{number} پیدا نشد.\n\n{bot.TELEGRAM_SIGNATURE}")
                elif kind == "active":
                    entry = float(trade.get("entry_price", 0))
                    side = trade.get("side")
                    lines = [
                        f"🎯 اهداف باقی‌مانده معامله #{number}",
                        "",
                    ]
                    for name, percent in bot.tp_levels_for_side(side)[2:]:
                        if percent is None:
                            lines.append(f"⬜ {name}: نامعتبر برای SELL")
                            continue
                        price = bot.calculate_tp_price(entry, side, percent)
                        mark = "✅" if trade.get(bot.tp_hit_key(name), False) else "⬜"
                        lines.append(f"{mark} {name}: {price:.12g} (+{percent:g}%)")
                    lines += ["", bot.TELEGRAM_SIGNATURE]
                    send_reply(callback_chat_id, "\n".join(lines))
                else:
                    entry = float(trade.get("entry_price", 0))
                    side = trade.get("side")
                    lines = [f"🎯 اهداف معامله #{number}", ""]
                    for name, percent in bot.tp_levels_for_side(side)[2:]:
                        if percent is None:
                            lines.append(f"⬜ {name}: نامعتبر برای SELL")
                            continue
                        price = bot.calculate_tp_price(entry, side, percent)
                        lines.append(f"{name}: {price:.12g} (+{percent:g}%)")
                    lines += ["", bot.TELEGRAM_SIGNATURE]
                    send_reply(callback_chat_id, "\n".join(lines))
            continue

        message = update.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        if chat_id is None or not bot.authorized_chat(chat_id):
            continue

        raw = str(message.get("text") or "").strip()
        if not raw:
            continue

        parts = raw.split()
        command = parts[0].lower()
        command = command.split("@", 1)[0]

        try:
            if command in ("/balance", "/bal"):
                portfolio = trading_state.get("portfolio", {})
                balance = float(portfolio.get("balance", bot.STARTING_BALANCE))
                pnl = float(portfolio.get("total_realized_pnl", 0.0))
                active = len(trading_state.get("active", {}))
                text = (
                    "💰 موجودی فعلی\n\n"
                    f"💵 Balance: ${balance:.2f}\n"
                    f"📈 PnL تحقق‌یافته: ${pnl:+.2f}\n"
                    f"📂 معاملات باز: {active}\n\n"
                    f"{bot.TELEGRAM_SIGNATURE}"
                )
                send_reply(chat_id, text)
                continue

            if command in ("/active", "/open"):
                lines = ["📂 معاملات باز", ""]
                active = trading_state.get("active", {})
                if not active:
                    lines.append("هیچ معامله بازی وجود ندارد.")
                else:
                    for trade in active.values():
                        sl = trade.get("current_sl", trade.get("sl"))
                        try:
                            sl_text = f"{float(sl):.12g}"
                        except Exception:
                            sl_text = "-"
                        lines.append(
                            f"#{trade.get('trade_number')} | "
                            f"{trade.get('symbol')} | "
                            f"{trade.get('side')} | "
                            f"SL {sl_text} | "
                            f"TP {trade.get('last_tp_hit') or '-'}"
                        )
                lines += ["", bot.TELEGRAM_SIGNATURE]
                send_reply(chat_id, "\n".join(lines))
                continue

            if command in ("/stats", "/report"):
                portfolio = trading_state.get("portfolio", {})
                trades = trading_state.get("closed_trades", [])
                wins = sum(1 for t in trades if float(t.get("pnl", 0)) > 0)
                losses = sum(1 for t in trades if float(t.get("pnl", 0)) < 0)
                balance = float(portfolio.get("balance", bot.STARTING_BALANCE))
                initial = float(portfolio.get("initial_balance", bot.STARTING_BALANCE))
                pnl = balance - initial
                text = (
                    "📊 آمار کلی\n\n"
                    f"💰 موجودی: ${balance:.2f}\n"
                    f"📈 PnL: ${pnl:+.2f}\n"
                    f"📊 معاملات بسته: {len(trades)}\n"
                    f"✅ برد: {wins}\n"
                    f"❌ باخت: {losses}\n"
                    f"📂 باز: {len(trading_state.get('active', {}))}\n\n"
                    f"{bot.TELEGRAM_SIGNATURE}"
                )
                send_reply(chat_id, text)
                continue

            if command in ("/daily", "/day"):
                text = bot.daily_report_text(trading_state, bot.utc_date())
                send_reply(chat_id, text)
                continue

            if command in ("/weekly", "/week"):
                text = bot.weekly_report_text(trading_state, bot.previous_week_start())
                send_reply(chat_id, text)
                continue

            if command in ("/monthly", "/month"):
                text = bot.monthly_report_text(trading_state, bot.utc_month())
                send_reply(chat_id, text)
                continue

            if command in ("/equity", "/curve"):
                curve = trading_state.get("portfolio", {}).get("equity_curve", [])[-10:]
                lines = ["📈 آخرین نقاط Equity Curve", ""]
                if not curve:
                    lines.append("هنوز داده‌ای ثبت نشده.")
                else:
                    for point in curve:
                        lines.append(
                            f"{point.get('time')} → "
                            f"${float(point.get('balance', 0)):.2f} "
                            f"({point.get('reason', '')})"
                        )
                lines += ["", bot.TELEGRAM_SIGNATURE]
                send_reply(chat_id, "\n".join(lines))
                continue

            if command == "/last":
                trades = trading_state.get("closed_trades", [])
                if not trades:
                    text = "📋 هنوز معامله بسته‌شده‌ای ثبت نشده.\n\n" + bot.TELEGRAM_SIGNATURE
                else:
                    text = bot.trade_details_text(trades[-1], "📋 آخرین معامله")
                send_reply(chat_id, text)
                continue

            # /trade 125, 125, #125
            match = re.fullmatch(
                r"(?:/trade\s*)?#?(\d+)",
                raw,
                flags=re.IGNORECASE,
            )
            if match:
                number = int(match.group(1))
                kind, trade = bot.find_trade(trading_state, number)
                if trade is None:
                    text = f"❌ معامله شماره #{number} پیدا نشد.\n\n{bot.TELEGRAM_SIGNATURE}"
                elif kind == "active":
                    symbol = trade.get("symbol")
                    info = {"symbol": symbol, "name": symbol, "rank": "-"}
                    text = bot.active_trade_details_text(trade, info)
                else:
                    text = bot.trade_details_text(trade)
                send_reply(chat_id, text)
                continue

        except Exception as error:
            print(f"Command failed: {raw} | {error}")
            try:
                send_reply(
                    chat_id,
                    f"❌ خطا در اجرای دستور.\n\n{bot.TELEGRAM_SIGNATURE}",
                )
            except Exception as reply_error:
                print(f"Command error reply failed: {reply_error}")

    save_command_state(command_state)
    print(f"Telegram command offset saved: {command_state.get('telegram_update_offset', 0)}")


def main():
    command_state = load_command_state()
    process_commands(command_state)
    print("Telegram command worker completed.")


if __name__ == "__main__":
    main()
