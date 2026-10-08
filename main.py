import os
import random
import asyncio
import logging
import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ================= КОНФИГУРАЦИЯ =================
BOT_TOKEN = "8966599826:AAE_DwGBZWRYhuiuc6Jy0X4kTwVeuC3a7jQ"
ADMIN_ID = 7916504148
DB_PATH = "data/db.db"
FILE_PATH = "seed.txt"
# ================================================

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

router = Router()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


class GenerationManager:
    """Класс для управления фоновым процессом генерации"""
    def __init__(self):
        self.is_running = False
        self.task = None
        self.count = 0
        self.words = {}
        self.buffer = []

    async def load_words(self) -> bool:
        """Загрузка слов из БД в оперативную память для максимальной скорости работы"""
        if not os.path.exists(DB_PATH):
            logging.warning(f"Файл БД не найден по пути: {DB_PATH}. Инициализируем тестовую базу данных...")
            await self._init_default_db()

        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT id, word FROM words") as cursor:
                    rows = await cursor.fetchall()
                    self.words = {row[0]: row[1] for row in rows}
                    logging.info(f"Успешно загружено слов из БД: {len(self.words)}")
                    return len(self.words) > 0
        except Exception as e:
            logging.error(f"Ошибка при чтении базы данных: {e}")
            return False

    async def _init_default_db(self):
        """Автоматическое создание структуры и наполнение тестовыми данными (2048 слов) при отсутствии файла БД"""
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS words (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    word TEXT NOT NULL
                )
            """)
            # Заполняем 2048 тестовыми словами, если таблица пуста
            async with db.execute("SELECT COUNT(*) FROM words") as cursor:
                count = (await cursor.fetchone())[0]
                if count == 0:
                    logging.info("Наполнение БД тестовыми 2048 словами...")
                    test_words = [(f"слово_{i}",) for i in range(1, 2049)]
                    await db.executemany("INSERT INTO words (word) VALUES (?)", test_words)
                    await db.commit()

    async def start(self, chat_id: int, status_message_id: int):
        """Запуск асинхронного цикла генерации"""
        if self.is_running:
            return
        
        # Предварительная загрузка слов в ОЗУ
        if not self.words:
            success = await self.load_words()
            if not success:
                await bot.send_message(chat_id, "❌ Не удалось загрузить базу данных слов.")
                return

        self.is_running = True
        self.count = 0
        self.buffer.clear()

        # Очищаем или создаем файл seed.txt
        os.makedirs(os.path.dirname(FILE_PATH) if os.path.dirname(FILE_PATH) else '.', exist_ok=True)
        with open(FILE_PATH, "w", encoding="utf-8") as f:
            f.write("")

        # Запуск задачи в фоновом режиме
        self.task = asyncio.create_task(self._loop(chat_id, status_message_id))

    async def _loop(self, chat_id: int, status_message_id: int):
        last_update_time = asyncio.get_event_loop().time()
        animation_frames = ["|", "/", "-", "\\"]
        frame_idx = 0
        available_ids = list(self.words.keys())

        try:
            while self.is_running:
                # Генерируем 12 случайных ID из доступных в базе
                selected_ids = random.choices(available_ids, k=12)
                phrase_words = [self.words[i] for i in selected_ids]
                
                # Формируем строку: [Слово1,слово2,...,слово12]
                line = "[" + ",".join(phrase_words) + "]\n"
                self.buffer.append(line)
                self.count += 1

                # Сбрасываем буфер на диск каждые 100 записей для снижения нагрузки на I/O
                if len(self.buffer) >= 100:
                    await self._flush_buffer()

                # Обновление сообщения с анимацией раз в 1.5 секунды (ограничение Telegram API)
                current_time = asyncio.get_event_loop().time()
                if current_time - last_update_time >= 1.5:
                    frame = animation_frames[frame_idx % len(animation_frames)]
                    frame_idx += 1
                    
                    keyboard = InlineKeyboardBuilder()
                    keyboard.button(text="🛑 Остановить процесс", callback_data="stop_gen")
                    
                    try:
                        await bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=status_message_id,
                            text=f"⚙️ **Процесс запущен** {frame}\n\nСгенерировано массивов: `{self.count}`",
                            reply_markup=keyboard.as_markup(),
                            parse_mode="Markdown"
                        )
                    except Exception:
                        # Игнорируем ошибки отсутствия изменений в тексте
                        pass
                    last_update_time = current_time

                # Даем возможность другим корутинам выполняться
                await asyncio.sleep(0.001)

        except asyncio.CancelledError:
            pass
        finally:
            # Принудительно сохраняем остатки буфера при выходе
            await self._flush_buffer()
            self.is_running = False

    async def _flush_buffer(self):
        """Запись накопленных строк в файл"""
        if self.buffer:
            # Выполняем в executor, чтобы не блокировать асинхронный поток дисковыми операциями
            await asyncio.to_thread(self._write_to_file, list(self.buffer))
            self.buffer.clear()

    @staticmethod
    def _write_to_file(lines):
        with open(FILE_PATH, "a", encoding="utf-8") as f:
            f.writelines(lines)

    async def stop(self):
        """Остановка фонового процесса"""
        if not self.is_running:
            return
        self.is_running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None


manager = GenerationManager()


# ================= ОБРАБОТЧИКИ СОБЫТИЙ =================

@router.message(Command("start"))
async def cmd_start(message: Message):
    # Проверка на права администратора
    if message.from_user.id != ADMIN_ID:
        logging.warning(f"Попытка доступа постороннего пользователя: ID {message.from_user.id}")
        return

    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🚀 Запустить генерацию", callback_data="start_gen")
    
    await message.answer(
        "👋 Добро пожаловать, Администратор!\n\n"
        "Вы можете управлять процессом непрерывного создания массивов из базы данных.",
        reply_markup=keyboard.as_markup()
    )


@router.callback_query(F.data == "start_gen")
async def handle_start_gen(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Доступ ограничен!", show_alert=True)
        return

    if manager.is_running:
        await callback.answer("Процесс уже запущен!", show_alert=True)
        return

    await callback.answer()
    
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🛑 Остановить процесс", callback_data="stop_gen")
    
    status_msg = await bot.send_message(
        chat_id=callback.message.chat.id,
        text="⏳ Инициализация генератора...",
        reply_markup=keyboard.as_markup()
    )
    
    await manager.start(callback.message.chat.id, status_msg.message_id)


@router.callback_query(F.data == "stop_gen")
async def handle_stop_gen(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Доступ ограничен!", show_alert=True)
        return

    if not manager.is_running:
        await callback.answer("Процесс не запущен.", show_alert=True)
        return

    await callback.answer("Остановка процесса...")
    
    # Останавливаем генерацию
    await manager.stop()
    
    # Проверяем файл и отправляем его администратору
    if os.path.exists(FILE_PATH) and os.path.getsize(FILE_PATH) > 0:
        file_to_send = FSInputFile(FILE_PATH, filename="seed.txt")
        await bot.send_document(
            chat_id=callback.message.chat.id,
            document=file_to_send,
            caption=f"✅ Генерация остановлена.\n📊 Всего получено строк: `{manager.count}`",
            parse_mode="Markdown"
        )
        
        # Обнуляем файл в боте (удаляем физически с диска)
        try:
            os.remove(FILE_PATH)
            logging.info("Файл seed.txt успешно удален и обнулен.")
        except Exception as e:
            logging.error(f"Не удалось удалить файл: {e}")
    else:
        await bot.send_message(
            chat_id=callback.message.chat.id,
            text="⚠️ Процесс завершен, но файл сгенерирован не был или оказался пуст."
        )

    # Обновляем сообщение статуса на завершенное
    await bot.edit_message_text(
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id,
        text=f"🏁 **Процесс завершен**\nФайл отправлен в чат. Сгенерировано строк: `{manager.count}`",
        parse_mode="Markdown"
    )


async def main():
    dp.include_router(router)
    logging.info("Бот запущен и ожидает команд...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен.")
