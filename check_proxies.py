"""
Скрипт для проверки работоспособности Telegram-прокси.
Использует асинхронную проверку TCP-соединений для максимальной скорости.
"""

import asyncio
import urllib.parse
from pathlib import Path
from typing import NamedTuple

DATA_DIR = Path("data")
INPUT_FILE = DATA_DIR / Path("all_proxies.txt")
OUTPUT_FILE = DATA_DIR / Path("working_proxies.txt")
TIMEOUT_SECONDS = 5  # Таймаут на попытку подключения
MAX_CONCURRENT_CHECKS = 100  # Максимальное количество одновременных проверок


class Proxy(NamedTuple):
    """Неизменяемая структура данных для хранения информации о прокси."""

    url: str
    server: str
    port: int


def parse_proxy(line: str) -> Proxy | None:
    """
    Фабричная функция для парсинга и валидации строки прокси.
    Возвращает объект Proxy, если строка корректна, иначе None.
    """
    try:
        parsed = urllib.parse.urlparse(line)
        params = urllib.parse.parse_qs(parsed.query)

        server = params.get("server", [None])[0]
        port_str = params.get("port", [None])[0]

        # Базовая валидация: server и port должны присутствовать
        if not server or not port_str:
            return None

        return Proxy(
            url=line,
            server=server,
            port=int(port_str),  # Вызовет ValueError, если порт не число
        )
    except (ValueError, IndexError, KeyError):
        return None


async def check_proxy(proxy: Proxy, semaphore: asyncio.Semaphore) -> tuple[Proxy, bool]:
    """
    Асинхронно проверяет доступность прокси через TCP-соединение.
    """
    async with semaphore:
        try:
            # Пытаемся установить TCP-соединение с сервером и портом
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(proxy.server, proxy.port),
                timeout=TIMEOUT_SECONDS,
            )
            # Если соединение установлено, сразу закрываем его
            writer.close()
            await writer.wait_closed()
            return proxy, True
        except (asyncio.TimeoutError, OSError, ConnectionRefusedError):
            # Любая ошибка сети означает, что прокси нерабочий
            return proxy, False


async def check_all_proxies(proxies: list[Proxy]) -> list[Proxy]:
    """
    Запускает параллельную проверку всех прокси и возвращает список рабочих.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
    working_proxies: list[Proxy] = []
    total = len(proxies)
    checked = 0

    # Создаем список асинхронных задач
    tasks = [check_proxy(proxy, semaphore) for proxy in proxies]

    # Обрабатываем задачи по мере их завершения
    for coro in asyncio.as_completed(tasks):
        proxy, is_working = await coro
        checked += 1

        if is_working:
            working_proxies.append(proxy)

        # Обновляем прогресс-бар в той же строке консоли
        percentage = (checked / total) * 100 if total > 0 else 0
        print(
            f"\rПроверено: {checked}/{total} ({percentage:.1f}%) | "
            f"Рабочих: {len(working_proxies)}",
            end="",
            flush=True,
        )

    print()  # Перенос строки после завершения прогресс-бара
    return working_proxies


def load_proxies(file_path: Path) -> list[Proxy]:
    """Загружает и парсит прокси из текстового файла."""
    if not file_path.exists():
        print(f"❌ Файл не найден: {file_path}")
        return []

    proxies: list[Proxy] = []
    with file_path.open("r", encoding="utf-8") as file:
        for line_num, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue

            proxy = parse_proxy(line)
            if proxy:
                proxies.append(proxy)
            else:
                print(f"⚠️  Строка {line_num}: некорректный формат прокси")

    return proxies


def save_proxies(proxies: list[Proxy], file_path: Path) -> None:
    """Сохраняет оригинальные URL рабочих прокси в файл."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as file:
        for proxy in proxies:
            print(proxy.url, file=file)


async def main() -> None:
    """Главная точка входа в скрипт."""
    print("=" * 60)
    print("🚀 Проверка работоспособности Telegram-прокси")
    print("=" * 60)
    print(f"📂 Входной файл : {INPUT_FILE}")
    print(f"📂 Выходной файл: {OUTPUT_FILE}")
    print(f"⏱️  Таймаут       : {TIMEOUT_SECONDS} сек.")
    print(f"🔄 Параллельно   : {MAX_CONCURRENT_CHECKS} потоков")
    print("=" * 60)

    proxies = load_proxies(INPUT_FILE)

    if not proxies:
        print("❌ Не найдено корректных прокси для проверки.")
        return

    print(f"\n🔍 Найдено прокси для проверки: {len(proxies)}")
    print("Начинаю проверку...\n")

    working_proxies = await check_all_proxies(proxies)

    print("\n" + "=" * 60)
    print("📊 ИТОГИ:")
    print(f"✅ Рабочих прокси : {len(working_proxies)}")
    print(f"❌ Нерабочих прокси: {len(proxies) - len(working_proxies)}")
    print("=" * 60)

    if working_proxies:
        save_proxies(working_proxies, OUTPUT_FILE)
        print(f"💾 Рабочие прокси успешно сохранены в: {OUTPUT_FILE}")
    else:
        print("⚠️  Нет рабочих прокси для сохранения.")


if __name__ == "__main__":
    # Запускаем асинхронный цикл событий
    asyncio.run(main())
