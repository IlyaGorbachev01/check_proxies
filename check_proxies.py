"""
Скрипт для проверки работоспособности Telegram-прокси.
Архитектура разделена на: UI (вывод), Business Logic (логика) и Orchestration (main).
"""

import argparse
import asyncio
import urllib.parse
import urllib.request
from enum import Enum
from pathlib import Path
from typing import Callable, NamedTuple

# ==================== КОНСТАНТЫ ====================
DATA_DIR = Path("data")
DEFAULT_INPUT_FILE = DATA_DIR / "all_proxies.txt"
DEFAULT_OUTPUT_FILE = DATA_DIR / "working_proxies.txt"

REMOTE_PROXY_LIST_URL = (
    "https://raw.githubusercontent.com/SoliSpirit/mtproto/master/all_proxies.txt"
)

DEFAULT_TIMEOUT = 5
DEFAULT_WORKERS = 100
# ===================================================


class Proxy(NamedTuple):
    """Неизменяемая структура данных прокси."""

    url: str
    server: str
    port: int
    secret: str | None = None


class LogLevel(Enum):
    """Уровни логирования для единообразного вывода."""

    INFO = "ℹ️ "
    SUCCESS = "✅ "
    WARNING = "⚠️ "
    ERROR = "❌ "


class Console:
    """Централизованный класс для форматированного вывода в консоль."""

    @staticmethod
    def log(msg: str, level: LogLevel = LogLevel.INFO) -> None:
        """Выводит сообщение с соответствующей пиктограммой."""
        print(f"{level.value}{msg}")

    @staticmethod
    def header(title: str) -> None:
        print(f"\n{'=' * 60}\n {title}\n{'=' * 60}")

    @staticmethod
    def progress(current: int, total: int, working: int) -> None:
        """Обновляет строку прогресса без переноса."""
        pct = (current / total * 100) if total > 0 else 0
        print(
            f"\r🔄 Проверка: {current}/{total} ({pct:.1f}%) | Найдено рабочих: {working}",
            end="",
            flush=True,
        )

    @staticmethod
    def finish_progress() -> None:
        """Завершает строку прогресса переносом."""
        print()


# ==================== БИЗНЕС-ЛОГИКА ====================


def parse_proxy(line: str) -> Proxy | None:
    """Парсит и валидирует строку прокси."""
    try:
        parsed = urllib.parse.urlparse(line)
        params = urllib.parse.parse_qs(parsed.query)

        server = params.get("server", [None])[0]
        port_str = params.get("port", [None])[0]
        secret = params.get("secret", [None])[0]

        if not server or not port_str:
            return None

        return Proxy(url=line, server=server, port=int(port_str), secret=secret)
    except (ValueError, IndexError, KeyError):
        return None


def download_proxies(url: str, destination: Path) -> int:
    """Скачивает файл и возвращает количество строк. Выбрасывает исключение при ошибке."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=30) as response:
        content = response.read()

    destination.write_bytes(content)
    return content.decode("utf-8").count("\n") + 1


def load_proxies_from_file(file_path: Path) -> tuple[list[Proxy], int]:
    """Загружает прокси из файла. Возвращает (список прокси, количество ошибок)."""
    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    proxies = []
    invalid_count = 0

    with file_path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            proxy = parse_proxy(line)
            if proxy:
                proxies.append(proxy)
            else:
                invalid_count += 1

    return proxies, invalid_count


async def check_proxy(
    proxy: Proxy, semaphore: asyncio.Semaphore, timeout: int
) -> tuple[Proxy, bool]:
    """Проверяет одно TCP-соединение."""
    async with semaphore:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(proxy.server, proxy.port),
                timeout=timeout,
            )
            writer.close()
            await writer.wait_closed()
            return proxy, True
        except (asyncio.TimeoutError, OSError, ConnectionRefusedError):
            return proxy, False


async def check_all_proxies(
    proxies: list[Proxy],
    max_workers: int,
    timeout: int,
    progress_callback: Callable[[int, int, int], None],
) -> list[Proxy]:
    """Проверяет все прокси и вызывает callback для обновления прогресса."""
    semaphore = asyncio.Semaphore(max_workers)
    working_proxies = []
    total = len(proxies)

    tasks = [check_proxy(p, semaphore, timeout) for p in proxies]

    for checked, coro in enumerate(asyncio.as_completed(tasks), start=1):
        proxy, is_working = await coro
        if is_working:
            working_proxies.append(proxy)

        progress_callback(checked, total, len(working_proxies))

    Console.finish_progress()
    return working_proxies


def save_proxies(proxies: list[Proxy], file_path: Path) -> None:
    """Сохраняет рабочие прокси в файл."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as file:
        for proxy in proxies:
            file.write(f"{proxy.url}\n")


# ==================== ОРКЕСТРАЦИЯ ====================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Быстрая проверка работоспособности Telegram-прокси",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Примеры:\n"
        "  python check_proxies.py --refresh\n"
        "  python check_proxies.py --timeout 10 --workers 50\n"
        "  python check_proxies.py --input custom.txt",
    )
    parser.add_argument(
        "--refresh", action="store_true", help="Принудительно обновить список с GitHub"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Таймаут в сек. (по умолч.: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Параллельных проверок (по умолч.: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_INPUT_FILE, help="Входной файл"
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT_FILE, help="Выходной файл"
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    Console.header("🚀 Telegram Proxy Checker")
    Console.log(f"Входной файл : {args.input}")
    Console.log(f"Выходной файл: {args.output}")
    Console.log(f"Таймаут      : {args.timeout} сек. | Потоков: {args.workers}")

    # 1. Получение файла
    try:
        if args.input.exists() and not args.refresh:
            Console.log(f"Локальный файл найден: {args.input.name}", LogLevel.SUCCESS)
        else:
            if args.refresh and args.input.exists():
                Console.log(
                    "Принудительное обновление: старый файл удалён", LogLevel.WARNING
                )
                args.input.unlink()

            Console.log("Скачивание актуального списка с GitHub...", LogLevel.INFO)
            lines_count = download_proxies(REMOTE_PROXY_LIST_URL, args.input)
            Console.log(
                f"Скачано {lines_count} строк в {args.input.name}", LogLevel.SUCCESS
            )
    except Exception as e:
        Console.log(f"Не удалось получить файл: {e}", LogLevel.ERROR)
        return

    # 2. Загрузка и парсинг
    try:
        proxies, invalid_count = load_proxies_from_file(args.input)
        if invalid_count > 0:
            Console.log(
                f"Пропущено некорректных строк: {invalid_count}", LogLevel.WARNING
            )
    except FileNotFoundError as e:
        Console.log(str(e), LogLevel.ERROR)
        return

    if not proxies:
        Console.log(
            "Список прокси пуст или не содержит валидных записей.", LogLevel.ERROR
        )
        return

    Console.log(f"Найдено прокси для проверки: {len(proxies)}")
    Console.log("Начинаю проверку...\n")

    # 3. Проверка
    working_proxies = await check_all_proxies(
        proxies=proxies,
        max_workers=args.workers,
        timeout=args.timeout,
        progress_callback=Console.progress,
    )

    # 4. Итоги
    Console.header("📊 ИТОГИ")
    Console.log(f"Всего проверено  : {len(proxies)}")
    Console.log(f"Рабочих прокси   : {len(working_proxies)}", LogLevel.SUCCESS)

    failed_count = len(proxies) - len(working_proxies)
    if failed_count > 0:
        Console.log(f"Нерабочих прокси : {failed_count}", LogLevel.WARNING)

    if working_proxies:
        save_proxies(working_proxies, args.output)
        Console.log(f"Результат сохранён в: {args.output}", LogLevel.SUCCESS)
    else:
        Console.log("Нет рабочих прокси для сохранения.", LogLevel.WARNING)

    Console.header("Готово!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Проверка прервана пользователем.")
