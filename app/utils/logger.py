import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
import tarfile
import shutil
from typing import IO
from app.core.config import get_settings


class DailyRotatingFileHandler(RotatingFileHandler):
    def __init__(
        self, filename, mode="a", maxBytes=0, backupCount=0, encoding=None, delay=0
    ):
        self.date = datetime.now().date()
        self.base_dir = Path(filename).parent
        self.archive_dir = self.base_dir / "archive"
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.daily_dir = self.archive_dir / self.date.isoformat()
        self.daily_dir.mkdir(parents=True, exist_ok=True)
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)

    def shouldRollover(self, record: logging.LogRecord) -> int:
        if super().shouldRollover(record):
            return 1
        if datetime.now().date() != self.date:
            return 1
        return 0

    def doRollover(self) -> None:
        if self.stream:
            self.stream.close()

        current_time = datetime.now()
        if current_time.date() != self.date:
            self._archive_and_compress()
            self.date = current_time.date()
            self.daily_dir = self.archive_dir / self.date.isoformat()
            self.daily_dir.mkdir(parents=True, exist_ok=True)

        dfn = self.rotation_filename(f"{self.baseFilename}.{current_time.isoformat()}")
        if os.path.exists(dfn):
            os.remove(dfn)
        self.rotate(self.baseFilename, dfn)
        shutil.move(dfn, self.daily_dir)

        if not self.delay:
            self.stream = self._open()
        else:
            self.stream = None  # type: ignore

    def _open(self) -> IO[str]:
        """
        Open the current base file with the (original) mode and encoding.
        Return the resulting stream.
        """
        return open(self.baseFilename, self.mode, encoding=self.encoding)

    def _archive_and_compress(self) -> None:
        archive_name = f"logs_{self.date.isoformat()}.tar.gz"
        archive_path = self.archive_dir / archive_name

        try:
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(self.daily_dir, arcname=self.daily_dir.name)

            shutil.rmtree(self.daily_dir)
        except Exception as e:
            print(f"Error during archiving: {e}")


def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    settings = get_settings()
    logger.setLevel(settings.app_log_level)

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    archive_dir = log_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(settings.app_log_format)

    file_handler = DailyRotatingFileHandler(
        filename=str(log_dir / f"{name}.log"),
        maxBytes=25 * 1024 * 1024,  # 25 MB
        backupCount=0,
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
