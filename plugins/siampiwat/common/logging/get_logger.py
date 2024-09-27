import logging
import inspect
import os
from datetime import datetime
import pytz

class Logger:
    """
    Centralized Logger class that ensures logs are correctly attributed to the module
    where the logger is used, with the filename displayed instead of the full module path.
    """

    def __init__(self, level: int = logging.INFO, timezone: str = 'Asia/Bangkok'):
        self.timezone = timezone
        self._logger = self._get_logger()
        self._logger.setLevel(level)
        self._ensure_handler()

    def _get_caller_name(self):
        """
        Determine the caller's filename and class name for correct logger attribution.
        """
        frame = inspect.currentframe()
        while frame:
            module = inspect.getmodule(frame)
            if module and module.__name__ != __name__:
                file_name = os.path.basename(module.__file__)
                return file_name
            frame = frame.f_back
        return "__main__"

    def _get_logger(self):
        """
        Get a logger instance using the correct name based on the caller's context.
        """
        logger_name = self._get_caller_name()
        return logging.getLogger(logger_name)

    def _ensure_handler(self):
        """
        Ensure that at least one handler is attached to the logger.
        """
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            formatter = CustomFormatter(self.timezone)
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def _log(self, level, message, *args, **kwargs):
        """
        Internal method to log a message at a specific level, ensuring correct attribution.
        """
        self._logger.log(level, message, *args, stacklevel=3, **kwargs)

    def info(self, message, *args, **kwargs):
        self._log(logging.INFO, message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self._log(logging.ERROR, message, *args, **kwargs)

    def debug(self, message, *args, **kwargs):
        self._log(logging.DEBUG, message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        self._log(logging.WARNING, message, *args, **kwargs)

    def critical(self, message, *args, **kwargs):
        self._log(logging.CRITICAL, message, *args, **kwargs)

    @classmethod
    def get_logger(cls):
        """
        Factory method to get a logger for the calling module.
        :return: An instance of Logger for the calling module.
        """
        return cls()

class CustomFormatter(logging.Formatter):
    """
    Custom Formatter class that formats the log records with the specified timezone
    and adds color to the output based on the log level.
    """

    BLUE = "\033[34m"
    RED = "\033[31m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"
    RESET = "\033[0m"

    LOG_COLORS = {
        logging.DEBUG: CYAN,
        logging.INFO: BLUE,
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
        logging.CRITICAL: MAGENTA,  # Usually mapped to EXCEPTION
    }

    def __init__(self, timezone: str, fmt: str = '[%(asctime_colored)s] - {%(name_colored)s:%(lineno_colored)s} - %(levelname)s - %(message)s'):
        super().__init__(fmt)
        self.timezone = pytz.timezone(timezone)

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.timezone)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime('%Y-%m-%d %H:%M:%S %z')

    def format(self, record):
        # Choose the color based on the log level
        log_color = self.LOG_COLORS.get(record.levelno, self.RESET)

        # Create colored versions of the asctime, name, and lineno fields
        record.asctime_colored = f"{log_color}{self.formatTime(record)}{self.RESET}"
        record.name_colored = f"{log_color}{record.name}{self.RESET}"
        record.lineno_colored = f"{log_color}{record.lineno}{self.RESET}"

        # Call the parent class's format method
        return super().format(record)
