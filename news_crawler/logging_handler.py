import logging
from logging.handlers import RotatingFileHandler
import warnings
from pathlib import Path
from typing import Dict

class LoggingHandler:
    @staticmethod
    def setup_logging(config: Dict):
        log_config = config.get('logging', {})
        global_level = log_config.get('global_level', 'DEBUG')
        log_file = log_config.get('file', 'app.log')
        log_max_size_mb = log_config.get('max_size_mb', 2)
        log_backup_count = log_config.get('backup_count', 5000)
        file_log_level = log_config.get('file_level', global_level)
        console_log_level = log_config.get('console_level', global_level)

        log_max_size = log_max_size_mb * 1024 * 1024  # Convert MB to bytes

        # Set log file path two levels up
        script_dir = Path(__file__).resolve().parent
        log_file_path = (script_dir / ".." / "logs" / log_file).resolve()
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, global_level.upper(), logging.DEBUG))

        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(filename)s: Line %(lineno)d] - %(message)s'
        )

        file_handler = LoggingHandler._create_file_handler(
            log_file_path, log_max_size, log_backup_count, file_log_level, formatter
        )
        stream_handler = LoggingHandler._create_stream_handler(console_log_level, formatter)

        # Clear existing handlers
        if root_logger.hasHandlers():
            root_logger.handlers.clear()

        root_logger.addHandler(file_handler)
        root_logger.addHandler(stream_handler)

        LoggingHandler._suppress_warnings()
        LoggingHandler._redirect_warnings_to_logging(stream_handler)

    @staticmethod
    def _create_file_handler(log_file_path: Path, max_bytes: int, backup_count: int, level: str, formatter: logging.Formatter) -> RotatingFileHandler:
        file_handler = RotatingFileHandler(log_file_path, maxBytes=max_bytes, backupCount=backup_count)
        file_handler.setLevel(getattr(logging, level.upper(), logging.DEBUG))
        file_handler.setFormatter(formatter)
        file_handler.addFilter(LoggingHandler._script_logger_filter)
        return file_handler

    @staticmethod
    def _create_stream_handler(level: str, formatter: logging.Formatter) -> logging.StreamHandler:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        stream_handler.setFormatter(formatter)
        stream_handler.addFilter(LoggingHandler._script_logger_filter)
        return stream_handler

    @staticmethod
    def _script_logger_filter(record: logging.LogRecord) -> bool:
        current_directory = Path(__file__).parent.resolve()
        return current_directory in Path(record.pathname).resolve().parents

    @staticmethod
    def _suppress_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)

    @staticmethod
    def _redirect_warnings_to_logging(handler: logging.Handler):
        logging.captureWarnings(True)
        warnings_logger = logging.getLogger('py.warnings')
        warnings_logger.addHandler(handler)
        warnings_logger.setLevel(logging.WARNING)

# Example usage:
if __name__ == "__main__":
    config = {
        'logging': {
            'global_level': 'DEBUG',
            'file': 'app.log',
            'max_size_mb': 5,
            'backup_count': 5,
            'file_level': 'DEBUG',
            'console_level': 'INFO'
        }
    }
    LoggingHandler.setup_logging(config)
    # Add any code that may generate FutureWarnings here to test the setup.
