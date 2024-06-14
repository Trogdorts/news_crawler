import logging
import os
from logging.handlers import RotatingFileHandler

class ScriptLoggerFilter(logging.Filter):
    def filter(self, record):
        return record.pathname.startswith(os.path.dirname(os.path.abspath(__file__)))

class LoggingHandler:
    @staticmethod
    def setup_logging(config):
        log_level = config.get('logging', {}).get('level', 'DEBUG')
        log_file = config.get('logging', {}).get('file', 'app.log')
        log_max_size = config.get('logging', {}).get('max_size', 1024 * 1024 * 5)  # Default: 5MB
        log_backup_count = config.get('logging', {}).get('backup_count', 0)  # Default: keep all files
        file_log_level = config.get('logging', {}).get('file_level', log_level)
        console_log_level = config.get('logging', {}).get('console_level', log_level)

        try:
            log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', os.path.dirname(log_file))
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            log_file_path = os.path.join(log_dir, os.path.basename(log_file))
            root_logger = logging.getLogger()
            root_logger.setLevel(getattr(logging, log_level.upper(), logging.DEBUG))  # Root logger level

            formatter = logging.Formatter('%(asctime)s - %(levelname)s - [Line:%(lineno)s] - %(message)s')

            # Rotating file handler
            file_handler = RotatingFileHandler(log_file_path, maxBytes=log_max_size, backupCount=log_backup_count)
            file_handler.setLevel(getattr(logging, file_log_level.upper(), logging.DEBUG))
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

            # Stream handler
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(getattr(logging, console_log_level.upper(), logging.DEBUG))
            stream_handler.setFormatter(formatter)
            root_logger.addHandler(stream_handler)

            # Add the custom filter to both handlers
            filter = ScriptLoggerFilter()
            file_handler.addFilter(filter)
            stream_handler.addFilter(filter)

        except Exception as e:
            raise RuntimeError(f"Error setting up logging: {e}")