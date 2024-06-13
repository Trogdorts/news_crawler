import logging
import os

class ScriptLoggerFilter(logging.Filter):
    def filter(self, record):
        return record.pathname.startswith(os.path.dirname(os.path.abspath(__file__)))

class LoggingHandler:
    @staticmethod
    def setup_logging(config):
        log_level = config.get('logging', {}).get('level', 'DEBUG')
        log_file = config.get('logging', {}).get('file', 'rename_completed_downloads.log')

        try:
            log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', os.path.dirname(log_file))
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            log_file_path = os.path.join(log_dir, os.path.basename(log_file))
            root_logger = logging.getLogger()
            root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - [Line:%(lineno)s] - %(message)s')

            # File handler
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

            # Stream handler
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            root_logger.addHandler(stream_handler)

            # Add the custom filter to both handlers
            filter = ScriptLoggerFilter()
            file_handler.addFilter(filter)
            stream_handler.addFilter(filter)

        except Exception as e:
            raise RuntimeError(f"Error setting up logging: {e}")
