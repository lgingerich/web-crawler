import logging
from colorlog import ColoredFormatter


def setup_logger():
    """Set up the logger with colored output."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Color scheme for different log levels
    color_scheme = {
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "red,bg_white",
    }

    # Create a ColoredFormatter
    formatter = ColoredFormatter(
        "%(log_color)s%(asctime)s - %(levelname)s - %(module)s - %(message)s",
        datefmt=None,
        reset=True,
        log_colors=color_scheme,
        secondary_log_colors={},
        style="%",
    )

    # Create a stream handler for console output
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(stream_handler)

    return logger
