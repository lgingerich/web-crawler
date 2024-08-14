import logging
from colorlog import ColoredFormatter
import os
from tranco import Tranco


def setup_logger():
    logger = logging.getLogger("main_logger")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        color_scheme = {
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        }
        formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - %(module)s - %(message)s",
            datefmt=None,
            reset=True,
            log_colors=color_scheme,
            secondary_log_colors={},
            style="%",
        )
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger


# Create a single instance of the logger
logger = setup_logger()


def fetch_tranco_list(
    list_date: str = "2024-01-01", top_n: int = 10000, cache_dir: str = "url_data"
):
    """
    Fetch the Tranco list and save it locally.

    :param list_date: Date of the list to fetch ('latest' or YYYY-MM-DD format)
    :param top_n: Number of top domains to fetch
    :param cache_dir: Directory to save the Tranco list
    :return: Path to the saved file
    """
    t = Tranco(cache=True, cache_dir=cache_dir)

    # Fetch the list
    tranco_list = t.list(date=list_date).top(top_n)

    # Ensure the cache directory exists
    os.makedirs(cache_dir, exist_ok=True)

    # Save the list to a file
    file_name = f"tranco_top_{top_n}_{list_date}.txt"
    file_path = os.path.join(cache_dir, file_name)

    with open(file_path, "w") as f:
        for domain in tranco_list:
            f.write(f"{domain}\n")

    return file_path
