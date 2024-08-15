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
    list_date: str, top_n: int, cache_dir: str
):
    """
    Fetch the Tranco list and save it locally.
    
    :param list_date: Date of the list to fetch
    :param top_n: Number of top domains to fetch
    :param cache_dir: Directory to save the Tranco list
    :return: Path to the saved file
    """
    t = Tranco(cache=True, cache_dir=cache_dir)
    tranco_list = t.list(date=list_date).top(top_n)

    os.makedirs(cache_dir, exist_ok=True)

    file_path = os.path.join(cache_dir, f"tranco_top_{top_n}_{list_date}.txt")

    with open(file_path, "w") as f:
        for domain in tranco_list:
            f.write(f"{domain}\n")

    return file_path