import logging
import os


LOG_DIR = "logs"

# Create log directory if missing
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR, exist_ok=True)


def get_logger(name: str):
    """Creates a consistent logger with console + file outputs."""

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    # Info file handler
    info_fh = logging.FileHandler(os.path.join(LOG_DIR, "info.log"))
    info_fh.setLevel(logging.INFO)
    info_fh.setFormatter(formatter)
    logger.addHandler(info_fh)

    # Error file handler
    err_fh = logging.FileHandler(os.path.join(LOG_DIR, "error.log"))
    err_fh.setLevel(logging.ERROR)
    err_fh.setFormatter(formatter)
    logger.addHandler(err_fh)

    return logger
