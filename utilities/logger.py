import logging
import os
from logging.handlers import TimedRotatingFileHandler


def setup_logger(name):
    """
    Set up a logger with TimedRotatingFileHandler.

    Parameters:
    name (str): The name of the logger.

    Returns:
    logger (logging.Logger): Configured logger instance.
    """

    try:
        name = name.lower().replace(" ", "_")
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        log_directory = "./logs"

        # Check if the directory exists, if not, create it
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        # Define the file path
        log_file = os.path.join(log_directory, f"{name}.log")

        handler = TimedRotatingFileHandler(
            log_file, when="midnight", interval=1, backupCount=7
        )
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)

        if not logger.handlers:
            logger.addHandler(handler)

        return logger
    except Exception as e:
        print(f"Failed to set up logger: {e}")
        return None
