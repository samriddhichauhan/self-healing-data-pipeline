"""
Central Pipeline & AI Logging Utility
Maintains persistent logs in root logs/ directory grouped by file name and task/component.
"""
import os
import sys
import logging
from datetime import datetime, timezone

# Resolve root logs/ directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
LOGS_DIR = os.path.join(project_root, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)


def get_logger(module_name: str, log_filename: str = None) -> logging.Logger:
    """
    Returns a configured logger writing to logs/<log_filename>.log and stdout.
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not log_filename:
        log_filename = f"{module_name.lower().replace('.', '_')}.log"
    elif not log_filename.endswith(".log"):
        log_filename = f"{log_filename}.log"

    log_filepath = os.path.join(LOGS_DIR, log_filename)

    # Prevent duplicate handlers
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == os.path.abspath(log_filepath) for h in logger.handlers):
        file_handler = logging.FileHandler(log_filepath, encoding="utf-8")
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s", "%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def log_ai_event(event_type: str, details: dict):
    """
    Writes structured AI/Ollama events directly to logs/ollama_ai_agent.log
    """
    ollama_logger = get_logger("ollama_ai_agent", "ollama_ai_agent.log")
    timestamp = datetime.now(timezone.utc).isoformat()
    msg = f"[{event_type.upper()}] - Timestamp: {timestamp}\n"
    for k, v in details.items():
        msg += f"  - {k}: {v}\n"
    ollama_logger.info(msg.strip())
