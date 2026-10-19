from pathlib import Path
from loguru import logger

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logger.remove()

logger.add(
    LOG_DIR / "engine.log",
    rotation="10 MB",
    retention="10 days",
    level="INFO"
)

logger.add(
    lambda m: print(m, end="")
)

log = logger

def setup_logger():
    return logger