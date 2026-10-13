"""PHI-free observability (AD-8): counts and names only, never row values."""

import logging

logger = logging.getLogger("pipeline")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def log_counts(layer: str, source: str, rows_in: int, rows_out: int) -> None:
    logger.info(
        "layer=%s source=%s rows_in=%d rows_out=%d dropped=%d",
        layer,
        source,
        rows_in,
        rows_out,
        rows_in - rows_out,
    )
