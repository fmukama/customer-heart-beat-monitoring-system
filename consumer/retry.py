import random
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry_with_backoff(
    operation: Callable[[], T],
    max_attempts: int = 4,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
) -> T:
    """
    Execute an operation with exponential backoff.

    Example delays:

        1s
        2s
        4s
        8s

    Random jitter is added to avoid synchronized retries.
    """

    for attempt in range(1, max_attempts + 1):

        try:
            return operation()

        except Exception:

            if attempt == max_attempts:
                raise

            exponential_delay = initial_delay * (
                2 ** (attempt - 1)
            )

            delay = min(
                exponential_delay,
                max_delay,
            )

            jitter = random.uniform(
                0,
                delay * 0.25,
            )

            time.sleep(
                delay + jitter
            )

    raise RuntimeError(
        "Retry operation exited unexpectedly."
    )