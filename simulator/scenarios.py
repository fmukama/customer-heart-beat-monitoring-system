import random


def generate_normal_heart_rate(
    minimum: int = 60,
    maximum: int = 100,
) -> int:
    """
    Generate a normal simulated heart-rate value.
    """

    return random.randint(minimum, maximum)


def generate_abnormal_heart_rate(
    minimum: int = 40,
    maximum: int = 180,
) -> int:
    """
    Generate an intentionally abnormal simulated heart-rate value.
    """

    # Generate either a low or high abnormal value.
    if random.choice([True, False]):
        return random.randint(minimum, 59)

    return random.randint(101, maximum)


def should_be_abnormal(probability: float) -> bool:
    """
    Decide whether the next event should be abnormal.
    """

    return random.random() < probability


def should_be_out_of_order(probability: float) -> bool:
    """
    Decide whether the next event should carry a backdated event_time.
    """

    return random.random() < probability


def generate_backdate_seconds(
    maximum_seconds: float,
) -> float:
    """
    Generate how far back an out-of-order event_time is shifted.

    Backdating event_time is what makes the event arrive out of order.
    Delaying delivery alone would not: event_time would still increase
    monotonically, so the watermark could never classify it as late.
    """

    return random.uniform(1.0, maximum_seconds)