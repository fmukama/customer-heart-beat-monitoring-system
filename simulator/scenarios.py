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


def should_be_late(probability: float) -> bool:
    """
    Decide whether the next event should experience
    artificial delivery delay.
    """

    return random.random() < probability


def generate_late_delay(
    maximum_seconds: float,
) -> float:
    """
    Generate a random artificial delay for a late event.
    """

    return random.uniform(1.0, maximum_seconds)