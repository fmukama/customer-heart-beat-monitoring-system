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


def should_happen(probability: float) -> bool:
    """
    Generic fault-injection coin flip.
    """

    return random.random() < probability


def generate_out_of_range_heart_rate() -> int:
    """
    A value outside the schema's 20-250 range, so it is rejected as
    invalid rather than merely classified abnormal.
    """

    if random.choice([True, False]):
        return random.randint(1, 19)

    return random.randint(251, 600)


# customer_id is deliberately never corrupted: the producer uses it as
# the Kafka partition key, so removing it would crash the producer
# rather than exercise the consumer's validation.
CORRUPTIONS = (
    "bad_event_id",
    "missing_event_time",
    "heart_rate_not_integer",
    "unexpected_field",
    "naive_event_time",
)


def corrupt_event(event: dict) -> dict:
    """
    Return a structurally invalid copy of an event.

    Varies the corruption so the DLQ sees a realistic spread of failure
    modes rather than one repeated shape.
    """

    broken = dict(event)

    match random.choice(CORRUPTIONS):
        case "bad_event_id":
            broken["event_id"] = "not-a-uuid"

        case "missing_event_time":
            broken.pop("event_time", None)

        case "heart_rate_not_integer":
            broken["heart_rate"] = f"{event['heart_rate']}"

        case "unexpected_field":
            broken["firmware_version"] = "1.4.2-beta"

        case "naive_event_time":
            broken["event_time"] = event["event_time"].split("+")[0]

    return broken