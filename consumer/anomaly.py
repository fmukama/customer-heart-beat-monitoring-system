NORMAL_MIN_HEART_RATE = 60
NORMAL_MAX_HEART_RATE = 100


def classify_heart_rate(heart_rate: int) -> str:
    """
    Classify a heart-rate reading.

    This is a simple simulation rule, not a medical diagnostic rule.
    """

    if (
        NORMAL_MIN_HEART_RATE
        <= heart_rate
        <= NORMAL_MAX_HEART_RATE
    ):
        return "NORMAL"

    return "ABNORMAL"