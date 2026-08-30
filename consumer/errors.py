class PermanentEventError(Exception):
    """
    The event itself is invalid.

    Retrying will not fix the event.
    The event should go to the DLQ.
    """


class TemporaryProcessingError(Exception):
    """
    A temporary infrastructure/application failure.

    The event should be retried.
    """


class DLQPublishError(Exception):
    """
    The event could not be published to the DLQ.
    """