import pytest

from consumer import retry


def test_successful_operation():

    calls = 0

    def operation():

        nonlocal calls

        calls += 1

        return "success"

    result = retry.retry_with_backoff(
        operation,
    )

    assert result == "success"
    assert calls == 1


def test_retry_then_success(
    monkeypatch,
):

    monkeypatch.setattr(
        retry.time,
        "sleep",
        lambda _: None,
    )

    calls = 0

    def operation():

        nonlocal calls

        calls += 1

        if calls < 3:

            raise RuntimeError(
                "temporary failure"
            )

        return "success"

    result = retry.retry_with_backoff(
        operation,
        max_attempts=4,
    )

    assert result == "success"
    assert calls == 3


def test_retry_exhausted(
    monkeypatch,
):

    monkeypatch.setattr(
        retry.time,
        "sleep",
        lambda _: None,
    )

    calls = 0

    def operation():

        nonlocal calls

        calls += 1

        raise RuntimeError(
            "database unavailable"
        )

    with pytest.raises(RuntimeError):

        retry.retry_with_backoff(
            operation,
            max_attempts=3,
        )

    assert calls == 3