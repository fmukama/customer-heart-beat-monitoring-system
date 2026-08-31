from consumer.window_state import WindowState


def test_new_state_is_empty():
    state = WindowState()

    assert state.count == 0
    assert state.average == 0.0
    assert state.minimum is None
    assert state.maximum is None


def test_counts_and_sums_events():
    state = WindowState()

    state.add(heart_rate=60, is_abnormal=False)
    state.add(heart_rate=80, is_abnormal=False)

    assert state.count == 2
    assert state.average == 70.0


def test_tracks_minimum_and_maximum():
    state = WindowState()

    for heart_rate in [70, 55, 120, 90]:
        state.add(heart_rate=heart_rate, is_abnormal=False)

    assert state.minimum == 55
    assert state.maximum == 120


def test_counts_abnormal_separately():
    state = WindowState()

    state.add(heart_rate=70, is_abnormal=False)
    state.add(heart_rate=180, is_abnormal=True)
    state.add(heart_rate=45, is_abnormal=True)

    assert state.count == 3
    assert state.abnormal_count == 2
