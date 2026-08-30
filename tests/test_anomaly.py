from consumer.anomaly import classify_heart_rate


def test_normal_heart_rate():

    assert classify_heart_rate(75) == "NORMAL"


def test_low_heart_rate():

    assert classify_heart_rate(40) == "ABNORMAL"


def test_high_heart_rate():

    assert classify_heart_rate(150) == "ABNORMAL"


def test_boundary_60():

    assert classify_heart_rate(60) == "NORMAL"


def test_boundary_100():

    assert classify_heart_rate(100) == "NORMAL"