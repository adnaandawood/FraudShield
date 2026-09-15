import sys
import time

sys.path.insert(0, ".")

from processor import feature_engine


def test_velocity_and_new_flags():
    uid = "test_user_velocity"
    now = time.time()
    # fresh user -> no history, no new-device penalty on first sight
    f0 = feature_engine.get_features(uid + "_fresh", "device_1", "IN", now)
    assert f0["tx_count_1m"] == 0 and f0["new_device"] == 0

    # record 6 rapid txns then check velocity
    for i in range(6):
        feature_engine.record_transaction(uid, 500, "device_1", "IN", now - 30 + i)
    f1 = feature_engine.get_features(uid, "device_1", "IN", now)
    assert f1["tx_count_1m"] == 6
    assert f1["new_device"] == 0
    # unseen device/country flagged
    f2 = feature_engine.get_features(uid, "device_9", "RU", now)
    assert f2["new_device"] == 1 and f2["new_country"] == 1
