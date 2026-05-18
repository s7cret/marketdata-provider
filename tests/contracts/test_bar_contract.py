from marketdata_provider.core import Bar


def test_bar_contract_uses_epoch_ms_open_time_and_default_volume():
    bar = Bar(time=1_779_552_000_000, open=1.0, high=2.0, low=0.5, close=1.5)

    assert bar.time == 1_779_552_000_000
    assert bar.volume == 0.0
    assert bar.time_close is None
