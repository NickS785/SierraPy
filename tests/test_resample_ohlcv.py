import pandas as pd

from sierrapy.parser import resample_ohlcv


def test_resample_open_from_close_first():
    index = pd.date_range("2023-01-01", periods=4, freq="min")
    frame = pd.DataFrame(
        {
            "Open": [0.0, 0.0, 0.0, 0.0],
            "Close": [1.0, 2.0, 3.0, 4.0],
        },
        index=index,
    )

    result = resample_ohlcv(frame, "2min")

    expected_index = pd.date_range("2023-01-01", periods=2, freq="2min")
    expected = pd.DataFrame(
        {
            "Open": [1.0, 3.0],
            "Close": [2.0, 4.0],
        },
        index=expected_index,
    )

    pd.testing.assert_index_equal(result.index, expected.index)
    pd.testing.assert_series_equal(result["Open"], expected["Open"], check_names=False)
    pd.testing.assert_series_equal(result["Close"], expected["Close"], check_names=False)
