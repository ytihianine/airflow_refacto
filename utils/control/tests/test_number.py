import pytest
import pandas as pd
from utils.control.number import is_lower, is_upper, is_in_range


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({"A": [10, 20, 30], "B": [5, 15, 25]})


def test_is_lower_with_all_values_below_threshold(sample_dataframe):
    assert is_lower(sample_dataframe, ["A", "B"], seuil=40) == True


def test_is_lower_with_at_least_one_values_above_threshold(sample_dataframe):
    assert is_lower(sample_dataframe, ["A", "B"], seuil=15) == False


def test_is_upper_with_all_values_above_threshold(sample_dataframe):
    assert is_upper(sample_dataframe, ["A"], seuil=10) == True


def test_is_upper_with_at_least_one_values_above_threshold(sample_dataframe):
    assert is_upper(sample_dataframe, ["B"], seuil=15) == False


def test_is_in_range_with_all_values_within_range(sample_dataframe):
    assert is_in_range(sample_dataframe, ["A", "B"], seuil_inf=0, seuil_sup=40) == True


def test_is_in_range_with_at_least_one_value_outside_range(sample_dataframe):
    assert (
        is_in_range(sample_dataframe, ["A", "B"], seuil_inf=10, seuil_sup=30) == False
    )
