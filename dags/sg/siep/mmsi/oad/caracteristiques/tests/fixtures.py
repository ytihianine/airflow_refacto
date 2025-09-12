import pytest
import pandas as pd


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({"A": [10, 20, 30], "B": [5, 15, 25]})
