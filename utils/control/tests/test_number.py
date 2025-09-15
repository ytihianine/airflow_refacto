"""Tests for numerical validation functions."""

import pytest
import pandas as pd
from utils.control.number import is_lower, is_upper, is_in_range


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({"A": [10, 20, 30], "B": [5, 15, 25]})


class TestIsLower:
    """Test suite for is_lower function."""

    def test_is_lower_with_all_values_below_threshold(self, sample_dataframe):
        """Test is_lower when all values are below threshold."""
        assert is_lower(sample_dataframe, ["A", "B"], seuil=40) == True

    def test_is_lower_with_at_least_one_values_above_threshold(self, sample_dataframe):
        """Test is_lower when at least one value is above threshold."""
        assert is_lower(sample_dataframe, ["A", "B"], seuil=15) == False

    def test_is_lower_invalid_dataframe(self):
        """Test is_lower with invalid DataFrame input."""
        with pytest.raises(TypeError, match="Expected pandas DataFrame"):
            is_lower("not a dataframe", ["A"], 10)

    def test_is_lower_empty_columns(self, sample_dataframe):
        """Test is_lower with empty column list."""
        with pytest.raises(ValueError, match="cols_to_check cannot be empty"):
            is_lower(sample_dataframe, [], 10)

    def test_is_lower_nonexistent_columns(self, sample_dataframe):
        """Test is_lower with non-existent columns."""
        with pytest.raises(KeyError, match="Columns not found in DataFrame"):
            is_lower(sample_dataframe, ["X", "Y"], 10)

    def test_is_lower_exclusive(self, sample_dataframe):
        """Test is_lower with exclusive comparison."""
        # Column A has values [10, 20, 30] - only first value is == 10
        # With exclusive (< 11), all values should be below 11
        assert (
            is_lower(sample_dataframe, ["A"], seuil=11, inclusive=False) == False
        )  # 10 < 11, but 20,30 > 11
        # With exclusive (< 31), all values should be below 31
        assert is_lower(sample_dataframe, ["A"], seuil=31, inclusive=False) == True


class TestIsUpper:
    """Test suite for is_upper function."""

    def test_is_upper_with_all_values_above_threshold(self, sample_dataframe):
        """Test is_upper when all values are above threshold."""
        assert is_upper(sample_dataframe, ["A"], seuil=10) == True

    def test_is_upper_with_at_least_one_values_below_threshold(self, sample_dataframe):
        """Test is_upper when at least one value is below threshold."""
        assert is_upper(sample_dataframe, ["B"], seuil=15) == False

    def test_is_upper_invalid_dataframe(self):
        """Test is_upper with invalid DataFrame input."""
        with pytest.raises(TypeError, match="Expected pandas DataFrame"):
            is_upper("not a dataframe", ["A"], 10)

    def test_is_upper_empty_columns(self, sample_dataframe):
        """Test is_upper with empty column list."""
        with pytest.raises(ValueError, match="cols_to_check cannot be empty"):
            is_upper(sample_dataframe, [], 10)

    def test_is_upper_nonexistent_columns(self, sample_dataframe):
        """Test is_upper with non-existent columns."""
        with pytest.raises(KeyError, match="Columns not found in DataFrame"):
            is_upper(sample_dataframe, ["X", "Y"], 10)


class TestIsInRange:
    """Test suite for is_in_range function."""

    def test_is_in_range_with_all_values_within_range(self, sample_dataframe):
        """Test is_in_range when all values are within range."""
        assert (
            is_in_range(sample_dataframe, ["A", "B"], seuil_inf=0, seuil_sup=40) == True
        )

    def test_is_in_range_with_at_least_one_value_outside_range(self, sample_dataframe):
        """Test is_in_range when at least one value is outside range."""
        assert (
            is_in_range(sample_dataframe, ["A", "B"], seuil_inf=10, seuil_sup=30)
            == False
        )

    def test_is_in_range_invalid_dataframe(self):
        """Test is_in_range with invalid DataFrame input."""
        with pytest.raises(TypeError, match="Expected pandas DataFrame"):
            is_in_range("not a dataframe", ["A"], 0, 10)

    def test_is_in_range_empty_columns(self, sample_dataframe):
        """Test is_in_range with empty column list."""
        with pytest.raises(ValueError, match="cols_to_check cannot be empty"):
            is_in_range(sample_dataframe, [], 0, 10)

    def test_is_in_range_invalid_bounds(self, sample_dataframe):
        """Test is_in_range with invalid bounds (lower > upper)."""
        with pytest.raises(
            ValueError, match="Lower bound .* cannot be greater than upper bound"
        ):
            is_in_range(sample_dataframe, ["A"], seuil_inf=10, seuil_sup=5)

    def test_is_in_range_nonexistent_columns(self, sample_dataframe):
        """Test is_in_range with non-existent columns."""
        with pytest.raises(KeyError, match="Columns not found in DataFrame"):
            is_in_range(sample_dataframe, ["X", "Y"], 0, 10)
