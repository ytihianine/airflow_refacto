"""Tests for DataFrame utility functions."""

import pytest
import pandas as pd
import numpy as np
from io import StringIO
import sys

from utils.dataframe import df_info, tag_last_value_rows


class TestDfInfo:
    """Test suite for df_info function."""

    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "col1": [1, 2, 3, None],
                "col2": ["A", "B", "C", "D"],
                "col3": [1.1, 2.2, 3.3, 4.4],
            }
        )

    def test_df_info_basic_functionality(self, sample_dataframe, capsys):
        """Test basic functionality of df_info."""
        df_info(sample_dataframe, "Test DataFrame", sample_size=2)
        captured = capsys.readouterr()

        assert "Test DataFrame" in captured.out
        assert "Shape of DataFrame: (4, 3)" in captured.out
        assert "col1, col2, col3" in captured.out

    def test_df_info_invalid_input_type(self):
        """Test df_info with invalid input type."""
        with pytest.raises(TypeError, match="Expected pandas DataFrame"):
            df_info("not a dataframe", "Test")

    def test_df_info_negative_sample_size(self, sample_dataframe):
        """Test df_info with negative sample size."""
        with pytest.raises(ValueError, match="sample_size must be non-negative"):
            df_info(sample_dataframe, "Test", sample_size=-1)

    def test_df_info_full_logs_false(self, sample_dataframe, capsys):
        """Test df_info with full_logs=False."""
        df_info(sample_dataframe, "Test DataFrame", full_logs=False)
        captured = capsys.readouterr()

        # Should not contain descriptive statistics
        assert "Descriptive Statistics" not in captured.out
        assert "Columns data types" not in captured.out

    def test_df_info_with_none_columns(self, capsys):
        """Test df_info with None values in column names."""
        df = pd.DataFrame([[1, 2], [3, 4]])
        df.columns = ["col1", None]

        df_info(df, "Test with None")
        captured = capsys.readouterr()
        assert "Warning: 1 None values found" in captured.out


class TestTagLastValueRows:
    """Test suite for tag_last_value_rows function."""

    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-02"],
                "value": [10, 20, 30, 25],
            }
        )

    def test_tag_last_value_rows_basic(self, sample_dataframe):
        """Test basic functionality of tag_last_value_rows."""
        result = tag_last_value_rows(sample_dataframe, "value")

        assert "is_last_value" in result.columns
        assert result["is_last_value"].sum() == 1  # Only one max value
        assert result.loc[2, "is_last_value"] == True  # Row with value 30
        assert result.loc[0, "is_last_value"] == False  # Row with value 10

    def test_tag_last_value_rows_multiple_max(self):
        """Test with multiple rows having the same max value."""
        df = pd.DataFrame({"values": [1, 3, 2, 3, 1]})
        result = tag_last_value_rows(df, "values")

        assert result["is_last_value"].sum() == 2  # Two rows with max value 3
        assert result.loc[1, "is_last_value"] == True
        assert result.loc[3, "is_last_value"] == True

    def test_tag_last_value_rows_invalid_dataframe(self):
        """Test with invalid DataFrame input."""
        with pytest.raises(TypeError, match="Expected pandas DataFrame"):
            tag_last_value_rows("not a dataframe", "column")

    def test_tag_last_value_rows_invalid_column(self, sample_dataframe):
        """Test with non-existent column name."""
        with pytest.raises(KeyError, match="Column 'nonexistent' not found"):
            tag_last_value_rows(sample_dataframe, "nonexistent")

    def test_tag_last_value_rows_doesnt_modify_original(self, sample_dataframe):
        """Test that original DataFrame is not modified."""
        original_columns = list(sample_dataframe.columns)
        result = tag_last_value_rows(sample_dataframe, "value")

        assert list(sample_dataframe.columns) == original_columns
        assert "is_last_value" not in sample_dataframe.columns
        assert "is_last_value" in result.columns

    def test_tag_last_value_rows_with_nan_values(self):
        """Test with NaN values in the target column."""
        df = pd.DataFrame({"values": [1, np.nan, 3, 2, np.nan]})
        result = tag_last_value_rows(df, "values")

        # Max should be 3, ignoring NaN values
        assert result["is_last_value"].sum() == 1
        assert result.loc[2, "is_last_value"] == True

    def test_tag_last_value_rows_empty_dataframe(self):
        """Test with empty DataFrame."""
        df = pd.DataFrame({"values": []})
        result = tag_last_value_rows(df, "values")

        # Should return empty DataFrame with is_last_value column
        assert "is_last_value" in result.columns
        assert len(result) == 0
