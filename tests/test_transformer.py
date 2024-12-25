import pytest
import polars as pl
from ..func_data_eng.transformer import transform_data


def test_transform_data():
    df = pl.DataFrame(
        {"item_id": [1, 2, 3], "price": [10.0, -5.0, 20.0], "quantity": [2, 3, 1]}
    )

    transformed = transform_data(df)

    assert "total_value" in transformed.columns
    assert transformed.shape == (2, 4)  # Only rows with positive prices remain
    assert transformed["total_value"].to_list() == [20.0, 20.0]
