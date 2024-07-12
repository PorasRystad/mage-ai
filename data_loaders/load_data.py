import io
import pandas as pd
import requests
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(**kwargs) -> DataFrame:
    df_from_url = pd.read_excel(
        io="https://assets.publishing.service.gov.uk/media/6655b5ddd470e3279dd3329c/ET_3.14_MAY_24.xlsx",
        sheet_name=7,
        skiprows=4
    )
    print("Test")
    return df_from_url


@test
def test_output(df) -> None:
    """
    Template code for testing the output of the block.
    """
    assert df is not None, 'The output is undefined'
