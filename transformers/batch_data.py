from pandas import DataFrame
from typing import List
import math

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import datetime

def batch_records(df):
    batch_time = datetime.datetime.now()
    batch_time = batch_time.strftime('%Y-%m-%d %H:%M:%S')
    df["batch_time"] = batch_time
    records_to_insert = df.to_records(index=False).tolist()
    return records_to_insert


@transformer
def transform_df(df: DataFrame, *args, **kwargs) -> List:
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        df (DataFrame): Data frame from parent block.

    Returns:
        DataFrame: Transformed data frame
    """
    # Specify your transformation logic here

    return batch_records(df)

