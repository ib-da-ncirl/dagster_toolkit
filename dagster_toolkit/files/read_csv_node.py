# The MIT License (MIT)
# Copyright (c) 2019 Ian Buttimer

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import pandas as pd
import os.path as path
from dagster import (
    solid,
    Field,
    String,
    Bool,
    Any,
    List,
    Dict,
    InputDefinition,
    OutputDefinition,
    Output
)
from dagster_pandas import DataFrame


@solid()
def load_csv(context, csv_path: String, kwargs: Dict) -> DataFrame:
    """
    Load csv file and convert into a panda DataFrame
    :param context: execution context
    :param csv_path: path to io file
    :param kwargs: dictionary of arguments as specified by the pandas.read_csv() function
    :return: panda DataFrame of data from csv file
    """
    # verify csv path
    if not path.exists(csv_path):
        raise ValueError(f'Invalid csv file path: {csv_path}')

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html#pandas.read_csv
    df = pd.read_csv(csv_path, **kwargs)

    context.log.info(f'Loaded {len(df)} entries from {csv_path}')

    return df
