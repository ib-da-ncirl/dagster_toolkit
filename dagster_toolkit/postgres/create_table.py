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

from dagster import solid
import pandas as pd


@solid(required_resource_keys={'postgres_warehouse'})
def query_table(context, sql):
    """
    Execute an SQL
    :param context: execution context
    :param sql: the SQL select query to execute
    :return: panda DataFrame or None
    :rtype: panda.DataFrame
    """
    df = None

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:

        context.log.info(f'Execute query')

        # execute the query and get all the results
        cursor = client.cursor()
        cursor.execute(sql)
        # http://initd.org/psycopg/docs/cursor.html
        results = cursor.fetchall()

        context.log.info(f'{len(results)} records retrieved')

        context.log.info(f'DataFrame loading in progress')

        # load the results into a DataFrame
        df = pd.DataFrame.from_records(results)

        context.log.info(f'Loaded {len(df)} records')

        # tidy up
        cursor.close()
        client.close_connection()

    return df
