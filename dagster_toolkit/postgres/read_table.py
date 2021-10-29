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
import psycopg2
from dagster import solid, Field, Bool, List
import pandas as pd
from dagster import String, Optional
from dagster_pandas import DataFrame


@solid(required_resource_keys={'postgres_warehouse'},
       config_schema={
           'fatal': Field(
               Bool,
               default_value=True,
               is_required=False,
               description='Controls whether exceptions cause a Failure or not',
           )
       }
       )
def query_table(context, sql: String) -> Optional[DataFrame]:
    """
    Execute an SQL
    :param context: execution context
    :param sql: the SQL select query to execute
    :return: panda DataFrame or None
    :rtype: panda.DataFrame
    """
    return __run_query_table(context, None, sql)


def __run_query_table(context, client, sql: String) -> Optional[DataFrame]:
    """
    Execute an SQL
    :param context: execution context
    :param sql: the SQL select query to execute
    :return: panda DataFrame or None
    :rtype: panda.DataFrame
    """
    df = None

    close_down = client is None
    if close_down:
        client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:

        context.log.info(f"Execute query: '{sql}'")

        # execute the query and get all the results
        cursor = client.cursor()

        try:
            cursor.execute(sql)
            # http://initd.org/psycopg/docs/cursor.html
            results = cursor.fetchall()

            context.log.info(f'{len(results)} records retrieved')

            context.log.info(f'DataFrame loading in progress')

            # load the results into a DataFrame
            df = pd.DataFrame.from_records(results)

            context.log.info(f'Loaded {len(df)} records')

        except psycopg2.Error as e:
            context.log.error(f'Error: {e}')
            if context.solid_config['fatal']:
                raise e

        finally:
            # tidy up
            cursor.close()
            if close_down:
                client.close_connection()

    return df


@solid(required_resource_keys={'postgres_warehouse'},
       config_schema={
           'fatal': Field(
               Bool,
               default_value=True,
               is_required=False,
               description='Controls whether exceptions cause a Failure or not',
           )
       }
       )
def multi_query_table(context, sql_list: List) -> List[Optional[DataFrame]]:
    """
    Execute an SQL
    :param context: execution context
    :param sql_list: list of SQL select queries to execute
    :return: panda DataFrame or None
    :rtype: panda.DataFrame
    """
    dfs = []

    client = context.resources.postgres_warehouse.get_connection(context)

    for sql in sql_list:
        dfs.append(__run_query_table(context, client, sql))

    client.close_connection()

    return dfs
