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
from dagster import solid, String, Bool, Field
from db_toolkit.postgres import drop_table_sql


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
def drop_table(context, table_name: String) -> Bool:
    """
    Drop a table from the Postgres server if it exists
    :param context: execution context
    :param table_name: name of database table to upload to
    """
    dropped = False

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:

        cursor = client.cursor()

        try:
            context.log.info(f'Execute drop table query for {table_name}')
            cursor.execute(drop_table_sql(table_name))
            client.commit()
            dropped = True

        except psycopg2.Error as e:
            context.log.error(f'Error: {e}')
            if context.solid_config['fatal']:
                raise e

        finally:
            # tidy up
            cursor.close()
            client.close_connection()

    return dropped
