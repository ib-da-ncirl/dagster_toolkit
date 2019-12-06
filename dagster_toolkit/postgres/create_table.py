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
from db_toolkit.postgres import does_table_exist_sql


@solid(required_resource_keys={'postgres_warehouse'},
       config={
           'fatal': Field(
               Bool,
               default_value=True,
               is_optional=True,
               description='Controls whether exceptions cause a Failure or not',
           )
       }
       )
def does_psql_table_exist(context, name: String) -> Bool:
    """
    Check if a table exists in postgres
    :param context: execution context
    :param name: name of database table to check
    :return: True if exists
    """
    exists = False

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:

        context.log.info(f'Execute table "{name}" exists query')

        try:
            # execute the query and get all the results
            cursor = client.cursor()
            cursor.execute(does_table_exist_sql(name))
            # http://initd.org/psycopg/docs/cursor.html
            exists = cursor.fetchone()

        except psycopg2.Error as e:
            context.log.error(f'Error: {e}')
            if context.solid_config['fatal']:
                raise e

        finally:
            # tidy up
            cursor.close()
            client.close_connection()

    return exists


@solid(required_resource_keys={'postgres_warehouse'},
       config={
           'fatal': Field(
               Bool,
               default_value=True,
               is_optional=True,
               description='Controls whether exceptions cause a Failure or not',
           ),
           'if_not_exists': Field(
               Bool,
               default_value=True,
               is_optional=True,
               description='Controls whether IF NOT EXISTS clause is included in sql statement',
           )
       }
       )
def create_table(context, create_columns: String, table_name: String):
    """
    Creating a table on the Postgres server if it doesn't exist
    :param context: execution context
    :param create_columns: database table columns
    :param table_name: name of database table to upload to
    """

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:

        cursor = client.cursor()

        if context.solid_config['if_not_exists']:
            exists = 'IF NOT EXISTS '
        else:
            exists = ''
        create_table_query = f'CREATE TABLE {exists}{table_name} ({create_columns})'
        try:
            context.log.info(f"Execute create table query for '{table_name}'")
            cursor.execute(create_table_query)
            client.commit()

        except psycopg2.Error as e:
            context.log.error(f'Error: {e}')
            if context.solid_config['fatal']:
                raise e

        finally:
            # tidy up
            cursor.close()
            client.close_connection()
