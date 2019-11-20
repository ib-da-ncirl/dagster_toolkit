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

from db_toolkit.postgres import PostgresDb
from dagster import (
    Failure,
    resource,
    Field,
    String,
    Bool
)

# see https://dagster.readthedocs.io/en/latest/sections/learn/tutorial/resources.html


class PostgresWarehouse(object):
    """
    Postgres data warehouse server object
    """
    def __init__(self, postgres_cfg, fatal=True):
        """
        Initialise object
        :param postgres_cfg: path to server configuration file
        :param fatal: Connection failure is fatal flag; default is True
        """
        self._postgres_cfg = postgres_cfg
        self._fatal = fatal
        self.client = None

    def get_connection(self, context):
        """
        Establish a connection to the Postgres server
        :param context: execution context
        :return: server object or None if unable to connect
        :rtype: PostgresDb
        """
        client = PostgresDb(cfg_filename=self._postgres_cfg)
        server = client['host']

        if client.get_connection() is not None:
            context.log.info(f'Connected to Postgres: {server}')
        else:
            context.log.info(f'Unable to connect to Postgres: {server}')
            client.close_connection()
            if self._fatal:
                raise Failure(f'Unable to connect to Postgres: {server}')
            client = None

        return client


@resource(config={
    'postgres_cfg': Field(String),
    'fatal': Field(Bool, default_value=True, is_optional=True)
})
def postgres_warehouse_resource(context):
    """
    Resource constructor function for Postgres database
    :param context: execution context
    :return:
    """
    return PostgresWarehouse(context.resource_config['postgres_cfg'], context.resource_config['fatal'])
