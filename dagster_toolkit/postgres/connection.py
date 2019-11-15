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
from dagster import Failure


def get_postgres(context, server_cfg, fatal=True):
    """
    Establish a connection to a Postgres server
    :param context: execution context
    :param server_cfg: path to server configuration
    :param fatal: Optional, fatal if no connection, if True raises Failure; default True
    :return: server object or None if unable to connect
    :rtype: PostgresDb
    """
    client = PostgresDb(cfg_filename=server_cfg)
    server = client['host']

    if client.get_connection() is not None:
        context.log.info(f'Connected to Postgres: {server}')
    else:
        context.log.info(f'Unable to connect to Postgres: {server}')
        client.close_connection()
        if fatal:
            raise Failure(f'Unable to connect to Postgres: {server}')
        client = None

    return client
