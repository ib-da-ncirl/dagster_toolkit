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

from db_toolkit.mongo import MongoDb
from dagster import (
    Failure,
    resource,
    Field,
    String,
    Any,
    Bool
)

# see https://dagster.readthedocs.io/en/latest/sections/learn/tutorial/resources.html


class MongoWarehouse(object):
    """
    mongoDB data warehouse server object
    """
    def __init__(self, mongo_cfg, fatal=True):
        """
        Initialise object
        :param mongo_cfg: path to server configuration file or configuration dict
        :param fatal: Connection failure is fatal flag; default is True
        """
        self._mongo_cfg = mongo_cfg
        self._fatal = fatal
        self.client = None

    def get_connection(self, context):
        """
        Establish a connection to the mongoDB server
        :param context: execution context
        :return: server object or None if unable to connect
        :rtype: MongoDb
        """
        if isinstance(self._mongo_cfg, str):
            client = MongoDb(cfg_filename=self._mongo_cfg)
        elif isinstance(self._mongo_cfg, dict):
            client = MongoDb(cfg_dict=self._mongo_cfg)
        else:
            raise Failure(f'No configuration provided for MongoWarehouse')

        server = client['server']

        if client.is_authenticated():
            context.log.info(f'Connected to mongoDB: {server}')
        else:
            context.log.info(f'Unable to connect to mongoDB: {server}')
            client.close_connection()
            if self._fatal:
                raise Failure(f'Unable to connect to mongoDB: {server}')
            client = None

        return client


@resource(config={
    'mongo_cfg': Field(Any),
    'fatal': Field(Bool, default_value=True, is_optional=True)
})
def mongo_warehouse_resource(context):
    """
    Resource constructor function for mongoDB database
    :param context: execution context
    :return:
    """
    return MongoWarehouse(context.resource_config['mongo_cfg'], context.resource_config['fatal'])
