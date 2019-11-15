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

from dagster_toolkit.mongo.connection import get_mongo
from dagster import solid
import pandas as pd

@solid
def download_from_mongo(context, server_cfg, sel_filter, projection):
    """
    Download panda DataFrame from a mongoDB server
    :param context: execution context
    :param server_cfg: path to server configuration
    :param sel_filter: a SON object specifying elements which must be present for a document to be included in the
                        result set
    :param projection: a list of field names that should be returned in the result set or a dict specifying the fields
                        to include or exclude. If projection is a list “_id” will always be returned.
                        Use a dict to exclude fields from the result (e.g. projection={‘_id’: False}).
    :return: panda DataFrame or None
    :rtype: panda.DataFrame
    """
    df = None

    client = get_mongo(context, server_cfg)

    if client is not None:
        db = client.get_connection()[client['dbname']]
        collection = db[client['collection']]

        # retrieve a cursor for required records
        # https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
        context.log.info(f'Record retrieval in progress')
        cursor = collection.find(filter=sel_filter, projection=projection)

        entries = list(cursor)
        context.log.info(f'Record retrieval complete')

        context.log.info(f'DataFrame loading in progress')
        df = pd.DataFrame.from_dict(entries)

        # tidy up
        cursor.close()
        client.close_connection()

        context.log.info(f'Loaded {len(df)} records')

    return df

