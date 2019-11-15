# dagster_toolkit

**dagster_toolkit** provides some utility functions/solids to work with [Dagster](https://dagster.readthedocs.io/) pipelines.

It is _definitely_ a work-in-progress.

Current functionality includes:

* get_postgres

    Get a connection to a Postgres server
    
    Connection parameters must be specified via a configuration file.
    See [postgres_cfg.sample](db_toolkit/docs/postgres_cfg.sample).

* get_mongo

    Get a connection to a mongoDb server
    
    Connection parameters must be specified via a configuration file.
    See [mongo_cfg.sample](db_toolkit/docs/mongo_cfg.sample).
    
* download_from_mongo

    Download data from a from a mongoDb server and save it to a pandas DataFrame.
    
    Connection parameters must be specified via a configuration file.
    See [mongo_cfg.sample](db_toolkit/docs/mongo_cfg.sample).
    
## Installation
Please see https://packaging.python.org/tutorials/installing-packages/ for general information on installation methods.

Install dependencies via

    pip install -r requirements.txt
  
## Acknowledgements

Package layout inspired by https://github.com/bast/somepackage
