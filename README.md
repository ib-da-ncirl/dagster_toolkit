# dagster_toolkit

**dagster_toolkit** provides some utility functions/solids to work with [Dagster](https://dagster.readthedocs.io/) pipelines.

It is _definitely_ a work-in-progress.

Current functionality includes:

* postgres_warehouse_resource()

    Resource constructor function for Postgres server
    
    Connection parameters must be specified via a configuration file. See [postgres_cfg.sample](db_toolkit/docs/postgres_cfg.sample).
    Path to configuration must be specified under 
        
        'resources': { 
            'postgres_warehouse': {
                'config': {
                    'postgres_cfg': 'path to config file'
                }
            }
        }
    in environment_dict for the pipeline.           

* mongo_warehouse_resource()

    Get a connection to a mongoDb server
    
    Connection parameters must be specified via a configuration file. See [mongo_cfg.sample](db_toolkit/docs/mongo_cfg.sample).
    Path to configuration must be specified under 
        
        'resources': { 
            'mongo_warehouse': {
                'config': {
                    'mongo_cfg': 'path to config file'
                }
            }
        }
    in environment_dict for the pipeline.           
    
* download_from_mongo()

    Download data from a from a mongoDb server and save it to a pandas DataFrame.
    
## Installation
Please see https://packaging.python.org/tutorials/installing-packages/ for general information on installation methods.

Install dependencies via

    pip install -r requirements.txt
  
## Acknowledgements

Package layout inspired by https://github.com/bast/somepackage
