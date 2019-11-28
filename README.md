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
    Alternatively, see EnvironmentDict for easier environment_dict generation.

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
    Alternatively, see EnvironmentDict for easier environment_dict generation.
    
* download_from_mongo()

    Download data from a mongoDb server and save it to a pandas DataFrame. See mongo_warehouse_resource() for how to configure the server resource. 

* query_table()

    Query a Postgres database table

* load_csv()

    Load a csv file into a pandas DataFrame

* EnvironmentDict

    The EnvironmentDict class may be used to ease the generation of environment_dict for a pipeline:
    
        env_dict = EnvironmentDict() \
                .add_solid_input('my_solid', 'arg_name', arg_value) \
                .add_solid('my_other_solid') \
                .add_resource('my_resource', resource_value) \
                .build()

## Installation
Please see https://packaging.python.org/tutorials/installing-packages/ for general information on installation methods.

Install dependencies via

    pip install -r requirements.txt
  
## Acknowledgements

Package layout inspired by https://github.com/bast/somepackage
