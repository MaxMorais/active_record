# This module sets the defaults for constants which are used internally by
# active record. They are also publicly available through the module, in case
# you want to do things like environment-specific tasks, or direct database
# querying.
#
# The constants that are meant to be used are the following:
#
#     ENV        -> The environment in which the application is being run.
#     INFLECTOR  -> An instance of inflect.engine(), done once for performance.
#     DATABASE   -> The name of the database that active record is connected to.
#     DB_ADAPTER -> The connection adapter instance that active record is using.

import sys
import yaml

import inflect

from active_record.connection_adapters import *

# Provide a single inflector for all of active_record to use
INFLECTOR = inflect.engine()

# Retrieve the requested execution environment, or default to development
ENV = 'development'
if '--env' in sys.argv:
  ENV = sys.argv[sys.argv.index('--env')+1]

# Load the database configuration file
db_config = yaml.load(open('./database.yaml'))[ENV]

# Import the specified database adapter
__connection_adapters = __import__('connection_adapters', locals(), globals())
# getattr() works as an "import" for submodules. It's complicated, but simple.
db_module = getattr(__connection_adapters, db_config['adapter']+'_adapter')

# Connect to the database.
DATABASE = db_config['name']
DB_ADAPTER = db_module.new(db_config)
