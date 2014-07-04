from active_record.setup import *
import re

# Helpers
#
# General-use functions which provide a simple interface for tedious tasks. For
# the most part, these functions are used by active record internals, but are
# also available publicly. Use these if you want to replicate some of the
# behavior of active record.

# Pluralize and parameterize the provided name to create an appropriate table
# name.
#
# Mainly used by models to determine which table they should be talking to.
def make_table_name(name):
  return INFLECTOR.plural(re \
            .sub(r"\B([A-Z])", r" \1", name) \
            .lower() \
            .replace(' ', '_')
  )

# Take the given name (generally the singular form of a table name) and return
# the corresponding class name (non-parameterized, CamelCase name).
def make_class_name(name):
  return name.replace('_', ' ').title().replace(' ', '')


# Return the names of the columns of the table with the given name.
def get_column_names(table_name):
  return map(lambda col: col[1], DB_ADAPTER.table_structure(table_name).values.values())
