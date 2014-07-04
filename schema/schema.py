from active_record.setup import *
from active_record.schema.table import Table

class Schema(object):
  def __init__(self):
    self.tables = {}

  # Initialize a new table object, append it to the list, and return it for
  # the user to interact with.
  def create_table(self, table_name):
    self.tables[table_name] = Table(table_name)
    return self.tables[table_name]

  def load(self, force=False):
    for name, table in self.tables.iteritems():
      if force:
        DB_ADAPTER.drop_table(name)
      DB_ADAPTER.create_table(table)


  ### TESTING ###
  def print_schema(self):
    for table_name, table_def in self.tables.iteritems():
      print '%s:' % table_name
      for name, attrs in table_def.columns.iteritems():
        print '\t%s: %s' % (name, attrs)
      print '\n'
