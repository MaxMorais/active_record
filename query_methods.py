import copy

# Query Methods
#
# These methods wrap methods from inside of arel's Table class to provide a
# cleaner interface for the user. As these methods map *directly* to their arel
# counterparts, no documentation will be provided here. Instead, check
# arel/table.py for information about syntax and usages.
#
# All methods here return isntances of model objects, meaning they can be
# interpreted as objects from the database. However, the instances that are
# returned have no attribute information. To get the appropriate records, use
# one of the finder methods.
#
# I blame Python's lack of native delegation and cloning for this.

def select(self, *columns):
  self.arel_table = self.arel_table.select(*columns)
  return self

def includes(self, *tables):
  self.arel_table = self.arel_table.includes(*tables)
  return self

def where(self, **conditions):
  self.arel_table = self.arel_table.where(**conditions)
  return self

def order(self, defaults, **conditionals):
  self.arel_table = self.arel_table.order(defaults, **conditionals)
  return self

def group(self, *columns):
  self.arel_table = self.arel_table.group(*columns)
  return self

def having(self, *statements, **conditions):
  self.arel_table = self.arel_table.having(*statements, **conditions)
  return self

def join(self, to=None, on={}, join_type='INNER', cols=[]):
  self.arel_table = self.arel_table.join(to, on, join_type, cols)
  return self

def limit(self, value):
  self.arel_table = self.arel_table.limit(value)
  return self

def offset(self, value):
  self.arel_table = self.arel_table.offset(value)
  return self

def reverse(self):
  self.arel_table = self.arel_table.reverse()
  return self

