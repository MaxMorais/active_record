from active_record.setup import *
from active_record.result import Result

# Finder Methods
#
# These methods provide most of the querying interface for retrieving
# information from the database.
#
# These methods should be the only way by which new instances of a model are
# created.

# Retrieve the record which has an id matching the given one. Limited to one
# result.
@classmethod
def find(cls, row_id):
  arel_table = cls.arel_table.where(**{ 'id': row_id }).limit(1)
  found = DB_ADAPTER.find(arel_table)

  if found:
    inst = cls(found[0],exists=True)
    return inst

# Similar to find, but with any condition. Limited to one result. References for
# syntax are located at arel/table.py#where
@classmethod
def find_by(cls, **attrs):
  arel_table = cls.arel_table.where(**attrs).limit(1)
  found = DB_ADAPTER.find(arel_table)

  if found:
    inst = cls(found[0],exists=True)
    return inst


# Search for a record with the given attributes. If none exists, create a new
# model instance with the same attributes.
@classmethod
def find_or_new(cls, **attrs):
  found = cls.find_by(**attrs)

  if found:
    return found
  return cls.new(**attrs)

# Similar to .find_or_new(), but calling .create() instead.
@classmethod
def find_or_create(cls, **attrs):
  found = cls.find_by(**attrs)

  if found:
    return found
  return cls.create(**attrs)


# The remaining methods are purposely not labeled as class methods as they
# can only be called on Relation objects.
#
# In other words, doing this forces proper usage.

# Return all records which match the current query.
#
# .all() is essentially an immutable attribute, so it is available as such,
# meaning also that you can not call this attribute as a normal method. Instead
# use the normal dot referencing syntax:
#
#     Person.all #=> [<list of Person objects>]
@property
def all(self):
  return [self.model(obj,True) for obj in DB_ADAPTER.find(self.arel_table)]

# Return only the first record (or n records) which match the current query.
def first(self, n=1):
  found = [self.model(f,True) for f in DB_ADAPTER.find(self.arel_table.limit(n))]
  if not found:
    return None
  if n == 1:
    return found[0]
  return found

# Return only the last record (or n records) which match the current query.
def last(self, n=1):
  found = [self.model(f,True) for f in DB_ADAPTER.find(self.arel_table.reverse().limit(n))]

  if not found:
    return None
  if n == 1:
    return found[0]
  return found
