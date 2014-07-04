from datetime import date, datetime
from time import time

# An object representation of a record retrieved by a database adapter. It is
# on the database adapter to instantiate these objects when returning results.
class Result(object):
  # Parse the data provided into a list of Result objects. Records is the cursor
  # object returned from query execution, containing the data to be placed into
  # each Result object.
  @classmethod
  def parse_all(self, records):
    if not records.description:
      return []
    columns = [col[0] for col in records.description]
    return [Result(columns, record) for record in records]

  # Return a new Result object from the provided attribute dictionary. This
  # allows for database-less creation, as performed by the relation methods.
  @classmethod
  def from_attrs(self, attributes):
    return Result(attributes.keys(), attributes.values())


  # Create a Result object which stores both column and row information for the
  # record provided. record is a tuple of values, columns is a list of 2-tuples
  # defining the type for each value.
  def __init__(self, columns, record):
    self.values = { name: val for name, val in zip(columns, record) }
