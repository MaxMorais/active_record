__all__ = ['sqlite3_adapter']

from active_record.result import Result

# A base definition of a connection adapter. An effective adapter must implement
# all of the included methods, if not more.
class AbstractAdapter(object):
  # A dictionary which maps Python types into types for the database. It should
  # contain at least the types which are defined here.
  _type_map = {
    'boolean':   None,
    'integer':   None,
    'decimal':   None,
    'string':    None,
    'text':      None,
    'date':      None,
    'time':      None,
    'datetime':  None,
    'timestamp': None
  }

  # A tuple of 2-tuples pairing different attributes of a database column to
  # their appropriate type. Used by .table_structure() to return an appropriate
  # Result object.
  _table_structure_tuple = (
    ('id', 'integer'),
    ('name', 'string'),
    ('type', 'string'),
    ('not_null', 'boolean'),
    ('default', 'string'),
    ('primary_key', 'boolean')
  )

  def __init__(self):
    # As an abstract adapter, we can't connect to anything.
    self.conn = None
    self.cursor = None

  # This method should query the database with the given sql, returning the
  # results casted into Result objects.
  def query(self, sql):
    raise Exception("ABSTRACT PERFORMING QUERY")

  def begin_transaction(self):
    raise Exception("ABSTRACT BEGINNING TRANSACTION")

  def end_transaction(self):
    raise Exception("ABSTRACT ENDING TRANSACTION")

  # TABLE METHODS
  def create_table(self, table_def, force=False):
    raise Exception("ABSTRACT CREATING TABLE")

  def drop_table(self, table_name):
    raise Exception("ABSTRACT DROPPING TABLE")

  # The result of this function should be list of 6-tuples that follow the
  # format of _table_structure_tuple.
  def table_structure(self, table_name):
    raise Exception("ABSTRACT GETTING TABLE STRUCTURE")

  # Return the ID of the last row that was inserted.
  def last_inserted(self):
    raise Exception("ABSTRACT GETTING LAST INSERTED")



  # DATA METHODS
  # This method should create a list of Result objects from the data provided.
  def results(self, records):
    return Result.parse_all(records)

  # For the remaining methods:
  #   - ast is the Abstract Syntax Tree (arel object)
  # These methods are responsible for converting ast into executable SQL,
  # and then executing that query.
  def find(self, ast):
    raise Exception("ABSTRACT SELECTING STUFF")

  def insert(self, ast, insert_clause="INSERT", defaults=False, commit=True):
    raise Exception("ABSTRACT INSERTING STUFF")

  def update(self, ast, update_clause="UPDATE", commit=True):
    raise Exception("ABSTRACT UPDATING STUFF")

  def delete(self, ast, commit=True):
    raise Exception("ABSTRACT DELETING STUFF")



  # HELPERS
  # Return the appropriate SQL representing the type definition tuple provided.
  # Should not need to be overridden.
  def _type_to_db(self, type_def):
    sql = ''
    # For this adapter, this will always return None. However, if a subclassing
    # adapter implements its _type_map correctly, this should return the correct
    # SQL for using the type
    return self._type_map[type_def[0]].format(*type_def[1:])

  # Returns a string representing the Python type that this type maps to.
  def _type_from_db(self, db_type):
    raise Exception("ABSTRACT INFERRING TYPE FROM DATABASE")

  # Returns a string representing the value as the database type that
  # corresponds to the given type.
  def _type_casted(self, typ, value):
    raise Exception("ABSTRACT CASTING VALUE TO TYPE")


# Each adapter module should re-implement this method to provide a common
# name for instantiating the adapter. It should instantiate a new adapter
# object and return it. Nothing else.
def new(db_config):
  pass
