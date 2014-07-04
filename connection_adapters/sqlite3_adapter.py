import sqlite3

from active_record.connection_adapters.sql_adapter import SQLAdapter

class SQLite3Adapter(SQLAdapter):
  _type_map = {
    'boolean':   """BOOL""",
    'integer':   """INTEGER""",
    'decimal':   """DECIMAL({0},{1})""",
    'string':    """VARCHAR({0})""",
    'text':      """TEXT({0})""",
    'date':      """DATE""",
    'time':      """TIME""",
    'datetime':  """DATETIME""",
    'timestamp': """TIMESTAMP"""
  }

  def __init__(self, db_name):
    self.conn = sqlite3.connect(db_name, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    self.conn.text_factory = str # Disregard unicode values, typecast as str()
    self.cursor = self.conn.cursor()

def new(db_config):
  return SQLite3Adapter(db_config['name'])
