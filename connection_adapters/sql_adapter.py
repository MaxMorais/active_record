import time
import datetime
import re

from active_record import arel
from active_record.connection_adapters import AbstractAdapter
from active_record.result import Result

# A proxy object which provides some general functions for AST conversion into
# SQL statements. It cannot be used on its own, as no connection is established.
# If any subclassing adapter finds that this class creates improper SQL, it
# should override all necessary functions, rather than rely on this class.
#
# This class also assumes that this class's database connection object follows
# the Python DB API v2 (PEP 249): http://legacy.python.org/dev/peps/pep-0249/.
# If so, subclasses which strictly follow SQL syntax (MySQL,SQLite, etc.) will
# have very little work left SQLite3Adapter gives an example.
class SQLAdapter(AbstractAdapter):
  # Individual adapters must set their own type maps and reverse type maps.
  _type_map = { }

  # Every database call is done using .query(). It is also publicly available to
  # the client if it needs more direct control in querying.
  #
  # In essence, this method performs the query on the database and casts the
  # returned records into a list of Result objects using the .results() method.
  def query(self, sql):
    return self.results(self.cursor.execute(sql))

  def begin_transaction(self):
    # For some reason, DB API 2 doesn't call for a .begin() method...
    self.cursor.execute('BEGIN')

  def end_transaction(self):
    # It does call for a .commit(), but it's on the connection object? Really?
    self.conn.commit()

  # TABLE METHODS
  def create_table(self, table_def, force=False):
    _force = ''
    if not force:
      _force = 'IF NOT EXISTS'
    sql = """CREATE TABLE %s %s""" % (_force, self._table_sql(table_def))
    self.cursor.execute(sql)

  def drop_table(self, table_name, force=False):
    _force = ''
    if not force:
      _force = 'IF EXISTS'
    sql = """DROP TABLE %s %s""" % (_force, table_name)
    self.cursor.execute(sql)

  def table_structure(self, table_name):
    sql = """PRAGMA table_info(%s)""" % table_name

    structure = []
    # The only thing we have to change in each record tuple is the type.
    for r in self.cursor.execute(sql):
      structure.append((r[0], r[1], self._type_from_db(r[2]), r[3], r[4], r[5]))

    column_names = [name for name, _ in self._table_structure_tuple]
    return Result(column_names, structure)

  def last_inserted(self):
    return self.cursor.lastrowid



  # DATA METHODS
  def find(self, ast):
    return self.query(self._build_find_sql(ast))

  def insert(self, ast, insert_clause="INSERT", defaults=False, commit=True):
    results = self.query(self._build_insert_sql(ast, insert_clause, defaults))
    if commit:
      self.conn.commit()

    return results

  def update(self, ast, update_clause="UPDATE", commit=True):
    results = self.query(self._build_update_sql(ast, update_clause))
    if commit:
      self.conn.commit()

    return results

  def delete(self, ast, commit=True):
    results = self.query(self._build_delete_sql(ast))
    if commit:
      self.conn.commit()

    return results



  # HELPERS
  # Return the SQL that defines this table and its columns.
  def _table_sql(self, table_def):
    columns = []
    # Trickery to ensure a comma is included in the column definition list.
    # If a foreign key exists for this table, the blank element will ensure that
    # a leading comma is inserted, which is necessary in the definition format.
    #     Result:  <columns>, <foreign_keys>
    foreign_keys = ['']

    for name, options in table_def.columns.iteritems():
      if 'foreign_key' in options:
        foreign_keys.append('%s' % self._foreign_key_sql(name, options['foreign_key']))

      column_def = '%s %s' % (name, self._type_to_db(options['type_def']))
      if options['primary_key']:
        column_def += ' PRIMARY KEY'
      if options['not_null']:
        column_def += ' NOT NULL'
      if options['unique']:
        column_def += ' UNIQUE'
      if options['default']:
        column_def += ' DEFAULT %s' % self._type_casted(options['type_def'][0], options['default'])
      columns.append(column_def)

    return "%s (%s%s)" % (table_def.name, ', '.join(columns), ', '.join(foreign_keys))

  # Type definitions in SQL are consistent, meaning each defintion is either the
  # type itself, or the type followed by "(<options>)". This means we can split
  # the type strings on the first parenthesis and compare for equality.
  def _type_from_db(self, sql_type):
    for _type, _def in self._type_map.iteritems():
      if sql_type.split('(')[0] == _def.split('(')[0]:
        return _type

  # Cast arel types into DB-safe values
  def _type_casted(self, typ, value):
    if typ == 'string' or typ == 'text':
      return '"%s"' % self._escaped(value)
    if typ == 'date':
      return value.strftime('%Y-%m-%d')
    if typ in ['time', 'datetime']:
      return value.strftime('%Y-%m-%d %H:%M:%S')
    return value

  # Return the appropriate SQL for defining a foreign key column, including the
  # column definition itself.
  def _foreign_key_sql(self, key_name, key_def):
    sql = """FOREIGN KEY (%s) REFERENCES %s(%s)""" % (key_name, key_def['table'], key_def['column'])

    if 'on_delete' in key_def:
      sql += ' ON DELETE %s' % key_def['on_delete'].upper()
    if 'on_update' in key_def:
      sql += ' ON UPDATE %s' % key_def['on_update'].upper()

    return sql


  # SQL Statement builders
  def _build_find_sql(self, ast):
    sql = self._build_select(ast)
    sql += self._build_from(ast)

    if ast.joins:
      sql += self._build_join(ast)
    if ast.wheres:
      sql += self._build_where(ast)
    if ast.groups:
      sql += self._build_group(ast)
    if ast.orders:
      sql += self._build_order(ast)
    if ast.havings:
      sql += self._build_having(ast)
    if ast.limits:
      sql += self._build_limit(ast)
    if ast.offsets:
      sql += self._build_offset(ast)
    if ast.locks:
      sql += self._build_lock(ast)
    if ast.unions:
      sql += self._build_union(ast)

    return sql

  def _build_insert_sql(self, ast, clause, defaults):
    sql = """%s INTO %s""" % (clause, ast.table_name)
    sql += self._build_columns(ast.projections[ast.table_name])
    if defaults:
      sql += """ DEFAULT VALUES"""
    else:
      sql += """ VALUES (%s)""" % self._build_values(ast)

    return sql

  def _build_update_sql(self, ast, clause):
    sql = """%s %s""" % (clause, ast.table_name)
    sql += self._build_set(ast)
    if ast.wheres:
      sql += self._build_where(ast)

    return sql

  def _build_delete_sql(self, ast):
    sql = """DELETE"""
    sql += self._build_from(ast)
    if ast.wheres:
      sql += self._build_where(ast)

    return sql


  # SQL expression builders, responsible for building each part of a query.
  def _build_select(self, ast):
    statements = []
    for table, fields in ast.projections.iteritems():
      for field in fields:
        statements.append(table+'.'+field)

    for aggregate in ast.aggregates:
      statements.append(aggregate)

    if not statements:
      statements.append('*')

    return """SELECT %s""" % ', '.join(statements)

  def _build_columns(self, columns):
    return """ (%s)""" % ', '.join(columns)

  # By using OrderedDicts, we are guaranteed that the columns will be in the
  # correct order, thus no checks are required.
  def _build_values(self, ast):
    statements = []

    for values in ast.value_set:
      casted_vals = [self._casted(value) for value in values]
      statements.append(','.join(casted_vals))

    return ', '.join(casted_vals)

  def _build_set(self, ast):
    statements = []
    for column, value in ast.sets.iteritems():
      statements.append('%s = %s' % (column, self._casted(value)))

    return """ SET %s""" % ', '.join(statements)

  def _build_from(self, ast):
    return """ FROM %s""" % ', '.join(ast.froms)

  def _build_where(self, ast):
    return """ WHERE %s""" % ' AND '.join(self._build_conditionals(ast.table_name, ast.wheres))

  def _build_order(self, ast):
    # Order by the ID of the record, but only if no other ordering is specified.
    if not ast.orders:
      ast.orders.append('id ASC')

    return """ ORDER BY %s""" % ', '.join(ast.orders)

  def _build_group(self, ast):
    return """ GROUP BY %s""" % ', '.join(ast.groups)

  def _build_having(self, ast):
    return """ HAVING %s""" % ' AND '.join(self._build_conditionals(ast.table_name, ast.havings))

  def _build_join(self, ast):
    statements = []

    for table, join in ast.joins.iteritems():
      on = ast.table_name+'.'+join['on']['this'] + ' = ' + table+'.'+join['on']['that']
      statements.append(' %s JOIN %s ON %s' % (join['type'], table, on))

    return ' '.join(statements)

  def _build_limit(self, ast):
    return """ LIMIT %d""" % ast.limits

  def _build_offset(self, ast):
    return """ OFFSET %d""" % ast.offsets

  # By default, SQL DBMSs lock tables for updates, so no need to add a
  # statement here.
  def _build_lock(self, ast):
    return ""

  def _build_union(self, ast):
    statements = []
    for union in ast.unions:
      statements.append(""" UNION ALL (%s)""" % self._build_select_sql(union))

    return ''.join(statements)

  # Both WHERE, HAVING, and SET use the same conditionals, so they are
  # abstracted here. This is fairly complicated because of the way that the
  # clauses are structured. Each kind of statement is labeled below, so you
  # might be able to figure it out. For reference, check arel/table.py#where
  def _build_conditionals(self, table_name, conditionals):
    statements = []

    for field, conditions in conditionals.iteritems():
      # Build the direct sql statements
      if field == 'sql':
        for stmt in conditions:
          statements.append(stmt)
        continue

      # Build as binary-operated inequality statement
      statement = table_name+'.'+field
      if isinstance(conditions, tuple):
        # Not equal
        if len(conditions) == 1:
          statement += ' != %s' % conditions[0]
        # Between
        elif conditions[0] and conditions[1]:
          statement += ' BETWEEN %s AND %s' % \
              (self._casted(conditions[0]), self._casted(conditions[1]))
        # Greater than
        elif conditions[0] and not conditions[1]:
          statement += ' >= %s' % self._casted(conditions[0])
        # Less than
        elif not conditions[0] and conditions[1]:
          statement += ' <= %s' % self._casted(conditions[1])

      # Build as IN (list) condition
      elif isinstance(conditions, list):
        statement += ' IN (%s)' % ','.join(map(self._casted, conditions))

      # Build as an equality condition
      else:
        statement += ' = %s' % self._casted(conditions)

      # Append this condition to the list
      statements.append(statement)

    return statements

  # Different from ._type_casted() in that this casts actual python types into
  # DB-safe values.
  def _casted(self, value):
    if isinstance(value, int): # bool and timestamp are captured here as well
      return '%d' % value
    if isinstance(value, str):
      valu = self._escaped(value)
    if type(value) is datetime.date:
      value = self._type_casted('date', value)
    if type(value) in (datetime.datetime, time.time):
      value = self._type_casted('time', value)

    # None-type values need to be casted specifically to `NULL` (no quotes)
    if value == None:
      return 'NULL'
    return '"%s"' % value

  # Escape strings so that queries are not unexpectedly stopped by string values
  # containing special characters (any kind of quote, backslashes, etc.).
  # We are using `re` because it does the job well enough and making sure you
  # catch every case is difficult.
  def _escaped(self, value):
    if type(value) in [str]:
      return re.escape(value)
    return value
