from collections import OrderedDict

import active_record as AR

class Table():
  def __init__(self, table_name):
    self.table_name = table_name

  # Returns a new Table instance with the default values set for all attributes.
  @classmethod
  def new(cls, table_name):
    self = cls(table_name)
    self.projections = OrderedDict(**{ table_name: ['*'] })
    self.value_set   = []
    self.sets        = OrderedDict()
    self.aggregates  = []
    self.froms       = [self.table_name]
    self.wheres      = OrderedDict()
    self.orders      = []
    self.groups      = []
    self.havings     = { }
    self.joins       = { }
    self.limits      = None
    self.offsets     = None
    self.locks       = False
    self.unions      = []

    return self

  # Return a copy of this table instance.
  def copy(self):
    table = Table(self.table_name)

    table.projections = self.projections.copy()
    table.value_set   = self.value_set[:]
    table.sets        = self.sets.copy()
    table.aggregates  = self.aggregates[:]
    table.froms       = self.froms[:]
    table.wheres      = self.wheres.copy()
    table.orders      = self.orders[:]
    table.groups      = self.groups[:]
    table.havings     = self.havings.copy()
    table.joins       = self.joins.copy()
    table.limits      = self.limits
    table.offsets     = self.offsets
    table.locks       = self.locks
    table.unions      = self.unions[:]

    return table


  # Specify that the database adapter should only ask the database for these
  # fields. Query functions can also be provided, such as COUNT(*). The optional
  # keyword argument of `table_name` specifies from which table the columns
  # should be selected. For distinct values, prepend "distinct " (case-
  # insensitive) in the column name. For aliasing, append " as <alias>" in the
  # column name.
  def select(self, *cols, **kwargs):
    copy = self.copy()

    table_name = copy.table_name
    if 'table_name' in kwargs:
      table_name = kwargs['table_name']

    if table_name not in copy.projections:
      copy.projections[table_name] = []

    if '*' in cols:
      copy.projections[table_name] = ['*']
    else:
      for col in cols:
        copy.projections[table_name].append(col)
    return copy

  # Specify that the database adapter should include the listed tables in the
  # query. This allows for more flexible where statements, as well as expanded
  # selections. Note that the columns of the included table will not be included
  # in the select portion of the query.
  def includes(self, *tables):
    copy = self.copy()

    for table in tables:
      copy.froms.append(table)

    return copy

  # A slightly more restrictive form of .select(). Only accounts for explicit
  # column names to be used. Useful for inserts where .columns() is more
  # semantically correct than .select()
  def columns(self, *cols):
    return self.select(*cols, table_name=self.table_name)

  # Specify that the database adapter should, on an insert or update query,
  # include the values specified here. `values` is a tuple inline with each
  # value inline with the columns it will be placed in. Should only be used with
  # an adjacent call to .column(). Setting `replace` will clear the existing
  # values list for this instance.
  def values(self, *values, **kwargs):
    copy = self.copy()

    if 'replace' in kwargs and kwargs['replace']:
      copy.value_set = []
    copy.value_set.append(values)

    return copy

  # Specify that the database adapter should, on an update query, assign the
  # provided attributes with new values as given here.
  def set(self, **assignments):
    copy = self.copy()

    for column, value in assignments.iteritems():
      copy.sets[column] = value

    return copy

  # Specify that the database adapter should include these aggregate functions
  # in the select statement of the query. Aggregates are interpreted as-is, and
  # thus must be holistic in their contents.
  def aggregate(self, *aggregates):
    copy = self.copy()

    for aggregate in aggregates:
      copy.aggregates.append(aggregate)

    return copy

  # Specify that the database adapter should only retrieve records which pass
  # these conditions. The conditions are stored in a dictionary with a syntax
  # that allows for more supported conditions, but is not necessarily semantic.
  #
  # The following operations are supported by this syntax:
  #   - =             .where(age=18)
  #   - !=            .where(age=(18,))         # Single-value tuple
  #   - <=            .where(age=(None,20))
  #   - >=            .where(age=(10,None))     # None must be provided
  #   - BETWEEN       .where(age=(10,20))
  #   - IN            .where(age=[10, 15, 20])
  #
  # Everything else is done with direct sql queries:
  #   .where('name LIKE "J%"')
  # These should be avoided, however, since they are not database-agnostic.
  #
  # Multiple conditions can all be combined into one call:
  #   .where('name LIKE "J%"', age=(12,14), grade=8, mile_time=(60,))
  def where(self, *statements, **conditions):
    copy = self.copy()

    for statement in statements:
      if 'sql' not in copy.wheres:
        copy.wheres['sql'] = []
      copy.wheres['sql'].append(statement)

    for column, condition in conditions.iteritems():
      if column not in copy.wheres:
        copy.wheres[column] = []
      copy.wheres[column] = condition
    return copy

  # Specify that the database adapter should arrange the records by these
  # columns, with priority going from first to last specified.
  #
  # All string arguments will default to sorting in ascending (asc) order. To
  # change the order, provide a named parameter with a value of the desired
  # order as a string. Ex: .order('name', score='desc')
  def order(self, *defaults, **conditionals):
    copy = self.copy()

    for field in defaults:
      copy.orders.append('%s ASC' % field)

    for field, direction in conditionals.iteritems():
      copy.orders.append('%s %s' % (field, direction.upper()))
    return copy

  # Specify that the database adapter should group records which have matching
  # values for all of the specified columns.
  def group(self, *columns):
    copy = self.copy()

    for field in columns:
      copy.groups.append(field)
    return copy

  # Specify that the database adapter should only return groups which pass these
  # conditions. The grouping equivalent of a .where() call. As such, the values
  # and arguments will follow the same structure. For distinct values, include
  # the word "distinct" (case-insensitive) in the column name.
  def having(self, *statements, **conditions):
    copy = self.copy()

    for statement in statements:
      if 'sql' not in copy.havings:
        copy.havings['sql'] = []
      copy.havings['sql'].append(statement)

    for column, condition in conditions.iteritems():
      if column not in copy.havings:
        copy.havings[column] = []
      copy.havings[column] = condition
    return copy

  # Specify that the database adapter should perform the specified join between
  # two tables, and only return the records which pass the join. The cols
  # arguments specify which columns should be retrieved from the joined table,
  # in the same way that .select() does.
  def join(self, to=None, on={}, join_type='INNER', cols=[]):
    copy = self.copy()

    # Default to matching on the ID of the other table
    if 'this' not in on:
      on['this'] = 'id'
    if 'that' not in on:
      on['that'] = AR.INFLECTOR.singular_noun(self.table_name)+'_id'

    copy.joins.update({ to: { 'type': join_type, 'on': on } })

    # Get all columns unless otherwise restricted.
    if not cols:
      cols = ['*']
    # Add the requested columns and return.
    return copy.select(*cols, table_name=to)

  # Specify that the database adapter should only return this many results from
  # the result set of the query. This limitation should only be applied after
  # all other operations have been performed.
  def limit(self, value):
    copy = self.copy()

    copy.limits = value
    return copy

  # Specify that the database adapter should skip this many rows into the result
  # set before starting the limit. The n-th row of the result set (where n is
  # the offset value), will be the first record returned.
  def offset(self, value):
    copy = self.copy()

    copy.offsets = value
    return copy

  # Specify that the database adapter should not allow any other operations
  # while this query is taking place. Most DBMSs do this natively, others
  # require the statement explicitly in the query.
  def lock(self, value):
    copy = self.copy()

    copy.locks = value
    return copy

  # Specify that the database adapter should perform a UNION query combining
  # this and other's queries into a single statement. Unless otherwise specified
  # by a database adapter, a UNION ALL will be performed.
  def union(self, *unions):
    copy = self.copy()

    for union in unions:
      copy.unions.append(union)
    return copy


  # No argument methods

  # Reverse the direction of each of the order clauses. Useful when trying to
  # find the last records in a large result set.
  def reverse(self):
    copy = self.copy()

    copy.orders = []
    for order in self.orders:
      if 'ASC' in order:
        copy.orders.append(order.replace('ASC', 'DESC'))
      else:
        copy.orders.append(order.replace('DESC', 'ASC'))

    return copy
