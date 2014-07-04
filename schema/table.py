# Pick up the constants from active_record
from active_record.setup import *

# An object representation of a potential table in a database
class Table(object):
  def __init__(self, name):
    # The name that this table will be saved and referenced as
    self.name = name

    # Column types are specified using tuples and are stored in a dictionary
    # keyed by the name of the column. All columns follow the same spec:
    #   {
    #     <name>: {
    #       type_def: (<type> [, <type_options> ] )
    #       [, primary_key: False | True        ]
    #       [, not_null: False | True           ]
    #       [, unique: False | True             ]
    #       [, default: None | True | <literal-value>  ]
    #       |
    #       foreign_key: None | {
    #         table: <foreign_table>,
    #         column: <foreign_column>
    #         [, on_delete: 'set null' | <on_delete_action> ]
    #         [, on_update: None | <on_update_action> ]
    #       }
    #   }
    #
    # A default value of `True` tells the database adapter to use a "current"
    # value as the default for that column. This is useful for date, time, and
    # datetime columns like `created_at` or `updated_at`, where a constant
    # default value is inappropriate.
    #
    # Any type that can be matched by your database adapter is supported, so
    # long as all of the necessary options can be satisfied. The default types
    # supported by the type-specific functions below are:
    #     - integer
    #     - decimal
    #     - string
    #     - text
    #     - date
    #     - time
    #     - datetime
    #     - timestamp
    self.columns = { }

    # ID is included by default as the primary key for the table. To remove it
    # from a table, include .remove_column('id') in your schema definition. To
    # modify it, use .change_column('id', int, [options]) instead.
    self.integer('id', not_null=True, primary_key=True)


  # The default way of setting an arbitrary column in the table. Useful, clear,
  # and powerful, but not concise. See the type-specific functions below for
  # alternative ways to add columns
  def add_column(self, name, type_def=None, default=None, unique=False, \
                 not_null=False, primary_key=False, foreign_key=None):
    # Add the column to this table.
    self.columns[name] = {
      'type_def': type_def,
      'default': default,
      'unique': unique,
      'not_null': not_null,
      'primary_key': primary_key
    }

    # The foreign key is not commonly used, so only add if it's present.
    if foreign_key:
      self.columns[name]['foreign_key'] = foreign_key

  # An alias for add_column
  def change_column(self, name, type_def, **options):
    options['type_def'] = type_def
    self.add_column(name, **options)

  # Remove the specified column from the table.
  def remove_column(self, name):
    del self.columns[name]


  # Type-specific column creation functions
  # Add a boolean column to the table.
  def boolean(self, name, **options):
    options['type_def'] = ('boolean',)
    self.add_column(name, **options)

  # Add an integer column to the table. If 'primary key' is in the options list,
  # the column will be set to autoincrement ascending. Size is the number of
  # digits this integer can contain
  def integer(self, name, size=8, **options):
    options['type_def'] = ('integer', size)
    self.add_column(name, **options)

  # Add an decimal column to the table. The default size has a maximum
  # value of 999,999.99. Precision is the total number of digits, scale is the
  # number of those digits to come after the decimal point. Pyton does not have
  # a fixed-point type, so the column will be read as a float.
  def decimal(self, name, precision=8, scale=2, **options):
    options['type_def'] = ('decimal', precision, scale)
    self.add_column(name, **options)

  # Add a string column to the table. In most DBMSs, this translates to a
  # varchar, but that translation is left for the adapter to perform.
  def string(self, name, max_length=30, **options):
    options['type_def'] = ('string', max_length)
    self.add_column(name, **options)

  # Add a text column to the table. Text differs from string in that it is
  # better suited for large sections of text, such as object dumps, or long
  # descriptions. Some DBMSs use the same type for both text and string, but
  # most do not, thus they are separated.
  def text(self, name, max_length=500, **options):
    options['type_def'] = ('text', max_length)
    self.add_column(name, **options)

  # (Next 4 functions) Add a time-related column to the field.
  def date(self, name, **options):
    options['type_def'] = ('date',)
    self.add_column(name, **options)

  def time(self, name, **options):
    options['type_def'] = ('time',)
    self.add_column(name, **options)

  def datetime(self, name, **options):
    options['type_def'] = ('datetime',)
    self.add_column(name, **options)

  def timestamp(self, name, **options):
    options['type_def'] = ('timestamp',)
    self.add_column(name, **options)

  # Adds a foreign key to this table. The type of association is irrelevant, and
  # will be determined later by the model definition. Note that the options
  # dictionary here is only for the foreign_key options, and should not contain
  # any other constraint information.
  def references(self, model, column='id', name=None, **options):
    options['table'] = INFLECTOR.plural(model)
    options['column'] = column
    type_def = ('integer',)
    if not name:
      name = model+'_id'
    self.add_column(name, type_def, foreign_key=options)
