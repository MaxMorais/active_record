import active_record.helpers as helpers
import active_record.arel    as arel
from active_record.relation  import Relation

# Methods that are called from a class, but do not actually belong to it.
# This includes validates(), belongs_to(), has_one(), has_many(), etc.
from active_record.macros import *

# Pyton doesn't really support delegation without some comlpex and ugly stuff,
# so it's simpler to subclass and "delegate" methods by not redefining them.
class Base(Relation):
  # These attributes are reserved for use by active record internals. They are
  # not available to be used as column (attribute) names.
  reserved_attributes = [
    'record', 'exists', 'table_name', 'arel_table', 'model', 'relation',
    'associations'
  ]

  # Create a new instance of this model which will represent the record that is
  # passed in.
  #
  # exists should only be set to True if this record already exists in the
  # database.
  #
  # This method should not be used directly. Instead use .new(), .create(), or
  # any of the finder methods. They handle creating the record object which gets
  # passed in, as well as any other set up that needs to take place.
  def __init__(self, record=None, exists=False):
    self.record = record
    self.exists = exists
    self.model  = self.__class__


  # Return a string representation of all of the attributes of this model.
  #
  # Used for basic inspection. No types are included.
  def __str__(self):
    _str = '#<%s ' % self.__class__.__name__

    attributes = []
    for attr, value in self.record.values.iteritems():
      attributes.append('%s: %s' % (attr, value))

    _str += ', '.join(attributes)
    return _str + '>'


  # Dynamic attribute accessors.
  #
  # The following two methods provide dynamic access to the columns of the table
  # that this object represents through a natural syntax. The goal is to require
  # minimal effort from the user, as well as from the model designer. Any
  # attribute that is loaded from the database will be accessible by simply
  # using the .<attribute> syntax.
  #
  # For an example, let's say we have a `Person` model which represents the
  # `people` table in our database. This table has the columns `name`, `age`,
  # and `email`. If we have a `Person` instance, jon, we can access these fields
  # via dot referencing:
  #       jon.name   #=> 'Jon'
  #       jon.age    #=> 18
  #       jon.email  #=> 'jon@example.com'
  #
  # If we want to set the field's value, we can use the same dot referencing
  # syntax, combined with natural assignment.
  #       jon       #=> ('Jon', 18, 'jon@example.com')
  #       jon.name  = 'John'
  #       jon.age   = 25
  #       jon.email = 'john@example.com'
  #       jon       #=> ('John', 25, 'john@example.com')
  #
  # Associations are handled by these methods as well.
  #
  # These fields are not type-casted by ActiveRecord, but instead by the
  # database adapter. Because of this, the range of types that are available is
  # limited primarily by the adapter implementation. In the case of sqlite3 (the
  # default), we get strings, integers, booleans, floats, and date/(time)s.
  # For more information, look in the `connection_adapters` package for your
  # adapter.
  def __getattr__(self, name):
    if name.startswith('__'):
      raise AttributeError(name)
    elif name in self.reserved_attributes:
      return self.__dict__[name]

    # First handle mirrored attributes.
    if name in self.record.values:
      return self.record.values[name]
    # Then handle association accesses.
    elif hasattr(self, 'associations') and name in self.associations:
      return self.associations[name].get_association(self)
    # Otherwise, raise an appropriate error.
    else:
      raise AttributeError('"%s" is not an attribute of model "%s"' % (name, self.__class__.__name__))


  def __setattr__(self, name, value):
    # Set reserved attributes appropriately.
    if name in self.reserved_attributes:
      object.__setattr__(self, name, value)
    # Set association values properly
    elif hasattr(self, 'associations') and name in self.associations:
      self.associations[name].set_association(self, value)
    # Everything else gets put into the record attribute's values dictionary.
    else:
      self.__dict__['record'].values[name] = value



  # A simple meta class for active record, used to dynamicaly define class
  # level attributes, such as the table name for the model. These can all be
  # overriden by simply redefining the attribute in the model definition.
  class __metaclass__(type):
    def __new__(meta, name, bases, dict):
      cls = type.__new__(meta, name, bases, dict)
      cls.table_name = helpers.make_table_name(name)
      cls.arel_table = arel.Table.new(cls.table_name)

      return cls


    # Return an instance of Relation, including all of the necessary information
    # to "bind" it to this model.
    #
    # Blame: Python's inability to declare class and instance methods with the
    # same name. Luckily, we can make it a property to at least look a little
    # cleaner:
    #     Person.relation.where(name="Jon").all()
    @property
    def relation(cls):
      return Relation(cls.table_name, cls.arel_table, cls)

