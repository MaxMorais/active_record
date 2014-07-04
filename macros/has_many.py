import sys
import importlib

import active_record.helpers as helpers
from active_record.setup import INFLECTOR

# Add a multiple child association to the referencing class.
#
# The child parameter is the name of a database table (generally the plural
# of the model name, unless otherwise specified).
#
# has_many associations indicate that a model is a parent of many instances of
# another model. Databases can not reflect this in the parent table, and rely
# on the child tables specifying a foreign key to define the relationship.
def has_many(children, name=None, column_name=None):
  frame = sys._getframe(1)
  locals = frame.f_locals

  # Ensure we were called from a class def.
  if locals is frame.f_globals or '__module__' not in locals:
    raise TypeError("has_many() can be used only from a class definition.")

  if not name:
    name = children

  self = locals['__module__'].split('.')[-1]

  if 'associations' not in locals:
    locals['associations'] = {}
  locals['associations'][name] = HasManyAssociation(self, children, column_name)


# A wrapping class to interact with all children of this association as one
# attribute of the model.
class HasManyAssociation():
  def __init__(self, parent, child, column_name):
    # The name of the module (parameterized form of the class name) in which the
    # child class should be located.
    self.child = INFLECTOR.singular_noun(child)

    # Determine the name of the class from the given.
    self.child_class = helpers.make_class_name(self.child)

    # If no column name is provided, default to the referenced class + "_id".
    if column_name:
      self.column = column_name
    else:
      self.column = parent+'_id'

  # Should return an instance of the child model, representing the record which
  # this association references.
  def get_association(self, inst):
    module = importlib.import_module('models.'+self.child)
    klass = getattr(module, self.child_class, None)

    if not klass:
      raise NameError('Model "%s" has not been defined.' % self.child_class)

    # If no children exist, this will return a blank array rather than failing
    # or returning None.
    return klass.relation.where(**{ self.column: inst.id }).all

  # Should set the appropriate attribute of each child in children (as
  # determined by self.column), then save each child record (with validations).
  #
  # Notice that this is the reverse of a normal singular association.
  def set_association(self, inst, children):
    for child_inst in children:
      child_inst.update_attributes({ self.column: inst.id })
      child_inst.save()
