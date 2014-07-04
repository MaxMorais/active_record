import sys
import importlib

import active_record.helpers as helpers

# Add a singular child association to the referencing class.
#
# The child parameter is the name of a database table (generally the plural
# of the model name, unless otherwise specified).
#
# has_one associations indicate that a model is a parent of one instance of
# another model. To reflect this in the database, the schema should define a
# `references` column, which will make the column a foreign key to the child
# table.
def has_one(child, name=None, column_name=None):
  frame = sys._getframe(1)
  locals = frame.f_locals

  # Ensure we were called from a class def.
  if locals is frame.f_globals or '__module__' not in locals:
    raise TypeError("has_one() can be used only from a class definition.")

  if not name:
    name = child

  if 'associations' not in locals:
    locals['associations'] = {}
  locals['associations'][name] = HasOneAssociation(child, column_name)


# A wrapping class to interact with the child record of this association.
class HasOneAssociation():
  def __init__(self, child, column_name):
    # The name of the module (parameterized form of the class name) in which the
    # child class should be located.
    self.child = child

    # Determine the name of the class from the given.
    self.child_class = helpers.make_class_name(child)

    # If no column name is provided, default to the referenced class + "_id".
    if column_name:
      self.column = column_name
    else:
      self.column = child+'_id'

    # To improve efficiency, cache the value of this association after the first
    # access of it. Then, when the value changes, refresh the cache.
    self._cached = None


  # Should return an instance of the child model, representing the record which
  # this association references.
  def get_association(self, inst):
    if self._cached:
      return self._cached

    module = importlib.import_module('models.'+self.child)
    klass = getattr(module, self.child_class, None)

    if not klass:
      raise NameError('Model "%s" has not been defined.' % self.child_class)

    child_id = getattr(inst, self.column, None)

    self._cached = klass.find(child_id)

    return self._cached

  # Should set the appropriate attribute of `inst` (as given by `self.column`)
  # to the id of the child, then save this instance (with validations).
  def set_association(self, inst, child_inst):
    inst.update_attributes({ self.column: child_inst.id })

    # Update the cache by clearing it, then accessing the association.
    self._cached = None
    self.get_association(inst)
