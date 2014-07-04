import sys
import importlib

import active_record.helpers as helpers

# Add a parent association to the referencing class.
#
# The parent parameter is the name of a database table (generally the plural
# of the model name, unless otherwise specified).
#
# belongs_to associations indicate that a model is a child of another model.
# To reflect this in the database, the schema should define a `references`
# column, which will make the column a foreign key to the parent table.
def belongs_to(parent, name=None, column_name=None):
  frame = sys._getframe(1)
  locals = frame.f_locals

  # Ensure we were called from a class def.
  if locals is frame.f_globals or '__module__' not in locals:
    raise TypeError("belongs_to() can be used only from a class definition.")

  if not name:
    name = parent

  if 'associations' not in locals:
    locals['associations'] = {}
  locals['associations'][name] = BelongsToAssociation(parent, column_name)


# A wrapping class to interact with the parent record of this association.
class BelongsToAssociation():
  def __init__(self, parent, column_name):
    # The name of the module (parameterized form of the class name) in which the
    # parent class should be located.
    self.parent = parent

    # Determine the name of the class from the given.
    self.parent_class = helpers.make_class_name(parent)

    # If no column name is provided, default to the referenced class + "_id".
    if column_name:
      self.column = column_name
    else:
      self.column = parent+'_id'

    # To improve efficiency, cache the value of this association after the first
    # access of it. Then, when the value changes, refresh the cache.
    self._cached = None


  # Should return an instance of the parent model, representing the record which
  # this association references.
  def get_association(self, inst):
    if self._cached:
      print "cached"
      return self._cached

    module = importlib.import_module('models.'+self.parent)
    klass = getattr(module, self.parent_class, None)

    if not klass:
      raise NameError('Model "%s" has not been defined.' % self.parent_class)

    parent_id = getattr(inst, self.column, None)

    self._cached = klass.find(parent_id)

    return self._cached

  # Should set the appropriate attribute of `inst` (as given by `self.column`)
  # to the id of the parent, then save this instance (with validations).
  def set_association(self, inst, parent_inst):
    inst.update_attributes({ self.column: parent_inst.id })

    print 'updating'
    # Update the cache by clearing it, then accessing the association.
    self._cached = None
    self.get_association(inst)

