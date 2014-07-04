from active_record.setup import *
from active_record.result import Result

# Relation Methods
#
# These methods deal with converting Result objects into model instances and
# vice versa.

# Create a new instance with the provided attributes.
#
#   jon = Person.new(name='Jon', age=18)
#   jon.name   #=> Jon
#   jon.age    #=> 18
@classmethod
def new(cls, **attrs):
  return cls(Result.from_attrs(attrs))

# Creates a new instance and saves it immediately.
@classmethod
def create(cls, **attrs):
  return cls.new(**attrs).save()

# Finds the record with the given id, updates it with the provided attributes,
# saves the record, and returns a new instance from that record.
@classmethod
def update(cls, row_id, **attrs):
  return cls.find(row_id).update_attributes(attrs).save()

# Unconditionally attempts to delete from the database all records matching the
# provided IDs, then returns the data for each in a similarly aligned list.
@classmethod
def destroy(cls, ids):
  arel_table = cls.arel_table.where(id=ids)
  records = DB_ADAPTER.find(arel_table)
  DB_ADAPTER.delete(arel_table)

  if len(records) == 1:
    return cls(records[0])
  return [cls(record) for record in records]


# Set the attributes dictionary of this instance (it's record) equal to the
# dictionary of attributes that are passed in, then save this instance through
# the normal procedure.
def update_attributes(self, attributes):
  for name, value in attributes.iteritems():
    setattr(self, name, value)

  return self.save(True)

# Similar to the above, but only updating one attribute. Note that this method
# skips the validation procedure, and should therefore only be used with trusted
# information.
#
# This method can be useful when you purposely want to have invalid information
# in a record for things like testing or special records. Because validations
# are only done on save, active record will have no trouble reading these values
# from the database, but may fail on save if validations are left on.
def update_attribute(self, name, value):
  setattr(self, name, value)
  return self.save(False)


# Validate that this instance passes all of the provided validations.
#
# The validations argument must be an iterable list of callables (a list of
# lambdas is the most common) which take a model instance as an argument.
#
# If no validations are provided (the default), then this method will call
# each of the classes validations, created with the `validates()` helper.
#
# If this class has no validations, and no validations are passed in,
# then this method will return True.
def validate(self, validations=None):
  # Use the model's validations if none are passed in.
  if not validations:
    if hasattr(self, 'validations'):
      validations = self.validations
    else:
      return True

  for validation in validations:
    if not validation.validate(self):
      return False

  return True


# Save this instance to the database.
#
# If validate is set to True, this instance will only be saved if it passes
# all of the model's validations. If this instance fails validation, this
# method will instantly return False with no other action taken. Otherwise,
# a save will be attempted on the database.
#
# If fail_hard is set to True, this method will raise an Exception if one or
# more of the validations did not pass.
#
# Save errors are to be dealt with by the database adapter and are therefore
# not referenced here.
def save(self, validate=True, fail_hard=False):
  if validate and not self.validate():
    if fail_hard:
      raise Exception('One or more validations did not pass')
    else:
      return False

  attrs = self.record.values

  if self.exists:
    arel_table = self.arel_table.set(**attrs).where(**{ 'id': self.id })
    DB_ADAPTER.update(arel_table)
  else:
    arel_table = self.arel_table.columns(*attrs.keys()).values(*attrs.values())
    DB_ADAPTER.insert(arel_table)
    self.id = DB_ADAPTER.last_inserted()

  self.exists = True
  return self

# Reload this instance with the latest information from the database.
#
# Useful if a model instance becomes dirty, needs resetting, or has had external
# changes applied to it.
def reload(self):
  self = self.save().find(self.id)

  return self
