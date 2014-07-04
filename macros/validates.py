import sys

# Add a validation to the referencing class.
#
# Validations are called automatically when a record is being saved to the
# database. They can also be called manually (using .validate()), or turned off
# by setting the `validate` flag in .save() to False.
def validates(function):
  frame = sys._getframe(1)
  locals = frame.f_locals

  # Ensure we were called from a class def.
  if locals is frame.f_globals or '__module__' not in locals:
    raise TypeError("validates() can be used only from a class definition.")

  if 'validations' not in locals:
    locals['validations'] = []
  locals['validations'].append(Validation(function))


# A wrapping class to store a validation function.
class Validation():
  def __init__(self, func):
    self.func = func

  # Should return True if the object passed in, `inst`, passes this validation.
  # Could be different if the provided validation is not defined correctly.
  def validate(self, inst):
    return self.func(inst)
