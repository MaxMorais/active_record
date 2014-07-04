import copy

from active_record.arel import Table
import active_record.helpers as helpers
import active_record.arel as arel

# Relation
#
# Relations are essentially all of an active record model, but with no reference
# to attributes or other related methods.

class Relation(object):
  from relation_methods import new, create, update, destroy, update_attribute, update_attributes, validate, save, reload
  from finder_methods import find, find_by, find_or_new, find_or_create, all, first, last
  from query_methods import select, includes, where, order, group, having, join, limit, offset, reverse

  # Create a new Relation instance which copies the given arel table into this
  # instance. This avoids having to reset the query chain with each use.
  #
  # The model parameter is the type of model that should be returned when the
  # relation is executed (.all(), .first(), etc.)
  def __init__(self, table_name, arel_table, model):
    self.table_name = table_name
    self.arel_table = copy.copy(arel_table)
    self.columns    = helpers.get_column_names(table_name)
    self.model      = model
