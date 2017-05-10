from sqlbuilder.smartsql import Q, T, compile


compilled = compile(Q().tables(
     (T.book & T.author).on(T.book.author_id == T.author.id)
 ).columns(
     T.book.name, T.author.first_name, T.author.last_name
 ).where(
     (T.author.first_name != 'Tom') & (T.author.last_name != 'Smith')
 )[20:30])


# {
#   "meta": {
#     "code": 400,
#     "error": {
#       "type": "paramIsOutsideSet",
#       "message": "Param 'fields' is invalid. Value 'items.companies' is outside of allowed values: "
#                  "'items.address', 'items.adm_div', 'items.access_comment', 'items.access', 'items.routes.directions', "
#                  "'items.access_name', 'items.schedule', 'items.geometry.style', 'items.is_paid', 'items.external_content',"
#                  " 'items.geometry.hover', 'items.stat', 'hash', 'items.geometry.centroid', 'items.capacity', 'items.structure_info.material',"
#                  " 'items.platforms', 'items.address.is_conditional', 'items.structure_info.porch_count', 'items.is_routing_available', "
#                  "'items.level_count', 'search_attributes', 'items.group', 'items.floors', 'items.statistics', 'items.links', 'items.photos', "
#                  "'items.geometry.selection', 'items.structure_info.apartments_count', 'items.description', 'items.region_id', "
#                  "'items.context', 'items.routes', 'items.attraction'"
#     },
#     "api_version": "2.0.1.6.353",
#     "issue_date": "20170419"
#   }
# }

