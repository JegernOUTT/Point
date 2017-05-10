from utility.tools import with_representation, with_eq_ne_hash, with_gt_lt


@with_representation
@with_eq_ne_hash(field_name='id')
@with_gt_lt(field_name='id')
class Region(object):
    def __init__(self):
        self.id = 0
        self.name = ''
        self.bounds = []
        self.org_count = 0
        self.branch_count = 0
        self.rubric_count = 0


@with_representation
@with_eq_ne_hash(field_name='building_id')
@with_gt_lt(field_name='building_id')
class Building(object):
    def __init__(self):
        self.building_id = 0
        self.address_synonyms = []

        self.street_name = ''
        self.street_id = ''
        self.house = ''
        self.floors = ''

        self.longitude = 0.
        self.latitude = 0.


@with_representation
@with_eq_ne_hash(field_name='id')
@with_gt_lt(field_name='id')
class MetroStation(object):
    def __init__(self):
        self.id = 0
        self.name = ''
        self.longitude = 0.
        self.latitude = 0.


@with_representation
@with_eq_ne_hash(field_name='id')
@with_gt_lt(field_name='id')
class MainRubric(object):
    def __init__(self):
        self.id = 0
        self.name = ''
        self.alias = ''
        self.org_count = 0
        self.branch_count = 0


@with_representation
@with_eq_ne_hash(field_name='id')
@with_gt_lt(field_name='id')
class SubRubric(object):
    def __init__(self):
        self.id = 0
        self.main_rubric_id = ''
        self.name = ''
        self.alias = ''
        self.org_count = 0
        self.branch_count = 0


@with_representation
@with_eq_ne_hash(field_name='id')
@with_gt_lt(field_name='id')
class Organization(object):
    def __init__(self):
        self.id = 0
        self.name = ''
        self.name_primary = ''
        self.name_extension = ''
        self.name_synonyms = []
        self.main_rubrics = {'doublegis_rubrics_ids': [],
                             'google_rubrics_names': [],
                             'yandex_rubrics_names': [],
                             'other_rubrics_names': []}
        self.sub_rubrics = {'doublegis_rubrics_ids': [],
                            'google_rubrics_names': [],
                            'yandex_rubrics_names': [],
                            'other_rubrics_names': []}
        self.contacts_json = {'email': [],
                              'other': [],
                              'phone': []}


@with_representation
@with_eq_ne_hash(field_name='doublegis_id')
@with_gt_lt(field_name='doublegis_id')
class Filial(object):
    def __init__(self):
        self.doublegis_id = 0
        self.organization_id = 0

        self.building_id = 0

        self.street_name = ''
        self.house = ''
        self.address_synonyms = []

        self.longitude = 0.
        self.latitude = 0.
        self.created_at_json = {'real_created_at': '',
                                'google_appear_at': '',
                                'yandex_appear_at': '',
                                '2gis_appear_at': '',
                                'other_appear_at': ''}
        self.updated_at_json = {'google_updated_at': '',
                                'yandex_updated_at': '',
                                '2gis_updated_at': '',
                                'other_updated_at': ''}
        self.closed_at_json = {'real_closed_at': '',
                               'google_removed_at': '',
                               'yandex_removed_at': '',
                               '2gis_removed_at': '',
                               'other_removed_at': ''}

        self.metro_distances_json = [{}]

        self.google_place_id = ''
        self.yandex_id = ''

        self.extra_json = {}
