import hashlib


def with_representation(cls):
    def __repr__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items()))
    cls.__repr__ = __repr__
    return cls


def with_eq_ne_hash(field_name):
    def with_eq_ne_hash_(cls):
        def __eq__(self, other):
            return getattr(self, field_name) == getattr(other, field_name)

        def __ne__(self, other):
            return getattr(self, field_name) != getattr(other, field_name)

        def __hash__(self):
            return hash(getattr(self, field_name))

        cls.__eq__ = __eq__
        cls.__ne__ = __ne__
        cls.__hash__ = __hash__

        return cls
    return with_eq_ne_hash_


def with_gt_lt(field_name):
    def with_gt_lt_(cls):
        def __gt__(self, other):
            return getattr(self, field_name) > getattr(other, field_name)

        def __lt__(self, other):
            return getattr(self, field_name) < getattr(other, field_name)

        cls.__gt__ = __gt__
        cls.__lt__ = __lt__

        return cls
    return with_gt_lt_
