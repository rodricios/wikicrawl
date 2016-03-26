__author__ = 'rodrigo'

from collections import defaultdict, Counter

class SortedKeyDict(dict):
    """A dictionary that applies an arbitrary key-altering
       function before accessing the keys"""

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __getitem__(self, key):
        return super(SortedKeyDict, self).__getitem__(self.__keytransform__(key))

    def __setitem__(self, key, value):
        super(SortedKeyDict, self).__setitem__(self.__keytransform__(key), value)

    def __delitem__(self, key):
        super(SortedKeyDict, self).__delitem__(self.__keytransform__(key))

    def __keytransform__(self, key):
        try:
            return tuple(sorted(key))
        except (TypeError):
            return key

class SortedKeyCounter(Counter):
    """A dictionary that applies an arbitrary key-altering
       function before accessing the keys"""

    def __init__(self, *args, **kwargs):
        Counter.__init__(self, *args, **kwargs)

    def __getitem__(self, key):
        return super(SortedKeyCounter, self).__getitem__(self.__keytransform__(key))

    def __setitem__(self, key, value):
        super(SortedKeyCounter, self).__setitem__(self.__keytransform__(key), value)

    def __delitem__(self, key):
        super(SortedKeyCounter, self).__delitem__(self.__keytransform__(key))

    def __keytransform__(self, key):
        try:
            return tuple(sorted(key))
        except (TypeError):
            return key

class SortedKeyDefaultDict(defaultdict):
    """A dictionary that applies an arbitrary key-altering
       function before accessing the keys"""

    def __init__(self, *args, **kwargs):
        defaultdict.__init__(self, *args, **kwargs)

    def __getitem__(self, key):
        return super(SortedKeyDefaultDict, self).__getitem__(self.__keytransform__(key))

    def __setitem__(self, key, value):
        super(SortedKeyDefaultDict, self).__setitem__(self.__keytransform__(key), value)

    def __delitem__(self, key):
        super(SortedKeyDefaultDict, self).__delitem__(self.__keytransform__(key))

    def __keytransform__(self, key):
        try:
            return tuple(sorted(key))
        except (TypeError):
            return key