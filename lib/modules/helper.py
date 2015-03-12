import sys
from cStringIO import StringIO
import cPickle as pickle

class SafeUnpickler(object):
    PICKLE_SAFE = {
        'copy_reg' : set(['_reconstructor']),
        '__builtin__' : set(['object']),
    }

    @classmethod
    def find_class(cls, module, name):
        if not module in cls.PICKLE_SAFE:
            raise pickle.UnpicklingError('Attempting to unpickle unsafe module %s' % module)
        __import__(module)
        mod = sys.modules[module]
        if not name in cls.PICKLE_SAFE[module]:
            raise pickle.UnpicklingError('Attempting to unpickle unsafe class %s' % name)
        return getattr(mod, name)

    @classmethod
    def transform(cls, pickle_string):
        pickle_obj = pickle.Unpickler(StringIO(pickle_string))
        pickle_obj.find_global = cls.find_class
        return pickle_obj.load()
