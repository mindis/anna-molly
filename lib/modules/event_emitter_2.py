import re
from boltons.dictutils import OrderedMultiDict


class EventEmitter2(object):

    def __init__(self):
        """
        """
        self.events = OrderedMultiDict()
        self.on = self.add_listener
        self.off = self.remove_listener
        # TODO: Add listener to remove

    def add_listener(self, event, listener, count=0):
        _event = re.compile(event)
        _listener = {"handler": listener, "calls": 0, "calls_left": count}
        self.events.add(_event, _listener)
        return True

    def emit(self, event, kwargs):
        for pattern, listener in self.events.iteritems(multi=True):
            if pattern.match(event):
                if not listener["calls_left"]:
                    self.remove_listener(pattern, listener)
                listener["calls"] += 1
                listener["calls_left"] -= 1
                yield listener["handler"](**kwargs)

    def remove_listener(self, pattern, listener):
        listeners = self.events.getlist(pattern)
        listeners.remove(listener)
        if len(listeners):
            self.events.update({pattern: listeners})
        else:
            self.events._remove(pattern)
        return

    def on_any(self, listener):
        raise NotImplementedError

    def off_any(self, listener):
        raise NotImplementedError

    def once(self, event, listener):
        self.add_listener(event, listener, left=1)

    def many(self, event, listener, left):
        self.add_listener(event, listener, left)
