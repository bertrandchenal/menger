from collections import defaultdict

EVENTS = defaultdict(list)


# Register a callback when an event is raised (currently available:
# 'clean_cache')
def register(event_name, callback):
    if callback not in EVENTS[event_name]:
        EVENTS[event_name].append(callback)

# Trigger all the callbacks links to an event
def trigger(event_name):
    for callback in EVENTS[event_name]:
        callback()

