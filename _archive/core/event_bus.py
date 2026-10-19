"""
====================================================

QuantEngine

Build : 2.5
Module: Event Bus

Purpose:
Simple publish/subscribe event system.

This allows different parts of the engine to
communicate without depending on each other.

====================================================
"""


class EventBus:

    def __init__(self):

        self._subscribers = {}

    def subscribe(self, event_name: str, callback):
        """
        Register a callback for an event.
        """

        if event_name not in self._subscribers:
            self._subscribers[event_name] = []

        self._subscribers[event_name].append(callback)

    def unsubscribe(self, event_name: str, callback):
        """
        Remove a callback from an event.
        """

        if event_name not in self._subscribers:
            return

        if callback in self._subscribers[event_name]:
            self._subscribers[event_name].remove(callback)

    def publish(self, event_name: str, *args, **kwargs):
        """
        Notify all subscribers.
        """

        callbacks = self._subscribers.get(event_name, [])

        for callback in callbacks:
            callback(*args, **kwargs)

    def clear(self):
        """
        Remove all subscriptions.
        """

        self._subscribers.clear()

    def subscriber_count(self, event_name: str):

        return len(self._subscribers.get(event_name, []))


# Global Event Bus
event_bus = EventBus()