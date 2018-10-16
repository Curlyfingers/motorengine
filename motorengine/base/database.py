#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import ABCMeta
from abc import abstractmethod
from six import with_metaclass


class BaseDatabase(with_metaclass(ABCMeta)):
    def __init__(self, connection, database):
        self.connection = connection
        self.database = database

    @abstractmethod
    def ping(self, *args, **kwargs):
        pass

    def disconnect(self):
        return self.connection.close()

    def __getattribute__(self, name):
        if name in ['ping', 'connection', 'database', 'disconnect']:
            return object.__getattribute__(self, name)

        return getattr(self.database, name)

    def __getitem__(self, val):
        return getattr(self.database, val)
