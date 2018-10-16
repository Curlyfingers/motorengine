#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import six

from motor.motor_asyncio import AsyncIOMotorClient
from motorengine.asyncio.database import Database
from motorengine.errors import MotorengineConnectionError

DEFAULT_CONNECTION_NAME = 'default'

_connection_settings = {}
_connections = {}
_default_dbs = {}


def register_connection(db, alias, **kwargs):
    global _connection_settings
    global _default_dbs

    _connection_settings[alias] = kwargs
    _default_dbs[alias] = db


def cleanup():
    global _connections
    global _connection_settings
    global _default_dbs

    _connections = {}
    _connection_settings = {}
    _default_dbs = {}


def disconnect(alias=DEFAULT_CONNECTION_NAME):
    global _connections
    global _connections_settings
    global _default_dbs

    if alias in _connections:
        get_connection(alias=alias).disconnect()
        del _connections[alias]
        del _connection_settings[alias]
        del _default_dbs[alias]


def get_connection(alias=DEFAULT_CONNECTION_NAME, db=None):
    global _connections
    global _default_dbs

    if alias not in _connections:
        conn_settings = _connection_settings[alias].copy()
        db = conn_settings.pop('name', None)

        connection_class = AsyncIOMotorClient
        try:
            _connections[alias] = connection_class(**conn_settings)
        except Exception:
            exc_info = sys.exc_info()
            err = MotorengineConnectionError('Cannot connect to database {} :\n{}'.format(alias, exc_info[1]))
            raise six.reraise(MotorengineConnectionError, err, exc_info[2])

    try:
        if not _connections[alias].connected:
            _connections[alias].open_sync()
    except Exception:
        exc_info = sys.exc_info()
        err = MotorengineConnectionError('Cannot connect to database {} :\n{}'.format(alias, exc_info[1]))
        raise six.reraise(MotorengineConnectionError, err, exc_info[2])

    database = getattr(_connections[alias], db or _default_dbs[alias])
    return Database(_connections[alias], database)


def connect(db, alias=DEFAULT_CONNECTION_NAME, **kwargs):
    global _connections
    if alias not in _connections:
        kwargs['name'] = db
        register_connection(db, alias, **kwargs)

    return get_connection(alias, db=db)
