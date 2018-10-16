#!/usr/bin/env python
# -*- coding: utf-8 -*-

from motorengine.base.database import BaseDatabase


class Database(BaseDatabase):
    def ping(self, callback):
        self.connection.admin.command('ping', callback=callback)
