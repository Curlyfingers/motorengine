#!/usr/bin/env python
# -*- coding: utf-8 -*-


class BaseField(object):
    total_creation_counter = 0

    def __init__(self, db_field=None, required=False):
        global creation_counter
        self.creation_counter = BaseField.total_creation_counter
        BaseField.total_creation_counter += 1

        self.db_field = db_field
        self.required = required

    def to_son(self, value):
        return value

    def from_son(self, value):
        return value