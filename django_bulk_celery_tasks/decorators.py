from __future__ import print_function
from functools import wraps

from django.db import connection


def multiprocess_func(view_func):
    """ Closes database connection before running wrapped function """

    def _decorator(args):
        connection.close()
        # unpack args if list/tuple (which means multiple arguments were given)
        if isinstance(args, (list, tuple)):
            return view_func(*args)
        # single argument, just send it
        return view_func(args)

    return wraps(view_func)(_decorator)
