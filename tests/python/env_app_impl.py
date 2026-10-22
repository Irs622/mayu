"""The implementation of a task declared in env_app; only the env's child imports this module."""
import os

from env_app import declared


@declared.implement
def _declared(x):
    return [x * 2, os.getpid()]
