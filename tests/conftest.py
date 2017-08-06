import copy
import pymongo
import pytest
from pymongo.database import Database  # noqa


class FunctionWrapper(object):
    """
    Class to wrap pymongo functions to enable monkeypatching their
    behavior.
    """
    def __init__(self, patch, config=None):
        # type: (callable, dict) -> None
        self.calls = []
        self.config = config
        self.patch = patch
        self.config = config
        if self.config is None:
            self.config = {}

    def __call__(self, *args, **kwargs):
        self.calls.append({
            'args': copy.deepcopy(args),
            'kwargs': copy.deepcopy(kwargs)
        })
        return self.patch(self, *args, **kwargs)


@pytest.fixture(scope='session')
def db():
    # type: () -> Database
    client = pymongo.MongoClient()
    test_db = client.test_db
    yield test_db
    client.drop_database('test_db')
