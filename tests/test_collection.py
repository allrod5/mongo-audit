import pytest
from bson import ObjectId
from pymongo.database import Database  # noqa
from pymongo.errors import DuplicateKeyError

from tests import copy_func
from tests.conftest import FunctionWrapper


class LoggingExceptionStub:
    def __init__(self):
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1

import logging  # noqa
logging.exception = LoggingExceptionStub()
from versionedmongo.collection import Collection  # noqa


def get_test_data():
    return {
        'name': "Hermione Jean Granger",
        'born': {
            'date': "19 September, 1979",
            'country': "England, Great Britan",
        },
    }


def get_test_audit_data():
    return {
        'audit_info': {
            'revision_authored_by': ObjectId('54f113fffba522406c9cc20e'),
            'revision_origin': 'APIv2'
        }
    }


def insert_one_patch(wrapper, *args, **kwargs):
    if wrapper.config.get('fail_on_first_call') and len(wrapper.calls) == 1:
        raise DuplicateKeyError(Exception)
    else:
        return wrapper.insert_one(wrapper.instance, *args, **kwargs)


def delete_one_patch(wrapper, *args, **kwargs):
    if wrapper.config.get('fail'):
        raise Exception
    else:
        return wrapper.delete_one(wrapper.instance, *args, **kwargs)


@pytest.fixture(scope='function')
def patched_collection(db):
    # type: (Database) -> Collection
    collection = Collection(db, 'test', 'test_aud', revision_field='revision')

    main_collection_insert_one_patch = FunctionWrapper(insert_one_patch)
    main_collection_insert_one_patch.insert_one = copy_func(
        collection.main_collection.insert_one)
    main_collection_insert_one_patch.instance = collection.main_collection
    collection.main_collection.insert_one = main_collection_insert_one_patch

    audit_collection_insert_one_patch = FunctionWrapper(insert_one_patch)
    audit_collection_insert_one_patch.insert_one = copy_func(
        collection.audit_collection.insert_one)
    audit_collection_insert_one_patch.instance = collection.audit_collection
    collection.audit_collection.insert_one = audit_collection_insert_one_patch

    main_collection_delete_one_patch = FunctionWrapper(delete_one_patch)
    main_collection_delete_one_patch.delete_one = copy_func(
        collection.main_collection.delete_one)
    main_collection_delete_one_patch.instance = collection.main_collection
    collection.main_collection.delete_one = main_collection_delete_one_patch

    audit_collection_delete_one_patch = FunctionWrapper(delete_one_patch)
    audit_collection_delete_one_patch.delete_one = copy_func(
        collection.audit_collection.delete_one)
    audit_collection_delete_one_patch.instance = collection.audit_collection
    collection.audit_collection.delete_one = audit_collection_delete_one_patch

    return collection


def test_insert_one(patched_collection):
    # type: (Collection) -> None
    """
    This test ensures that :func:`Collection.insert_one` will perform
    an insert to the collection and add a record to audit collection
    as well, including audit data when required to.

    :param patched_collection: Collection instance with patched
        external dependencies
    :type patched_collection: Collection
    """
    # setup ####
    test_data = get_test_data()
    test_audit_data = get_test_audit_data()
    test_audit_data['audit_info'].update({'operation': 'insert_one'})
    revision_field = patched_collection.revision_field
    ############

    # when :func:`Collection.insert_one` is called
    inserted_id = patched_collection.insert_one(
        test_data, audit=test_audit_data).inserted_id

    # the insertion shall succeed and return the inserted_id
    assert inserted_id is not None

    # the audit_collection shall contain a record of the insertion and
    # the audit data supplied
    audit_document = patched_collection.audit_collection.find_one(
        {'document_id': inserted_id}, {'document_id': False})
    assert test_data[revision_field] == audit_document['_id']
    assert dict(audit_document, **test_audit_data) == audit_document

    # except for the `_id` and additional audit_data when supplied, the
    # audit_document shall be an exact copy of the inserted data
    del test_data['_id']
    del audit_document['_id']
    del audit_document['audit_info']
    assert audit_document == test_data


def test_insert_one_optimistic_lock_failure(patched_collection):
    # type: (Collection) -> None
    """
    This test ensures that :func:`Collection.insert_one` will retry
    insertion when optimistic lock strategy fails.

    :param patched_collection: Collection instance with patched
        external dependencies
    :type patched_collection: Collection
    """
    # setup ####
    test_data = get_test_data()
    test_audit_data = get_test_audit_data()
    test_audit_data['audit_info'].update({'operation': 'insert_one'})
    revision_field = patched_collection.revision_field
    main_collection_insert_one = patched_collection.main_collection.insert_one
    main_collection_delete_one = patched_collection.main_collection.delete_one
    audit_collection_insert_one = (
        patched_collection.audit_collection.insert_one)
    audit_collection_delete_one = (
        patched_collection.audit_collection.delete_one)
    audit_collection_insert_one.config['fail_on_first_call'] = True
    ############

    # when :func:`Collection.insert_one` is called and optimistic lock
    # failures occur at first
    inserted_id = patched_collection.insert_one(
        test_data, audit=test_audit_data).inserted_id

    # the insertion shall succeed and return the inserted_id
    assert inserted_id is not None

    # insertion on the main_collection shall be performed once as
    # the first insertion on the audit_collection will fail and
    # insertion on the audit_collection shall be performed twice due to
    # failure on the first attempt
    assert len(main_collection_insert_one.calls) == 1
    assert len(audit_collection_insert_one.calls) == 2

    # each insertion attempt shall use a unique `_id`
    first_id = audit_collection_insert_one.calls[0]['args'][0]['_id']
    second_id = audit_collection_insert_one.calls[1]['args'][0]['_id']
    assert first_id != second_id

    # deletion on the audit_collection shall not be performed, neither
    # on the main_collection
    assert len(audit_collection_delete_one.calls) == 0
    assert len(main_collection_delete_one.calls) == 0

    # the audit_collection shall contain a record of the insertion and
    # the audit data supplied
    audit_document = patched_collection.audit_collection.find_one(
        {'document_id': inserted_id}, {'document_id': False})
    assert test_data[revision_field] == audit_document['_id']
    assert dict(audit_document, **test_audit_data) == audit_document

    # except for the `_id` and additional audit_data when supplied, the
    # audit_document shall be an exact copy of the inserted data
    del test_data['_id']
    del audit_document['_id']
    del audit_document['audit_info']
    assert audit_document == test_data


def test_insert_one_rollback(patched_collection):
    # type: (Collection) -> None
    """
    This test ensures that :func:`Collection.insert_one` will perform
    a rollback when an insertion fails due to optimistic lock strategy
    failure and retry the insertion.

    :param patched_collection: Collection instance with patched
        external dependencies
    :type patched_collection: Collection
    """
    # setup ####
    test_data = get_test_data()
    test_audit_data = get_test_audit_data()
    test_audit_data['audit_info'].update({'operation': 'insert_one'})
    revision_field = patched_collection.revision_field
    main_collection_insert_one = patched_collection.main_collection.insert_one
    audit_collection_insert_one = (
        patched_collection.audit_collection.insert_one)
    audit_collection_delete_one = (
        patched_collection.audit_collection.delete_one)
    main_collection_insert_one.config['fail_on_first_call'] = True
    ############

    # when :func:`Collection.insert_one` is called and optimistic lock
    # failures occur for the main_collection only at first
    inserted_id = patched_collection.insert_one(
        test_data, audit=test_audit_data).inserted_id

    # the insertion shall succeed and return the inserted_id
    assert inserted_id is not None

    # insertion on the main_collection and audit_collection shall be
    # performed twice due to one rollback performed
    assert len(main_collection_insert_one.calls) == 2
    assert len(audit_collection_insert_one.calls) == 2

    # each insertion attempt shall use a unique `_id`
    first_id = main_collection_insert_one.calls[0]['args'][0]['_id']
    second_id = main_collection_insert_one.calls[1]['args'][0]['_id']
    assert first_id != second_id

    # deletion on the audit_collection shall be performed
    assert len(audit_collection_delete_one.calls) == 1

    # the document with the `_id` which failed insertion but made it
    # to the audit_collection shall be the one deleted
    deleted_id = audit_collection_delete_one.calls[0]['args'][0]['_id']
    assert deleted_id == first_id

    # the audit_collection shall contain a record of the insertion and
    # the audit data supplied
    audit_document = patched_collection.audit_collection.find_one(
        {'document_id': inserted_id}, {'document_id': False})
    assert test_data[revision_field] == audit_document['_id']
    assert dict(audit_document, **test_audit_data) == audit_document

    # except for the `_id` and additional audit_data when supplied, the
    # audit_document shall be an exact copy of the inserted data
    del test_data['_id']
    del audit_document['_id']
    del audit_document['audit_info']
    assert audit_document == test_data


def test_insert_one_rollback_failure(patched_collection):
    # type: (Collection) -> None
    """
    This test ensures that :func:`Collection.insert_one` will raise an
    exception when it fails to rollback a failed insertion attempt and
    log the error.

    :param patched_collection: Collection instance with patched
        external dependencies
    :type patched_collection: Collection
    """
    # setup ####
    test_data = get_test_data()
    test_audit_data = get_test_audit_data()
    test_audit_data['audit_info'].update({'operation': 'insert_one'})
    main_collection_insert_one = patched_collection.main_collection.insert_one
    audit_collection_delete_one = (
        patched_collection.audit_collection.delete_one)
    main_collection_insert_one.config['fail_on_first_call'] = True
    audit_collection_delete_one.config['fail'] = True
    logging.exception.calls = 0
    ############

    # an exception shall be raised
    with pytest.raises(Exception):
        # when :func:`Collection.insert_one` is called and optimistic lock
        # fails to insert on main_collection but not on audit_collection
        # and an exception occurs while cleaning the audit_collection
        patched_collection.insert_one(test_data, audit=test_audit_data)

    # deletion on the audit_collection shall be attempted
    assert len(audit_collection_delete_one.calls) == 1

    # an exception shall be logged
    assert logging.exception.calls == 1
