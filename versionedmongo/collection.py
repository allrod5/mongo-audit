import logging

from bson import ObjectId
from pymongo.collection import Collection as PyMongoCollection
from pymongo.database import Database  # noqa
from pymongo.errors import DuplicateKeyError
from pymongo.results import InsertOneResult  # noqa


class Collection(PyMongoCollection):
    """
    A Mongo collection linked to an audit collection
    """
    def __init__(
            self, database, main_collection_name, audit_collection_name,
            revision_field, **kwargs):
        # type: (Database, str, str, str, dict) -> None
        """
        :param database: A PyMongo Database
        :type database: Database

        :param main_collection_name: Name of the main collection
        :type main_collection_name: str

        :param audit_collection_name: Name of the audit collection
        :type audit_collection_name: str

        :param revision_field: Name of the field to be added to any
            document in the main collection to reference the current
            revision in the audit collection
        :type revision_field: str

        :param kwargs: additional arguments
        :type kwargs: dict
        """
        super(Collection, self).__init__(
            database, main_collection_name, **kwargs)
        self.main_collection = PyMongoCollection(
            database, main_collection_name, **kwargs)
        self.audit_collection = PyMongoCollection(
            database, audit_collection_name, **kwargs)
        self.audit_collection.create_index(
            'document_id', unique=False, name='document_id')
        self.revision_field = revision_field

    def insert_one(
            self, document, bypass_document_validation=False, audit=None):
        # type: (dict, bool, dict) -> InsertOneResult
        """
        The strategy of this method is to insert an audit_document into
        audit collection first and then to insert the actual document
        into the main collection.

        The audit_document carries a reference to the actual document
        _id. Inserting the actual document before the audition succeed
        jeopardises the audition integrity as a rollback can't be
        guaranteed in case of failure. A third update operation to put
        the actual document _id in the audit_document should be avoided
        to save the cost of the operation.

        For the audit_document to carry the reference to the actual
        document _id an optimistic lock strategy is used.
        The document _id is generated before any insertions.
        This saves one extra update operation on the audit collection.
        If the _id conflicts with one already existent in the database
        and the insertion of the actual document fails, then the
        audit_document inserted is removed to rollback the operation
        and to try again.

        :param document: The document to insert. Must be a mutable
            mapping type. If the document does not have an _id field
            one will be added automatically.
        :type document: dict

        :param bypass_document_validation: (optional) If ``True``,
            allows the write to opt-out of document level validation.
            Default is ``False``.

        :param audit: (optional) Additional audit data. The
            audit_document derived from the original document will be
            updated with entries in this dictionary. Default is
            ``None``.
        :type audit: dict
        """
        while True:
            _id = ObjectId()
            document['_id'] = _id
            document[self.revision_field] = _id
            audit_document = dict(document)
            audit_document['document_id'] = _id
            if audit:
                audit_document.update(audit)

            inserted_audit_id = None
            try:
                inserted_audit_id = self.audit_collection.insert_one(
                    audit_document, bypass_document_validation).inserted_id
                return self.main_collection.insert_one(
                    document, bypass_document_validation)
            except DuplicateKeyError:
                try:
                    if inserted_audit_id is not None:
                        self.audit_collection.delete_one({'_id': _id})
                except Exception:
                    logging.exception(
                        "An error occurred while cleaning failed insertion"
                        " operation from the audit collection.\n"
                        "An orphaned record may have been left in the audit"
                        " collection.")
                    raise
