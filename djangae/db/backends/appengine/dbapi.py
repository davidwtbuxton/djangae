""" Fake DB API 2.0 for App engine """
from django.db import utils


DatabaseError = utils.DatabaseError
DataError = utils.DataError
IntegrityError = utils.IntegrityError
InterfaceError = utils.InterfaceError
InternalError = utils.InternalError
NotSupportedError = utils.NotSupportedError
OperationalError = utils.OperationalError
ProgrammingError = utils.ProgrammingError


class CouldBeSupportedError(NotSupportedError):
    pass


def Binary(val):
    return val


Error = DatabaseError
Warning = DatabaseError
