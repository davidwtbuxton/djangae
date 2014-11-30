from __future__ import absolute_import

import logging

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.files import File
from django.core.files.storage import Storage
from google.appengine.api import app_identity


log = logging.getLogger(__name__)
cloudstorage_pypi = 'GoogleAppEngineCloudStorageClient'

try:
    import cloudstorage as gcs
except ImportError:
    log.critical('%s requires %s', __file__, cloudstorage_pypi)
    raise


BUCKET_SETTINGS_KEY = 'CLOUDSTORAGE_BUCKET'
DIR_SEP_KEY = 'CLOUDSTORAGE_DIRECTORY_SEPARATOR'


class CloudStorage(Storage):
    _delimiter = '/'

    def _open(self, name, mode='rb'):
        # cloudstorage lib only does 'r' or 'w'.
        mode = mode.replace('b', '')

        return gcs.open(name, mode=mode)

    def _save(self, name, content):
        with gcs.open(name, mode='w') as fh:
            fh.write(content)

        return name

    def delete(self, name):
        return gcs.delete(name)

    def exists(self, name):
        return bool(gcs.stat(name))

    def listdir(self, path):
        dirs_files = ([], [])

        for obj in gcs.listbucket(path, delimiter=self._delimiter):
            dirs_files[not obj.is_dir].append(obj.filename)

        return dirs_files

    def size(self, name):
        return gcs.stat(name).st_size

    def created_time(self, name):
        return gcs.stat(name).st_ctime

    def modified_time(self, name):
        return self.created_time(name)

    def get_valid_name(self, name):
        return name

    def get_available_name(self, name):
        return name


def get_bucket():
    """Returns the bucket named in settings or the default bucket name.

    Raises ImproperlyConfigured if no bucket specified or if there's no default
    bucket enabled for the application.
    """
    try:
        bucket = getattr(settings, BUCKET_SETTINGS_KEY)
    except AttributeError:
        bucket = app_identity.get_default_gcs_bucket_name()

    if not bucket:
        name = __name__ + '.' + CloudStorage.__name__
        msg = 'Put a bucket name in your %r setting to use %r' % (BUCKET_SETTINGS_KEY, name)
        raise ImproperlyConfigured(msg)

    return bucket
