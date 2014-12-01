from __future__ import absolute_import

import logging

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.files import File
from django.core.files.storage import Storage
from django.utils.http import urlquote
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
DOWNLOAD_URL = 'https://storage-download.googleapis.com'


class CloudStorage(Storage):
    _delimiter = '/'

    def _open(self, name, mode='rb'):
        # cloudstorage lib only does 'r' or 'w'.
        mode = mode.replace('b', '')
        name = '/%s/%s' % (self._bucket, name)
        return gcs.open(name, mode=mode)

    def _save(self, name, content):
        dest = '/%s/%s' % (self._bucket, name)
        with gcs.open(dest, mode='w') as fh:
            fh.write(content.read())

        return name

    def delete(self, name):
        name = '/%s/%s' % (self._bucket, name)
        return gcs.delete(name)

    def exists(self, name):
        name = '/%s/%s' % (self._bucket, name)
        try:
            gcs.stat(name)
            return True
        except gcs.NotFoundError:
            return False

    def listdir(self, path):
        path = '/%s/%s' % (self._bucket, path)
        dirs_files = ([], [])

        for obj in gcs.listbucket(path, delimiter=self._delimiter):
            dirs_files[not obj.is_dir].append(obj.filename)

        return dirs_files

    def size(self, name):
        name = '/%s/%s' % (self._bucket, name)
        return gcs.stat(name).st_size

    def created_time(self, name):
        name = '/%s/%s' % (self._bucket, name)
        return gcs.stat(name).st_ctime

    def modified_time(self, name):
        return self.created_time(name)

    def get_valid_name(self, name):
        return name

    def get_available_name(self, name):
        return name

    def url(self, name):
        return '/'.join(DOWNLOAD_URL, self._bucket, urlquote(name))

    @property
    def _bucket(self):
        return get_bucket()


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
