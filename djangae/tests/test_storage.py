# coding: utf-8
# STANDARD LIB
from unittest import skipIf
import httplib
import io
import os
import unittest
import urlparse

# THIRD PARTY
from django.core.files.base import File, ContentFile
from django.db import models
from django.test.utils import override_settings
from google.appengine.api import urlfetch
from google.appengine.api.images import TransformationError, LargeImageError

# DJANGAE
from djangae.contrib import sleuth
from djangae.db import transaction
from djangae import storage
from djangae.test import TestCase


class ModelWithImage(models.Model):
    class Meta:
        app_label = "djangae"

    image = models.ImageField()


class ModelWithTextFile(models.Model):
    class Meta:
        app_label = "djangae"

    text_file = models.FileField()


@skipIf(not storage.has_cloudstorage, "Cloud Storage not available")
class CloudStorageTests(TestCase):

    @override_settings(CLOUD_STORAGE_BUCKET='test_bucket')
    def test_basic_actions(self):
        storage_obj = storage.CloudStorage()
        name = u'tmp.ąćęłńóśźż.马铃薯.zip'

        f = ContentFile('content', name='my_file')
        filename = storage_obj.save(name, f)
        self.assertIsInstance(filename, basestring)
        self.assertTrue(filename.endswith(name))

        self.assertTrue(storage_obj.exists(filename))
        self.assertEqual(storage_obj.size(filename), len('content'))
        url = storage_obj.url(filename)
        self.assertIsInstance(url, basestring)
        self.assertNotEqual(url, '')

        abs_url = urlparse.urlunparse(
            ('http', os.environ['HTTP_HOST'], url, None, None, None)
        )
        response = urlfetch.fetch(abs_url)
        self.assertEqual(response.status_code, httplib.OK)
        self.assertEqual(response.content, 'content')

        f = storage_obj.open(filename)
        self.assertIsInstance(f, File)
        self.assertEqual(f.read(), 'content')

        # Delete it
        storage_obj.delete(filename)
        self.assertFalse(storage_obj.exists(filename))

    @override_settings(CLOUD_STORAGE_BUCKET='test_bucket')
    def test_dotslash_prefix(self):
        storage_obj = storage.CloudStorage()
        name = './my_file'
        f = ContentFile('content')
        filename = storage_obj.save(name, f)
        self.assertEqual(filename, name.lstrip("./"))

    @override_settings(CLOUD_STORAGE_BUCKET='test_bucket')
    def test_supports_nameless_files(self):
        storage_obj = storage.CloudStorage()
        f2 = ContentFile('nameless-content')
        storage_obj.save('tmp2', f2)

    @override_settings(CLOUD_STORAGE_BUCKET='test_bucket')
    def test_new_objects_get_the_default_acl(self):
        storage_obj = storage.CloudStorage()
        filename = 'example.txt'
        fileobj = ContentFile('content', name=filename)

        with sleuth.watch('cloudstorage.open') as open_func:
            storage_obj.save(filename, fileobj)

        self.assertTrue(storage_obj.exists(filename))
        # There's no x-goog-acl argument, so default perms are applied.
        self.assertEqual(open_func.calls[0].kwargs['options'], {})

    @override_settings(CLOUD_STORAGE_BUCKET='test_bucket')
    def test_new_objects_with_an_explicit_acl(self):
        storage_obj = storage.CloudStorage(google_acl='public-read')
        filename = 'example.txt'
        fileobj = ContentFile('content', name=filename)

        with sleuth.watch('cloudstorage.open') as open_func:
            storage_obj.save(filename, fileobj)

        self.assertTrue(storage_obj.exists(filename))
        self.assertEqual(
            open_func.calls[0].kwargs['options'],
            {'x-goog-acl': 'public-read'},
        )

    @override_settings(
        CLOUD_STORAGE_BUCKET='test_bucket',
        DEFAULT_FILE_STORAGE='djangae.storage.CloudStorage'
    )
    def test_access_url_inside_transaction(self):
        """ Regression test.  Make sure that accessing the `url` of an ImageField can be done
            inside a transaction without causing the error:
            "BadRequestError: cross-groups transaction need to be explicitly specified (xg=True)"
        """
        instance = ModelWithImage(
            image=ContentFile('content', name='my_file')
        )
        instance.save()
        with sleuth.watch('djangae.storage.get_serving_url') as get_serving_url_watcher:
            with transaction.atomic():
                instance.refresh_from_db()
                instance.image.url  # Access the `url` attribute to cause death
                instance.save()
            self.assertTrue(get_serving_url_watcher.called)

    @override_settings(
        CLOUD_STORAGE_BUCKET='test_bucket',
        DEFAULT_FILE_STORAGE='djangae.storage.CloudStorage'
    )
    def test_get_non_image_url(self):
        """ Regression test. Make sure that if the file is not an image
            we still get a file's urls without throwing a
            TransformationError.
        """
        instance = ModelWithTextFile(
            text_file=ContentFile('content', name='my_file')
        )
        instance.save()
        with sleuth.watch('urllib.quote') as urllib_quote_watcher:
            with sleuth.detonate('djangae.storage.get_serving_url', TransformationError):
                instance.refresh_from_db()
                instance.text_file.url
                instance.save()
                self.assertTrue(urllib_quote_watcher.called)


class BlobstoreStorageTests(TestCase):
    def test_basic_actions(self):

        storage_obj = storage.BlobstoreStorage()

        # Save a new file
        f = ContentFile('content', name='my_file')
        filename = storage_obj.save('tmp', f)

        self.assertIsInstance(filename, basestring)
        self.assertTrue(filename.endswith('tmp'))

        # Check .exists(), .size() and .url()
        self.assertTrue(storage_obj.exists(filename))
        self.assertEqual(storage_obj.size(filename), len('content'))
        url = storage_obj.url(filename)
        self.assertIsInstance(url, basestring)
        self.assertNotEqual(url, '')

        # Check URL can be fetched
        abs_url = urlparse.urlunparse(
            ('http', os.environ['HTTP_HOST'], url, None, None, None)
        )
        response = urlfetch.fetch(abs_url)
        self.assertEqual(response.status_code, httplib.OK)
        self.assertEqual(response.content, 'content')

        # Open it, read it
        # NOTE: Blobstore doesn’t support updating existing files.
        f = storage_obj.open(filename)
        self.assertIsInstance(f, File)
        self.assertEqual(f.read(), 'content')

        # Delete it
        storage_obj.delete(filename)
        self.assertFalse(storage_obj.exists(filename))

    def test_supports_nameless_files(self):
        storage_obj = storage.BlobstoreStorage()
        f2 = ContentFile('nameless-content')
        storage_obj.save('tmp2', f2)

    def test_transformation_error(self):
        storage_obj = storage.BlobstoreStorage()
        with sleuth.detonate('djangae.storage.get_serving_url', TransformationError):
            self.assertEqual('thing', storage_obj.url('thing'))

    def test_large_image_error(self):
        storage_obj = storage.BlobstoreStorage()
        with sleuth.detonate('djangae.storage.get_serving_url', LargeImageError):
            self.assertEqual('thing', storage_obj.url('thing'))


class ParseGsObjectNameTestCase(unittest.TestCase):
    upload_body_prod = (
        # This is the file data we get from an upload that was handled by
        # App Engine's create_upload_url thing (where you upload directly
        # into a cloud storage bucket).
        'Content-Type: application/zip\r\n'
        'Content-Length: 792196\r\n'
        'X-AppEngine-Upload-Creation: 2017-03-29 15:04:56.401187\r\n'
        'X-AppEngine-Cloud-Storage-Object: /gs/example.appspot.com/uploads/L2FwcGh=\r\n'
        'vc3RpbmdfcHJvZC9ibG9icy9BRW5CMlVyd3VwQXc2c0ctMTVFb0FnY2tVNGN4cHVWRW9VU001cX=\r\n'
        'JZNDBFdUVOR0NwN0tieWdsalBtRjNhQnNldktnUzRrSS1iQnZZR3dGYmd3WU5vUnRnNUxWOEZLT=\r\n'
        'ThZSGJlTTg3NXFUQVAxcFZjRXFuZmhJZy5OS0RTZXBVa2RWNUU2Sl9t\r\n'
        'Content-MD5: YjQzMDc4ZGRjYjQ0NWE3MDJkNTVlZmZjMmNmYWM4ZDA=3D\r\n'
        'Content-Disposition: form-data; name=3Dzip_file; filename=3D"example-upload=\r\n'
        '.zip"\r\n'
        '\r\n'
        ''
    )

    upload_body_dev = (
        # This is the file data from an App Engine create_upload_url callback
        # when running with the dev server. Unlike production, the content does
        # not use quoted-printable encoding.
        'Content-Type: application/zip\r\n'
        'Content-Length: 26427691\r\n'
        'Content-MD5: NzU3ZWVkZTQ3ODI1YjQwMjNjM2Q5YmExZDg5NTUyODY=\r\n'
        'X-AppEngine-Cloud-Storage-Object: /gs/app_default_bucket/uploads/fake-fatW_uIRa_DlR07gOblNyg==\r\n'
        'content-disposition: form-data; name="zip_file"; filename="example.zip"\r\n'
        'X-AppEngine-Upload-Creation: 2017-04-18 17:56:20.460998\r\n'
        '\r\n'
    )

    def test_parse_multiline_cloud_storage_header(self):
        data = io.BytesIO(self.upload_body_prod)
        result = storage.parse_gs_object_name(data, transfer_encoding='quoted-printable')
        expected = (
            '/example.appspot.com/uploads/L2FwcGh'
            'vc3RpbmdfcHJvZC9ibG9icy9BRW5CMlVyd3VwQXc2c0ctMTVFb0FnY2tVNGN4cHVWRW9VU001cX'
            'JZNDBFdUVOR0NwN0tieWdsalBtRjNhQnNldktnUzRrSS1iQnZZR3dGYmd3WU5vUnRnNUxWOEZLT'
            'ThZSGJlTTg3NXFUQVAxcFZjRXFuZmhJZy5OS0RTZXBVa2RWNUU2Sl9t'
        )

        self.assertEqual(result, expected)

    def test_parse_upload_with_missing_header(self):
        data = io.BytesIO('Content-Type: application/zip\r\n')
        result = storage.parse_gs_object_name(data)

        self.assertIsNone(result)

    def test_parse_local_dev_upload(self):
        data = io.BytesIO(self.upload_body_dev)
        result = storage.parse_gs_object_name(data)
        expected = '/app_default_bucket/uploads/fake-fatW_uIRa_DlR07gOblNyg=='

        self.assertEqual(result, expected)
