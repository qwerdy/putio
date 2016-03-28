"""
putio.py is an api wrapper for put.io


Example usage:

>>> client = putio.Client(OAUTH_TOKEN)

test if token is valid:
>>> client.check_token()

list files:
>>> client.File.list()

add a new transfer:
>>> new = client.Transfer.add_url('http://example.com/good.torrent')

download file as zip:
>>> client.File.download_zip(new['file_id'], dest='/tmp/')

"""

import os
import re
import json
import logging
import requests
import datetime
from time import sleep

BASE_URL = 'https://api.put.io/v2'

LOGGER = logging.getLogger(__name__)


class Client(object):
    """
    Creates a session to put.io with your access token.

    Variables:
        File
            Gives access to file actions
        Transfer
            Gives access to transfer actions
    """
    def __init__(self, access_token):
        self.access_token = access_token
        self.session = requests.session()

        self.File = _File(self)  # Contain file actions
        self.Transfer = _Transfer(self)  # Contain transfer actions
        self._last_response = None

    def check_token(self):
        """Returns True if token is valid, False otherwise"""
        r = self.request('/account/info')
        return 'status' in r and r['status'] == 'OK'

    def sa_request_get(self, url, raw=False, headers=None):
        """
        "Standalone" get request (not using session)
        """
        return requests.get(url, allow_redirects=True, stream=raw, headers=headers)

    def request(self, path, method='GET', params=None, data=None, files=None,
                headers=None, raw=False, timeout=60):
        """
        Wrapper around requests.request()

        Prepends BASE_URL to path.
        Inserts oauth_token to query params.
        Parses response as JSON and returns it.

        """
        if not params:
            params = {}

        if not headers:
            headers = {}

        # All requests must include oauth_token
        params['oauth_token'] = self.access_token

        headers['Accept'] = 'application/json'

        url = BASE_URL + path
        LOGGER.debug('url: %s', url)

        #Try 3 times:
        for i in range(1, 4):
            try:
                response = self.session.request(
                    method, url, params=params, data=data, files=files,
                    headers=headers, allow_redirects=True, stream=raw, timeout=timeout)
                LOGGER.debug('response: %s', response)
                break
            except requests.exceptions.ConnectionError as e:
                LOGGER.debug(e)
                LOGGER.warning('A Connection error occurred (%s/3)' % i)
                response = None
                if i != 3:  # Don't sleep the last time
                    sleep(5*i)

        self._last_response = response

    #On failed response:
        if response is None:
            LOGGER.error('Failed to connect %s times, giving up' % i)
            if raw:
                return
            else:
                return {'status': 'ERROR',
                        'error_type': 'A Connection error occurred',
                        'error_message': 'Failed to connect %s times, giving up' % i}

    #On success response:
        if raw:
            return response

        LOGGER.debug('content: %s', response.content)
        try:
            response = json.loads(response.content)
        except ValueError:
            LOGGER.error('Received invalid JSON from put.io')
            LOGGER.error('invalid-content: %s', response.content)
            response = {'status': 'ERROR',
                        'error_type': 'JSON ValueError',
                        'error_message': 'Received invalid JSON from put.io'}

        return response


class _File(object):
    """A class for file operations"""
    def __init__(self, parent):
        self.client = parent

    def get(self, id):
        """Returns a file's properties"""
        d = self.client.request('/files/%i' % id)
        if d['status'] == 'ERROR':
            return {}
        return d['file']

    def list(self, parent_id=0):
        """Lists files in a folder"""
        d = self.client.request('/files/list', params={'parent_id': parent_id})
        if d['status'] == 'ERROR':
            return {}
        return d['files']

    def upload(self, path, name=None):
        """Uploads a file. If the uploaded file is a torrent file, starts it as a transfer"""
        with open(path) as f:
            if name:
                files = {'file': (name, f)}
            else:
                files = {'file': f}
            d = self.client.request('/files/upload', method='POST', files=files)
        if d['status'] == 'ERROR':
            return {}
        return d['file']

    #TODO: rewrite this function ...... !!
    def download(self, file_id, dest='.', url=False, progress_callback=None):
        """Download the contents of the file

        Returns the filename on success, or False otherwise.
        """
        if not url:
            response = self.client.request('/files/%s/download' % file_id, raw=True)
        else:
            response = self.client.sa_request_get(url, raw=True)

        if not response or not response.ok:
            if url:
                s = 'Failed to download url: %s' % (url)
            else:
                s = 'Failed to download file with id: %s' % (file_id)

            LOGGER.error(s)

            if progress_callback:
                progress_callback({
                    'string': s
                })

            return

        filename = re.match(
            'attachment; filename=(?:"(.*)"|(.*))',
            response.headers['Content-Disposition'])
        filename = filename.groups()[0] or filename.groups()[1]
        total_length = response.headers.get('Content-Length')

        if total_length is not None and not total_length:
            LOGGER.warning('content-length == 0. Exiting')
            return False

        with open(os.path.join(dest, filename), 'ab') as f:
            current_time = datetime.datetime.now()
            if total_length is None:
                f.seek(0)  # overwrite existing file
                f.write(response.content)  # No error checkking can be done, just dump data
            else:
                total_length = int(total_length)
                LOGGER.info('Total size of file: %s MB' % (total_length / 1024 / 1024))
                attempts = 1
                MAX_ATTEMPTS = 5
                orig_file_size = f.tell()

                if not orig_file_size:
                    #New file, start downloading
                    self._write_data_with_progress(f, response, total_length, progress_callback=progress_callback)
                else:
                    LOGGER.info('Continuing previously partially downloaded file: %s' % filename)
                    attempts = 0

                #If we did not receive everything, try to download missing chunks.
                old_tell = orig_file_size
                while f.tell() != total_length:
                    if attempts == 0:
                        attempts += 1
                    else:
                        LOGGER.warning('Download failed (%s/%s), will try to resume. Got %s of %s bytes (%s %%)'
                                       % (attempts, MAX_ATTEMPTS, f.tell(), total_length, int(f.tell() * 100 / total_length)))
                    headers = {'range': 'bytes=%s-' % (f.tell())}  # Get rest of file
                    if not url:
                        response = self.client.request('/files/%s/download' % file_id, raw=True, headers=headers)
                    else:
                        response = self.client.sa_request_get(url, raw=True, headers=headers)

                    if not response or response.headers.get('Content-Length') is None:
                        s = 'Failed to resume download'

                        LOGGER.error(s)

                        if progress_callback:
                            progress_callback({
                                'string': s
                            })

                        return

                    partial_length = int(response.headers.get('Content-Length'))
                    LOGGER.info('Getting partial file with size: %s MB' % (partial_length / 1024 / 1024))
                    self._write_data_with_progress(f, response, partial_length, progress_callback=progress_callback)

                    #If we did not progress, stop trying after MAX_ATTEMPTS
                    if f.tell() == old_tell:
                        if attempts == MAX_ATTEMPTS:  # Give up:
                            s = 'Failed to download rest of file after %s tries. Got %s of %s bytes' % (attempts, f.tell(), total_length)

                            LOGGER.error(s)

                            if progress_callback:
                                progress_callback({
                                    'string': s
                                })

                            return
                        attempts += 1
                    #If we DID progress, reset variables.
                    else:
                        old_tell = f.tell()
                        attempts = 1

                download_time = datetime.datetime.now() - current_time
                download_speed = int((f.tell() - orig_file_size) / download_time.total_seconds() / 1024)
                speed_unit = 'KB/s'

                if download_speed >= 1024:
                    download_speed /= 1024.0
                    download_speed = '%.2f' % download_speed
                    speed_unit = 'MB/s'

                LOGGER.info('Download time: %s, avg speed: %s %s' % (download_time, download_speed, speed_unit))

        return filename

    def download_zip(self, file_id, dest='.', progress_callback=None):
        """Downloads the contents of the file as a zip archive

        Returns the filename on success, or False otherwise.
        """
        response = self.client.request('/files/zip', params={'file_ids': str(file_id)})

        if not response or not 'zip_id' in response:
            LOGGER.error('Failed to request zip file for file: %s', file_id)
            return False

        zip_id = response['zip_id']
        LOGGER.debug('Got zip id: %s', zip_id)

        sleep(3)

        response = self.client.request('/files/zipcheck/%s' % zip_id)
        while response:
            if not 'status' in response:
                LOGGER.error('No "status" in response')
                return False

            if response['status'] != 'OK':
                LOGGER.error('Status not ok: %s', response['status'])
                return False

            if 'url' in response and response['url']:
                LOGGER.debug('Got zip url: %s', response['url'])
                if 'size' in response:
                    LOGGER.debug('zip size: %s', response['size'])
                if 'missing_files' in response and response['missing_files']:
                    LOGGER.error('Missing files in zip, aborting zip download: %s (file id: %s)', zip_id, file_id)
                    return False
                return self.download(file_id, dest, url=response['url'], progress_callback=progress_callback)

            LOGGER.debug('Waiting for zipcheck')
            sleep(5)
            response = self.client.request('/files/zipcheck/%s' % zip_id)

        LOGGER.error('Failed zipcheck for zip: %s (file id: %s)', zip_id, file_id)
        return False

    def delete(self, file_id):
        """Deletes given files"""
        d = self.client.request('/files/delete', method='POST', data={'file_ids': str(file_id)})
        if d['status'] == 'ERROR':
            return {}
        return d

    def create_folder(self, name, parent_id=''):
        """Creates a new folder."""
        d = self.client.request('/files/create-folder', method='POST', data={'name': str(name), 'parent_id': str(parent_id)})
        if d['status'] == 'ERROR':
            return {}
        return d['file']

    @staticmethod
    def _write_data_with_progress(target_file, source, length, progress_callback=None):
        """Download a file with progress"""
        start_time = datetime.datetime.now()
        downloaded = 0
        chunks = 0
        steps = 1
        wanted_chunksize = 1024*1024
        chunk = (length / 4) or 1

        while chunk > wanted_chunksize:
            chunk /= 4
            steps *= 2

        chunk_step = length / (1024*1024) / steps
        if not chunk_step:
            chunk_step = 1

        LOGGER.info('Download chunksize: %s, steps: %s (%sMB)', wanted_chunksize, steps, chunk_step)
        for data in source.iter_content(wanted_chunksize):
            downloaded += wanted_chunksize
            target_file.write(data)
            chunks += 1
            if not chunks % chunk_step:
                download_time = datetime.datetime.now() - start_time
                download_speed = int((chunks * wanted_chunksize) / download_time.total_seconds() / 1024)
                speed_unit = 'KB/s'

                if download_speed >= 1024:
                    download_speed /= 1024
                    speed_unit = 'MB/s'

                s = 'Download progress: %.1f%% (%dMB) speed: %d %s - (Time: %s)' % ((100.0 * downloaded / length), downloaded/1024/1024, download_speed, speed_unit, download_time)
                LOGGER.info(s)
                if progress_callback:
                    progress_callback({
                        'string': s,
                        'downloaded': downloaded,
                        'length': length,
                        'download_speed': download_speed,
                        'speed_unit': speed_unit,
                        'download_time': download_time
                    })


class _Transfer(object):
    """A class for transfer operations"""
    def __init__(self, parent):
        self.client = parent

    def list(self):
        """Lists active transfers. If transfer is completed, it is removed from the list"""
        d = self.client.request('/transfers/list')
        if d['status'] == 'ERROR':
            return {}
        return d['transfers']

    def clean(self):
        """Clean completed transfers from the list."""
        d = self.client.request('/transfers/clean', method='POST')
        if d['status'] == 'ERROR':
            return {}
        return d

    def get(self, id):
        """Returns a transfer's properties"""
        d = self.client.request('/transfers/%i' % id)
        if d['status'] == 'ERROR':
            return {}
        return d['transfer']

    def cancel(self, id):
        """Deletes the given transfers."""
        d = self.client.request('/transfers/cancel', method='POST',
                                data=dict(transfer_ids=id))
        if d['status'] == 'ERROR':
            return {}
        return d['transfer']

    def add_url(self, url, parent_id=0, extract=False, callback_url=None):
        """Adds a new transfer"""
        d = self.client.request('/transfers/add', method='POST', data=dict(
            url=url, parent_id=parent_id, extract=extract,
            callback_url=callback_url))
        if d['status'] == 'ERROR':
            return {}
        return d['transfer']

    def add_torrent(self, path, parent_id=0, extract=False, callback_url=None):
        """Adds a new torrent transfer"""
        with open(path) as f:
            files = {'file': f}
            d = self.client.request('/files/upload', method='POST', files=files,
                                    data=dict(parent_id=parent_id,
                                              extract=extract,
                                              callback_url=callback_url))
        if d['status'] == 'ERROR':
            return {}
        return d['transfer']
