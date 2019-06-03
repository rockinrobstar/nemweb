"""Module for downloading data for online resources.

Module will download and create a local copy of files.  This local copy will be used
in order to speed up repeated requests for data."""

from io import BytesIO
import os
import requests
from nemweb import CONFIG

def get_file(remote_url, local_cache_id, file, cache=True):
    """Method that gets a file of any type (as BytesIO), retrieving the local (cached) 
    file first if it exists. Method checks the file size of the remote and if it's 
    bigger replaces the local file"""
    file_cache = CONFIG['local_settings']['file_cache']
    localfile_URL = ("{0}/{1}{2}").format(file_cache, local_cache_id, file)
    remotefile_URL = ("{0}{1}").format(remote_url, file)
    print(localfile_URL)
    print(remotefile_URL)
    try:
        remote_size = int(requests.get(remotefile_URL, stream=True, 
            headers={'Accept-Encoding': 'deflate'}).headers['Content-Length'])
        local_size = os.path.getsize(localfile_URL)
        print("remote_size " + str(remote_size) + " >= local_size " + str(local_size) + ": " + str(remote_size >= local_size))
        if remote_size >= local_size:
            with open (localfile_URL, 'r+b') as f:
                f_bytes = BytesIO(f.read())
                print("Read Local: " + localfile_URL)
        else:
            raise IOError("File too short")

    except (FileNotFoundError, IOError):
        response = requests.get(remotefile_URL)
        response.raise_for_status() #check for bad responses
        f_bytes = BytesIO(response.content)
        print("Read Remote: " + remotefile_URL)
        if cache is True:
            try:
                os.makedirs(os.path.dirname(localfile_URL), exist_ok=True)
                with open (localfile_URL, 'wb') as f:
                    f.write(response.content)
            except OSError:
                print("Failed to write Local file: " + localfile_URL)             
    return f_bytes