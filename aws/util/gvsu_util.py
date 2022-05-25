import os
import random
import sys
import json
import time
import uuid
import math
import posixpath
import datetime
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler
from argparse import ArgumentParser, Namespace
import queue
import ctypes
import multiprocessing
from multiprocessing import Process, Queue, Value, Lock, Manager
import urllib.parse
import hashlib
import requests
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
import hmac
import watchtower
import base64
import s3fs
from botocore.config import Config
import traceback
import datetime
import dateutil.tz
import gvsu_restartability as restartability

SUCCESS: int = 0
FAILURE: int = 1
S3_SSE_KMS: str = 'aws:kms'

# set s3fs
s3fs.S3FileSystem.read_timeout = 300
s3fs.S3FileSystem.connect_timeout = 300


###############################################################################
# Classes
###############################################################################

class CS3:
    """ Wrapper class for AWS S3 operations """

    def __init__(self, sse: str = S3_SSE_KMS):
        """ s3_resource: boto3.resources.factory.s3.ServiceResource """
        self.__s3_resource = boto3.resource('s3')
        self.__s3_file_system = s3fs.S3FileSystem(
            anon=False,
            s3_additional_kwargs={'ServerSideEncryption': sse, 'ACL': 'bucket-owner-full-control'}
        )

    def set_s3fs(self, sse: str = S3_SSE_KMS):
        self.__s3_file_system = s3fs.S3FileSystem(
            anon=False,
            s3_additional_kwargs={'ServerSideEncryption': sse, 'ACL': 'bucket-owner-full-control'}
        )

    @property
    def resource(self):
        return self.__s3_resource

    @property
    def client(self):
        return self.__s3_resource.meta.client

    @property
    def s3fs(self) -> s3fs.S3FileSystem:
        return self.__s3_file_system

    def s3fs_exists(self, s3_url: str):
        self.__s3_file_system.invalidate_cache(s3_url)
        return self.__s3_file_system.exists(s3_url)

    def s3fs_delete_folder(self, s3_url: str):
        url: str = posixpath.join(s3_url, '')

        if not self.__s3_file_system.exists(url):
            return

        if not self.__s3_file_system.isdir(url):
            raise RuntimeError(f"Argument '{url}' is not an S3 folder.")

        self.__s3_file_system.rm(url, recursive=True)
        self.wait_until_url_not_exists(url)

    def s3fs_delete_file(self, s3_url: str):
        url: str = s3_url.strip()

        if not self.__s3_file_system.exists(url):
            return

        if not self.__s3_file_system.isfile(url):
            raise RuntimeError(f"Argument '{s3_url}' is not an S3 file.")

        self.__s3_file_system.rm(s3_url, recursive=False)
        self.wait_until_url_not_exists(s3_url)

    def s3fs_touch(self, s3_url: str):
        self.__s3_file_system.touch(s3_url)
        self.wait_until_url_exists(s3_url)

    def key_exists(self, bucket: str, key: str) -> bool:
        """
            Check whether an S3 object exists by key either physically or logically.
            Take an S3 folder as an example. This folder may not exist PHYSICALLY in S3, but its
            children (i.e. its child files or child folders) exist physically. In this case we consider that
            this folder exists LOGICALLY in S3.
        """
        response: dict = self.__s3_resource.meta.client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=3)
        return response['KeyCount'] > 0

    def url_exists(self, s3_url: str) -> bool:
        """
            Check whether an S3 object exists by url either physically or logically.
            Take an S3 folder as an example. This folder may not exist PHYSICALLY in S3, but its
            children (i.e. its child files or child folders) exist physically. In this case we consider that
            this folder exists LOGICALLY in S3.
        """
        bucket, obj_key = parse_url(s3_url)
        return self.key_exists(bucket, obj_key)

    def key_exists_physically(self, bucket: str, key: str) -> bool:
        try:
            obj = self.__s3_resource.meta.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response['Error']['Code'] == '404' or exc.response['Error']['Code'] == '403':
                return False
            else:
                raise

    def url_exists_physically(self, s3_url: str) -> bool:
        bucket, obj_key = parse_url(s3_url)
        return self.key_exists_physically(bucket, obj_key)

    def get_object(self, bucket: str, obj_key: str) -> dict:
        obj = self.__s3_resource.Object(bucket, obj_key)
        content: str = obj.get()['Body'].read().decode('utf-8')
        return json.loads(content)

    def get_object_by_url(self, s3_url: str) -> dict:
        bucket, obj_key = parse_url(s3_url)
        return self.get_object(bucket, obj_key)

    def get_head_object(self, bucket: str, key: str) -> dict:
        obj = self.__s3_resource.meta.client.head_object(Bucket=bucket, Key=key)
        return obj

    def get_head_object_by_url(self, s3_url: str) -> dict:
        bucket, obj_key = parse_url(s3_url)
        return self.get_head_object(bucket, obj_key)

    def put_object(self, bucket: str, obj_key: str, content: str, encryption: str = S3_SSE_KMS,
                   acl: str = None) -> None:
        obj = self.__s3_resource.Object(bucket, obj_key)
        if acl:
            obj.put(Body=str.encode(content), ServerSideEncryption=encryption, ACL=acl)
        else:
            obj.put(Body=str.encode(content), ServerSideEncryption=encryption)

    def put_object_by_url(self, url: str, content: str, encryption: str = S3_SSE_KMS, acl: str = None) -> None:
        bucket, obj_key = parse_url(url)
        self.put_object(bucket, obj_key, content, encryption, acl)

    def put_dict_object(self, bucket: str, obj_key: str, content: dict, acl: str = None) -> None:
        obj = self.__s3_resource.Object(bucket, obj_key)
        if acl:
            obj.put(Body=(bytes(json.dumps(content, indent=4).encode('UTF-8'))), ServerSideEncryption='aws:kms',
                    ACL=acl,
                    SSEKMSKeyId=get_kms_key_id())
        else:
            obj.put(Body=(bytes(json.dumps(content, indent=4).encode('UTF-8'))), ServerSideEncryption='aws:kms',
                    SSEKMSKeyId=get_kms_key_id())

    def put_object_dict_by_url(self, url: str, content: dict, acl: str = None) -> None:
        bucket, obj_key = parse_url(url)
        self.put_dict_object(bucket, obj_key, content, acl)

    def upload_object(self, bucket: str, obj_key: str, local_file: str, encryption: str = S3_SSE_KMS) -> None:
        obj = self.__s3_resource.Object(bucket, obj_key)
        with open(local_file, 'rb') as file:
            obj.upload_fileobj(file, ExtraArgs={"ServerSideEncryption": encryption})

    def upload_object_by_url(self, url: str, src_file: str, encryption: str = S3_SSE_KMS) -> None:
        bucket, obj_key = parse_url(url)
        self.upload_object(bucket, obj_key, src_file, encryption)

    def check_and_create_prefix(self, bucket: str, prefix_key: str, encryption: str = S3_SSE_KMS,
                                acl: str = None) -> None:
        """ s3_resource: boto3.resources.factory.s3.ServiceResource """
        # add '/' to the end to make it an S3 prefix
        prefix_key = join_path(prefix_key, '')
        if not self.key_exists(bucket, prefix_key):
            self.put_object(bucket, prefix_key, '', encryption, acl)

    def delete_object(self, bucket: str, obj_key: str) -> None:
        self.__s3_resource.Object(bucket, obj_key).delete()

    def delete_object_by_url(self, s3_url: str) -> None:
        bucket, obj_key = parse_url(s3_url)
        self.delete_object(bucket, obj_key)

    def copy_object(self, src_bucket: str, src_key: str, tgt_bucket: str, tgt_key: str,
                    encryption: str = S3_SSE_KMS) -> None:
        source: dict = {
            'Bucket': src_bucket,
            'Key': src_key
        }
        self.__s3_resource.meta.client.copy(source, tgt_bucket, tgt_key, ExtraArgs={
            'ServerSideEncryption': encryption,
            'ACL': 'bucket-owner-full-control'
        })

    def wait_until_object_exists(self, bucket: str, obj_key: str) -> None:
        try:
            wait_delay: int = int(os.environ.get('WAIT_DELAY', 5))
            wait_max_attempts: int = int(os.environ.get('WAIT_MAX_ATTEMPTS', 120))
            waiter = self.__s3_resource.meta.client.get_waiter('object_exists')
            logger.debug("waiting on object. bucket=%s;  key=%s", bucket, obj_key)
            waiter.wait(Bucket=bucket, Key=obj_key,
                        WaiterConfig={'Delay': wait_delay, 'MaxAttempts': wait_max_attempts})
        except BaseException as ex:
            msg: str = f"Failed in locating the specified object. Please check if it exists in S3. Bucket={bucket}, Key={obj_key}"
            raise RuntimeError(msg) from ex

    def wait_until_object_not_exists(self, bucket: str, obj_key: str) -> None:
        try:
            wait_delay: int = int(os.environ.get('WAIT_DELAY', 5))
            wait_max_attempts: int = int(os.environ.get('WAIT_MAX_ATTEMPTS', 120))
            waiter = self.__s3_resource.meta.client.get_waiter('object_not_exists')
            logger.debug("waiting on object. bucket=%s;  key=%s", bucket, obj_key)
            waiter.wait(Bucket=bucket, Key=obj_key,
                        WaiterConfig={'Delay': wait_delay, 'MaxAttempts': wait_max_attempts})
        except BaseException as ex:
            msg: str = f"Failed in waiting object. Please check if it exists in S3. Bucket={bucket}, Key={obj_key}"
            raise RuntimeError(msg) from ex

    def wait_until_url_exists(self, s3_url: str) -> None:
        """ s3_resource: boto3.resources.factory.s3.ServiceResource """
        bucket, obj_key = parse_url(s3_url)
        self.wait_until_object_exists(bucket, obj_key)

    def wait_until_url_not_exists(self, s3_url: str) -> None:
        """ s3_resource: boto3.resources.factory.s3.ServiceResource """
        bucket, obj_key = parse_url(s3_url)
        self.wait_until_object_not_exists(bucket, obj_key)

    def wait_and_get_object(self, bucket: str, obj_key: str) -> dict:
        self.wait_until_object_exists(bucket, obj_key)
        logger.debug("reading object. bucket=%s;  key=%s", bucket, obj_key)
        return self.get_object(bucket, obj_key)

    def wait_and_get_object_by_url(self, s3_url: str) -> dict:
        bucket, obj_key = parse_url(s3_url)
        return self.wait_and_get_object(bucket, obj_key)

    def wait_for_folder(self, file_count: int, bucket: str, folder_key: str) -> None:
        """ Ignore folder name itself and _SUCCESS file """
        wait_delay: int = int(os.environ.get('WAIT_DELAY', 5))
        wait_max_attempts: int = int(os.environ.get('WAIT_MAX_ATTEMPTS', 120))

        # wait until prefix exists
        folder_key = join_path(folder_key, '')
        self.wait_until_object_exists(bucket, folder_key)

        # wait until objects under prefix exist
        start_time: float = time.time()
        while True:
            number_of_files_found: int = self.__get_file_count_in_folder(bucket, folder_key)
            if number_of_files_found == file_count:
                logger.debug("Found all files under folder '%s' in bucket '%s'.", folder_key, bucket)
                return
            elif number_of_files_found > file_count:
                raise RuntimeError(
                    f"Found too many files. Should be {file_count} files instead of {number_of_files_found}")

            # sleep for xx seconds
            logger.debug("Some files are still missing. Waiting...")
            time.sleep(wait_delay)

            # time out
            end_time: float = time.time()
            delta: float = end_time - start_time  # in seconds
            logger.debug("Time Difference: %s", delta)

            if delta > wait_delay * wait_max_attempts:
                err: str = f"Timeout - didn't find all files for {folder_key} in {delta} seconds."
                raise RuntimeError(err)

    def wait_for_folder_by_url(self, file_count: int, s3_url: str) -> None:
        bucket, obj_key = parse_url(s3_url)
        self.wait_for_folder(file_count, bucket, obj_key)

    def put_and_wait_object(self, bucket: str, obj_key: str, content: str, encryption: str = S3_SSE_KMS,
                            acl: str = None):
        self.put_object(bucket, obj_key, content, encryption, acl)
        self.wait_until_object_exists(bucket, obj_key)

    def put_and_wait_object_by_url(self, s3_url: str, content: str, encryption: str = S3_SSE_KMS, acl: str = None):
        bucket, obj_key = parse_url(s3_url)
        self.put_and_wait_object(bucket, obj_key, content, encryption, acl)

    def upload_folder(self, bucket: str, s3_folder: str, local_dir: str) -> None:
        """ Upload a directory on a local computer to S3 recursively, including all files """
        # check and create S3 prefix
        self.check_and_create_prefix(bucket, s3_folder)

        # change '\' to '/' for local_dir
        local_dir = local_dir.strip().replace('\\', '/')

        # upload the missing files under the folder (only add direct children, no recursive calls)
        for path, sub_dirs, files in os.walk(local_dir):
            # path = path.replace('\\', '/')
            s3_file_path_list: list = []
            for file in files:
                local_file_path: str = join_path(path, file)
                s3_file_path: str = join_path(s3_folder, file)
                if not self.key_exists(bucket, s3_file_path):
                    self.upload_object(bucket, s3_file_path, local_file_path)
                    s3_file_path_list.append(s3_file_path)
            for s3_file_path in s3_file_path_list:
                self.wait_until_object_exists(bucket, s3_file_path)
            break
        pass

    def download_folder(self, bucket: str, rpath: str, lpath: str):
        self.__download_folder(bucket, rpath, len(rpath), lpath)
        pass

    def download_folder_by_url(self, s3_url: str, lpath: str):
        bucket, obj_key = parse_url(s3_url)
        self.download_folder(bucket, obj_key, lpath)

    def __download_folder(self, bucket: str, rpath: str, rpath_len: int, lpath: str):
        """ List the short name of direct sub-folders """
        rpath: str = posixpath.join(rpath, '') if rpath else rpath
        # folder_key_len: int = len(rpath)

        kwargs = {'Bucket': bucket, 'Prefix': rpath, 'Delimiter': '/'}
        while True:  # one page per loop
            resp = self.__s3_resource.meta.client.list_objects_v2(**kwargs)

            # sub-folders
            if resp.get('CommonPrefixes'):
                for sub_folder in resp.get('CommonPrefixes'):
                    self.__download_folder(bucket, sub_folder.get('Prefix'), rpath_len, lpath)
            # files
            for file in resp.get('Contents', []):
                file_key: str = file.get('Key')
                local_file_name: str = file_key[rpath_len:].lstrip('/')
                local_file_path: str = posixpath.join(lpath, local_file_name)
                if file_key.endswith('/'):
                    if not posixpath.exists(posixpath.dirname(local_file_path)):
                        os.makedirs(posixpath.dirname(local_file_path))
                else:
                    self.__s3_resource.meta.client.download_file(bucket, file.get('Key'), local_file_path)

            # pagination
            if 'NextContinuationToken' in resp:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            else:
                break

    def list_subfolders(self, bucket: str, folder_key: str):
        """ Deprecate this method since 8/3/2020 """
        if folder_key:  folder_key = join_path(folder_key, '')
        response = self.__s3_resource.meta.client.list_objects_v2(Bucket=bucket, Prefix=folder_key, Delimiter='/')
        folders: list = response.get('CommonPrefixes')
        if folders:
            for obj in folders:
                yield obj.get('Prefix')
        pass

    def list_subfolder_names(self, bucket: str, folder_key: str):
        """ List the short name of direct sub-folders """
        if folder_key:  folder_key = join_path(folder_key, '')
        folder_key_len: int = len(folder_key)

        kwargs = {'Bucket': bucket, 'Prefix': folder_key, 'Delimiter': '/'}
        while True:
            resp = self.__s3_resource.meta.client.list_objects_v2(**kwargs)
            folders: list = resp.get('CommonPrefixes')
            if folders:
                for obj in folders:
                    full_sub_folder: str = obj.get('Prefix')
                    sub_folder_name: str = full_sub_folder[folder_key_len:]
                    yield sub_folder_name.rstrip('/')

            if 'NextContinuationToken' in resp:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            else:
                break

    def list_subfolder_names_by_url(self, folder_url: str):
        bucket, obj_key = parse_url(folder_url)
        return self.list_subfolder_names(bucket, obj_key)

    def list_files(self, bucket: str, folder_key: str):
        """
            Get all files under a folder page by page
        """
        if folder_key:  folder_key = join_path(folder_key, '')
        kwargs = {'Bucket': bucket, 'Prefix': folder_key}

        while True:
            resp = self.__s3_resource.meta.client.list_objects_v2(**kwargs)

            for obj in resp['Contents']:
                if obj.get('Key') == folder_key:  continue
                yield obj

            if 'NextContinuationToken' in resp:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            else:
                break

    def list_file_keys(self, bucket: str, folder_key: str):
        if folder_key:  folder_key = join_path(folder_key, '')

        kwargs = {'Bucket': bucket, 'Prefix': folder_key}
        while True:
            resp = self.__s3_resource.meta.client.list_objects_v2(**kwargs)

            for obj in resp['Contents']:
                obj_key: str = obj.get('Key')
                if obj_key == folder_key:  continue
                yield obj_key

            if 'NextContinuationToken' in resp:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            else:
                break

    def list_file_urls(self, folder_url):
        bucket, folder_key = parse_url(folder_url)
        if folder_key:  folder_key = join_path(folder_key, '')

        kwargs = {'Bucket': bucket, 'Prefix': folder_key}
        while True:
            resp = self.__s3_resource.meta.client.list_objects_v2(**kwargs)

            for obj in resp['Contents']:
                obj_key: str = obj.get('Key')
                if obj_key == folder_key:  continue
                scheme, *_ = folder_url.split(':')
                file_key_url: str = f"{scheme}://{bucket}/{obj_key}"
                yield file_key_url

            if 'NextContinuationToken' in resp:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            else:
                break

    def list_files_by_url_v2(self, s3_folder_url: str, recursive: bool = False) -> (list, list):
        bucket, folder_key = parse_url(s3_folder_url)
        return self.list_files_v2(bucket, folder_key, recursive)

    def list_files_v2(self, bucket: str, folder_key: str, recursive: bool = False) -> (list, list):
        """
            Get files and sub_folders under a given folder page by page.
            Return: (sub_files, sub_folders)
                sub_files: a list of dicts (list[dict]). Each list element is an object.
                sub_folders: a list of strings (list[str]). Each list element is a folder key.
        """
        sub_files: list = list()
        sub_folders: list = list()

        self.__list_files_ext_v2(bucket, folder_key, recursive, sub_files, sub_folders)
        return sub_files, sub_folders

    def __list_files_ext_v2(self, bucket: str, folder_key: str, recursive: bool, sub_files: list,
                            sub_folders: list) -> None:
        folder_key = join_path(folder_key, '')
        child_folders: list = list()
        kwargs = {'Bucket': bucket, 'Prefix': folder_key, 'Delimiter': '/'}

        while True:
            resp = self.__s3_resource.meta.client.list_objects_v2(**kwargs)

            files: list = resp.get('Contents')
            if files:
                for file in files:
                    if file['Key'] == folder_key:  continue
                    sub_files.append(file)

            folders: list = resp.get('CommonPrefixes')
            if folders:
                for folder in folders:
                    folder_pref: str = folder['Prefix']
                    if folder_pref == folder_key:  continue
                    sub_folders.append(folder_pref)
                    child_folders.append(folder_pref)

            if 'NextContinuationToken' in resp:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            else:
                break

        # recursive
        if recursive:
            for child_folder in child_folders:
                self.__list_files_ext_v2(bucket, child_folder, recursive, sub_files, sub_folders)

    def read_manifest_file(self, manifest_bucket, manifest_objkey) -> dict:
        return self.wait_and_get_object(manifest_bucket, manifest_objkey)

    def get_file_count(self, bucket: str, folder_key: str, extension: str = None) -> int:
        file_count: int = 0
        bucket_obj = self.__s3_resource.Bucket(bucket)
        folder_key = join_path(folder_key, '')
        for output_part_file in bucket_obj.objects.filter(Prefix=folder_key):
            if (extension is None) or (extension in output_part_file.key):
                file_count += 1
        logger.debug("files_count: %s", file_count)
        return file_count

    def get_file_count_by_url(self, s3_folder_url: str, extension: str = None) -> int:
        bucket, folder_key = parse_url(s3_folder_url)
        return self.get_file_count(bucket, folder_key, extension)

    def __get_file_count_in_folder(self, bucket: str, folder_key: str) -> int:
        response = self.__s3_resource.meta.client.list_objects_v2(Bucket=bucket, Prefix=folder_key, MaxKeys=500)
        logger.debug('list s3 response: %s', response)

        file_list: list = response.get('Contents')
        if not file_list:  return 0
        success_key: str = join_path(folder_key, '_SUCCESS')
        number_of_files_found: int = 0

        for file in file_list:
            file_key: str = file['Key']
            if file_key != folder_key and file_key != success_key:
                number_of_files_found += 1
        return number_of_files_found

    def delete_folder(self, bucket: str, folder_key: str) -> None:
        folder_key: str = join_path(folder_key, '')
        if not self.key_exists(bucket, folder_key):
            return

        for file_key in self.list_file_keys(bucket, folder_key):
            logger.debug("deleting: %s/%s", bucket, file_key)
            self.delete_object(bucket, file_key)

        if self.key_exists(bucket, folder_key):
            self.delete_object(bucket, folder_key)

    def delete_folder_by_url(self, s3_folder_url: str) -> None:
        bucket, folder_key = parse_url(s3_folder_url)
        return self.delete_folder(bucket, folder_key)

    def delete_merged_files(self, bucket: str, folder_key: str, keyword: str) -> None:
        folder_key: str = join_path(folder_key, '')
        if not self.key_exists(bucket, folder_key):
            return

        for file_key in self.list_file_keys(bucket, folder_key):
            if keyword in file_key:
                logger.debug("deleting: %s/%s", bucket, file_key)
                self.delete_object(bucket, file_key)

    def copy_folder(self, src_bucket: str, src_key: str, tgt_bucket: str, tgt_key: str,
                    encryption: str = S3_SSE_KMS) -> None:
        src_key: str = join_path(src_key, '')
        tgt_key: str = join_path(tgt_key, '')

        # delete target folder first
        self.delete_folder(tgt_bucket, tgt_key)

        # copy files under source folder
        for src_file in self.list_file_keys(src_bucket, src_key):
            logger.debug("copying: %s/%s", src_bucket, src_file)
            *_, short_file_name = src_file.split('/')

            tgt_file: str = f"{tgt_key}{short_file_name}"
            self.copy_object(src_bucket, src_file, tgt_bucket, tgt_file, encryption)

    def get_folder_size(self, bucket: str, folder_key: str) -> int:
        folder_key: str = join_path(folder_key, '')
        if not self.key_exists(bucket, folder_key):
            return 0

        size: int = 0
        for file in self.list_files(bucket, folder_key):
            size += file.get('Size')
        return size

    def get_folder_size_by_url(self, folder_url: str) -> int:
        bucket, folder_key = parse_url(folder_url)
        return self.get_folder_size(bucket, folder_key)


class CMultiRunStatus:
    def __init__(self):
        self.lock = Lock()
        manager = Manager()
        self.status = manager.Value('i', SUCCESS)
        self.err_msg = manager.Value(ctypes.c_wchar_p, "init")

    def set_failed(self, err_msg_arg: str):
        """ only takes first error """
        with self.lock:
            if self.status.value == SUCCESS:
                self.status.value = FAILURE
                self.err_msg.value = err_msg_arg

    def is_failed(self) -> bool:
        with self.lock:
            return self.status.value == FAILURE

    def get_err_msg(self) -> str:
        with self.lock:
            return self.err_msg.value


class CS3Merger:
    """ This class merges all files under an S3 folder to a single S3 file using multipart-copy """

    def __init__(self, sse: str = S3_SSE_KMS):
        self.sse = sse

    def merge_files_under_folder_by_url(self, src_folder_url: str, tgt_file_url: str, parallelism: int = 0,
                                        file_ext: str = None):
        src_bucket, src_folder_key = parse_url(src_folder_url)
        tgt_bucket, tgt_file_key = parse_url(tgt_file_url)
        self.merge_files_under_folder(src_bucket, src_folder_key, tgt_bucket, tgt_file_key, parallelism, file_ext)

    def merge_files_under_folder(self, src_bucket: str, src_folder_key: str, tgt_bucket: str, tgt_file_key: str,
                                 parallelism: int = 0, file_ext: str = None):
        ##### Step 0 - get src_files from src folder
        start_time: float = time.time()
        s3: CS3 = CS3(self.sse)

        src_folder_key = posixpath.join(src_folder_key, '')
        src_files, _ = s3.list_files_v2(src_bucket, src_folder_key, False)
        src_files.sort(key=lambda src_file: src_file['Key'])

        ##### Step 1 - create_multipart_upload
        cmu_resp: dict = s3.client.create_multipart_upload(
            Bucket=tgt_bucket,
            Key=tgt_file_key,
            ACL='bucket-owner-full-control',
            ServerSideEncryption=self.sse
        )

        ##### Step 2 - upload_part_copy for every part
        completed_parts: list = self.__merge_parts_driver(s3, src_bucket, src_folder_key, src_files, cmu_resp,
                                                          parallelism, file_ext)
        completed_parts.sort(key=lambda completed_part: completed_part['PartNumber'])

        ##### Step 3: complete_multipart_upload
        s3.client.complete_multipart_upload(
            Bucket=cmu_resp['Bucket'],
            Key=cmu_resp['Key'],
            UploadId=cmu_resp['UploadId'],
            MultipartUpload={'Parts': completed_parts}
        )

        end_time: float = time.time()
        delta: float = end_time - start_time  # in seconds
        logger.info("It took %.3f seconds to merge %d files.", delta, len(completed_parts))

    def __merge_parts_driver(self, s3: CS3, src_bucket: str, src_folder_key: str, src_files: list, cmu_resp: dict,
                             parallelism: int, file_ext: str) -> list:
        ##### populate input queue
        in_que: Queue = Queue()
        out_que: Queue = Queue()
        part_num: int = 0

        # AWS only allows 10,00 parts at most. So in_que will have at most 10,000 elements.
        for src_file in src_files:
            if src_file['Size'] == 0 or (file_ext and (not src_file['Key'].endswith(file_ext))):  continue

            part_num: int = part_num + 1
            in_que.put((part_num, src_file['Key'], src_file['Size'] - 1), block=False)

        ##### set other variables
        num_parts: int = part_num
        completed_parts: list = list()

        if parallelism <= 0:
            parallelism: int = 5 * multiprocessing.cpu_count()

        parallelism: int = min(parallelism, num_parts)
        logger.info("final parallelism: %d", parallelism)
        logger.info("merging %d files...", num_parts)

        ##### launch child processes as a pool
        status = CMultiRunStatus()
        processes: list = list()

        for i in range(parallelism):
            p: Process = Process(
                target=merge_parts_runner,
                args=(status, in_que, out_que, src_bucket, cmu_resp['Bucket'], cmu_resp['Key'], cmu_resp['UploadId'],
                      self.sse,)
            )
            p.start()
            processes.append(p)

        for p in processes:  p.join()
        for p in processes:  p.close()

        if status.is_failed():
            time.sleep(10)
            abort_multipart_upload(s3, cmu_resp['Bucket'], cmu_resp['Key'], cmu_resp['UploadId'])
            raise RuntimeError(status.get_err_msg())

        ##### get completed_parts
        try:
            while True:
                completed_part: dict = out_que.get(block=False)
                completed_parts.append(completed_part)
        except queue.Empty as ex:
            # out_que is empty now. No more completed_part to process.
            pass

        if len(completed_parts) != num_parts:
            raise RuntimeError(
                f"Number of completed_parts ({len(completed_parts)}) is not equal to number of parts ({num_parts}).")
        return completed_parts


def merge_parts_runner(status: CMultiRunStatus, in_que: Queue, out_que: Queue, src_bucket: str, tgt_bucket: str,
                       tgt_key: str,
                       upload_id: str, sse: str) -> None:
    """ This function is executed by multiple child processes from a pool """
    s3: CS3 = CS3(sse)

    try:
        while True:
            if status.is_failed():
                break

            part_num, src_file_key, last_byte = in_que.get(block=False)

            resp: dict = s3.client.upload_part_copy(
                CopySource={'Bucket': src_bucket, 'Key': src_file_key},
                Bucket=tgt_bucket,
                Key=tgt_key,
                UploadId=upload_id,
                CopySourceRange=f"bytes=0-{last_byte}",
                PartNumber=part_num
            )

            completed_part: dict = {
                'ETag': resp['CopyPartResult']['ETag'],
                'PartNumber': part_num
            }
            out_que.put(completed_part, block=False)

            logger.debug("merged file: %s", posixpath.join("s3://", src_bucket, src_file_key))
    except queue.Empty as ex:
        # in_que is empty now. No more file for this process to work on.
        pass
    except BaseException as ex:
        err_msg: str = f"Error from [util.merge_parts_runner]:\n{traceback.format_exc()}"
        status.set_failed(err_msg)


def abort_multipart_upload(s3: CS3, tgt_bucket: str, tgt_key: str, upload_id: str) -> None:
    s3.client.abort_multipart_upload(
        Bucket=tgt_bucket,
        Key=tgt_key,
        UploadId=upload_id
    )


class Time:
    def __init__(self):
        self.time_zone = os.environ.get("TIME_ZONE")
        self.time_format = os.environ.get("TIME_FORMAT")
        self.dt_tz = dateutil.tz.gettz(self.time_zone)
        self.current_time = datetime.datetime.now(tz=self.dt_tz).strftime(self.time_format)


    def n_days_before_date(self, timeInDays):
        now = datetime.datetime.strptime(self.current_time,self.time_format)
        nDaysAgo = now - datetime.timedelta(days=timeInDays)
        return str(nDaysAgo)

    def getEpochTime(self, timeInDays):
        nDaysLater = datetime.datetime.today() + datetime.timedelta(days=timeInDays)
        nDaysLaterEpochTime = int(time.mktime(nDaysLater.timetuple()))

        return nDaysLaterEpochTime



class SimpleQueueService:
    def __init__(self):
        self.sqs_queue_client = boto3.client('sqs', config=Config(connect_timeout=30, read_timeout=300,
                                                                  retries={'max_attempts': 10}))

    def send_message_to_queue(self, queue_name: str, message_deduplication_id: str, message_group_id: str,
                              message_body: str):
        """ FIXME: this function supports only FIFO queue"""
        sqs_queue_url = self.sqs_queue_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        message_sent = self.sqs_queue_client.send_message(QueueUrl=sqs_queue_url, MessageBody=message_body,
                                                          MessageDeduplicationId=message_deduplication_id,
                                                          MessageGroupId=message_group_id)
        logger.info(f"message-sent-metadata : {message_sent}")
        return message_sent

    def receive_message_from_queue(self, queue_name: str, max_number_of_messages: int, visibility_timeout: int,
                                   wait_time_seconds: int = 20):
        sqs_queue_url = self.sqs_queue_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        response_messages = self.sqs_queue_client.receive_message(QueueUrl=sqs_queue_url,
                                                                  MaxNumberOfMessages=max_number_of_messages,
                                                                  VisibilityTimeout=visibility_timeout,
                                                                  WaitTimeSeconds=wait_time_seconds)

        return response_messages

    def delete_message_from_queue(self, queue_name: str, receipt_handle: str):
        sqs_queue_url = self.sqs_queue_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        delete_message = self.sqs_queue_client.delete_message(QueueUrl=sqs_queue_url,
                                                              ReceiptHandle=receipt_handle)
        logger.info(f"message deleted : {delete_message}")
        return delete_message


class CDynamodDB:
    """ Wrapper class for AWS DynamoDB operations """

    def __init__(self):
        self.dynamodb_resource = boto3.resource('dynamodb', config=Config(connect_timeout=30, read_timeout=300,
                                                                          retries={'max_attempts': 10}))
        self.dynamodb_client = boto3.client("dynamodb", config=Config(connect_timeout=30, read_timeout=300,
                                                                      retries={'max_attempts': 10}))

    def scan_DB(self, table_name: str, event: dict):
        """ Scans the Table with given Attribute and Item Value"""
        table = self.dynamodb_resource.Table(table_name)
        if len(event) == 1:
            col_name = list(event.keys())
            col_value = list(event.values())
            resp = table.scan(FilterExpression=Attr(col_name).eq(col_value))
        elif len(event) == 2:
            col1, col2 = list(event.keys())
            val1, val2 = list(event.values())
            resp = table.scan(
                FilterExpression=Attr(col1).eq(val1) & Attr(col2).eq(val2))
        elif len(event) == 3:
            col1, col2, col3 = list(event.keys())
            val1, val2, val3 = list(event.values())
            resp = table.scan(
                FilterExpression=Attr(col1).eq(val1) & Attr(col2).eq(val2) & Attr(col3).eq(val3))
        else:
            raise NotImplementedError("Input request exceeded the length.")
        return resp.get("Items")

    def scanBetweenDates(self, table_name, fromDate, toDate, column_name_1="start_time", column_name_2="end_time"):
        attr = boto3.dynamodb.conditions.Attr(column_name_1)
        attr1 = boto3.dynamodb.conditions.Attr(column_name_2)
        table = self.dynamodb_resource.Table(table_name)
        response = table.scan(
            FilterExpression=attr.between(fromDate, toDate) | attr1.between(fromDate, toDate)
        )
        return response

    def put_item_in_DB(self, table_name: str, jsonInput: dict):
        """ inserts the item into Dynamo DB"""
        table = self.dynamodb_resource.Table(table_name)
        table.put_item(Item=jsonInput)

    def update_item_in_DB(self, table_name: str, pk_name: str, pk_value: str, jsonInput: dict):
        # FIXME: this function supports only string type items , user has to provide table name,primary key column name,
        #  primary key column value and required attributes column names, values in json format
        """ json example : { column_1 : value_1 ,
                             column_2 : value_2 ,
                             ......
                             column_n : value_n } """

        upExp = 'set '
        exAtNames = '{'
        exAtValues = '{'
        cnt = 1
        for k, v in jsonInput.items():
            upExp += f'#col_name{cnt} = :col_value{cnt}'
            exAtNames += f'"#col_name{cnt}": "{k}"'
            exAtValues += f'":col_value{cnt}": '
            exAtValues += '{'
            if 'int' in str(type(v)):
                exAtValues += f'"N": "{v}"'
            else:
                exAtValues += f'"S": "{v}"'
            exAtValues += '}'
            if 1 < len(jsonInput) != cnt:
                upExp += ","
                exAtNames += ','
                exAtValues += ','
            cnt += 1
        exAtNames += '}'
        exAtValues += '}'
        response = self.dynamodb_client.update_item(
            TableName=table_name,
            Key={
                pk_name: {"S": pk_value}
            },
            UpdateExpression=upExp,
            ExpressionAttributeNames=json.loads(exAtNames),
            ExpressionAttributeValues=json.loads(exAtValues),
            ReturnValues="UPDATED_NEW"
        )
        logger.info(f"Table update Item response : {response['Attributes']}")
        return response

    def delete_table_item(self, table_name: str, pk_name: str, pk_value: str):
        """deletes the item from table using primary key of String type """
        resp = self.dynamodb_client.delete_item(
            TableName=table_name,
            Key={
                pk_name: {"S": pk_value}
            }
        )
        logger.info(f"Table Delete Item response : {resp}")
        return resp

    def table_is_empty(self, table_name):
        """ checks Table Empty or not dynamo DB refreshes this for every 6 hrs"""
        table = self.dynamodb_resource.Table(table_name)
        return table.item_count

    def get_row(self, table_name: str, key_name: str, key_value: str) -> dict:
        table = self.dynamodb_resource.Table(table_name)

        logger.debug("Calling GetItem from DynamoDB. Table=%s; key_name=%s; key_value=%s", table_name, key_name,
                     key_value)
        response = table.get_item(
            Key={
                key_name: key_value
            }
        )
        logger.debug("Returned DynamoDB response: %s", response)

        item: dict = response.get('Item')
        logger.debug("Returned item from DynamoDB: %s", item)
        return item


class ClusterDetails:

    def __init__(self, cluster_type: str,
                 application: str, client: str, project: str, account_number: str,
                 execution_id: str, target_spot_capacity: int, emr_release: str, cluster_name: str):
        self.cluster_type: str = cluster_type
        self.application: str = application
        self.client: str = client
        self.project: str = project
        self.account_number: str = account_number
        self.execution_id: str = execution_id
        self.target_spot_capacity: int = target_spot_capacity
        self.emr_release: str = emr_release
        self.cluster_name: str = cluster_name
        pass


class StepDetails:

    def __init__(self, action_on_failure: str,
                 step_type: str, command_args: list, name: str):
        self.action_on_failure: str = action_on_failure
        self.step_type: str = step_type
        self.command_args: list = command_args
        self.name: str = name
        pass


class ConfAndArguments:
    def __init__(self,
                 main_jar_path: str, dependency_jars_path: str,
                 class_name: str, file: str,
                 additional_conf: list, dynamic_allocation: bool,
                 parameters: list):
        self.main_jar_path: str = main_jar_path
        self.dependency_jars_path: str = dependency_jars_path
        self.class_name: str = class_name
        self.file: str = file
        self.parameters: list = parameters
        self.additional_conf: list = additional_conf
        self.dynamic_allocation: bool = dynamic_allocation
        pass


class CLambda:
    """ Wrapper class for AWS Lambda function calls """

    def __init__(self):
        self.lambda_client = boto3.client('lambda', config=Config(connect_timeout=30, read_timeout=300,
                                                                  retries={'max_attempts': 10}))

    def invoke(self, lambda_function_name: str, payload: dict, raise_body_error: bool = False) -> dict:
        if logger.isEnabledFor(logging.INFO):
            logger.info("calling lambda function '%s'. payload=%s", lambda_function_name, json.dumps(payload))
        response = self.lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        log_dict_debug('response: %s', response)
        status_code: int = int(response['StatusCode'])
        request_id = response['ResponseMetadata']['RequestId']
        logger.debug("response: RequestId=%s; StatusCode=%s", request_id, status_code)
        if status_code < 200 or status_code > 209:
            raise RuntimeError(f"Error when calling lambda function '{lambda_function_name}'. StatusCode={status_code}")
        result: dict = json.loads(response['Payload'].read())
        body = result['body']
        if raise_body_error:
            body_status_code: int = result['statusCode']
            if body_status_code < 200 or body_status_code > 299:
                raise RuntimeError(
                    f"Error when calling lambda function '{lambda_function_name}'. StatusCode={body_status_code}; errMsg={body}")

        if isinstance(body, str):
            body = body.strip().strip('"')
            body = f'"{body}"'
            body = json.loads(body)
        log_dict_info("response body: %s", body)
        return body

    def invoke_asynch(self, lambda_function_name: str, payload: dict, log_level: int = logging.INFO) -> None:
        if logger.isEnabledFor(log_level):
            logger.log(log_level, "calling lambda function '%s' asynchronously. payload=%s", lambda_function_name,
                       json.dumps(payload))
        response = self.lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='Event',
            Payload=json.dumps(payload)
        )

        # if logger.isEnabledFor(log_level):
        #    logger.log(log_level, 'response: %s', json.dumps(response))
        status_code: int = int(response['StatusCode'])
        request_id = response['ResponseMetadata']['RequestId']
        if logger.isEnabledFor(log_level):
            logger.log(log_level, "response: RequestId=%s; StatusCode=%s", request_id, status_code)
        if status_code < 200 or status_code > 202:
            raise RuntimeError(f"Error when calling lambda function '{lambda_function_name}'. StatusCode={status_code}")
        pass


class CStorageGateway:
    """ Wrapper class for AWS Storage Gateway """

    def __init__(self, files_share_arn: str):
        self.sg_client = boto3.client('storagegateway',
                                      config=Config(connect_timeout=30, read_timeout=300, retries={'max_attempts': 10}))
        self.file_share_arn: str = files_share_arn
        pass

    def refresh_cache_folder(self, folder: str) -> None:
        outgoing_folder: str = folder
        logger.debug("outgoing_folder to refresh: %s", outgoing_folder)
        self.sg_client.refresh_cache(FileShareARN=self.file_share_arn, FolderList=[outgoing_folder])


class CHttp:
    """ Wrapper class for HTTP operations """

    def __init__(self):
        pass

    def get(self, url: str, raise_error: bool = True) -> dict:
        logger.info("HTTP GET request:  %s", url)
        time_out: float = float(os.environ.get('HTTP_GET_TIMEOUT', '60'))
        response = None
        start_time: float = time.time()

        try:
            auth_token: str = os.environ.get('AUTH_TOKEN')
            auth: bool = to_bool(auth_token)
            if auth:
                aws4_url_signature = get_aws4_signature_header(url, "GET")
                response = requests.get(aws4_url_signature[0], headers=aws4_url_signature[1], timeout=time_out)
            else:
                response = requests.get(url, timeout=time_out)

            if logger.isEnabledFor(logging.DEBUG):
                try:
                    json_str: str = json.dumps(response)
                except(TypeError, OverflowError):
                    json_str: str = "Raw response object is not json-serializable."
                logger.debug("HTTP GET raw response: %s", json_str)

            result = response.json()

            if logger.isEnabledFor(logging.INFO):
                logger.info("HTTP GET response: %s", json.dumps(result))

            if raise_error and response.status_code != requests.codes.ok:
                if isinstance(result, str):
                    raise RuntimeError(f"Error when calling '{url}': {json.dumps(result)}")
                elif isinstance(result, dict):
                    msg: str = result.get('message')
                    if msg and ('timed out' in msg or 'time out' in msg or 'timeout' in msg or 'Timeout' in msg):
                        raise SunriseTimeoutError
                    else:
                        raise RuntimeError(f"Error when calling '{url}': {json.dumps(result)}")
        except Exception as ex:
            logger.info("HTTP GET failed: %s", str(ex))
            raise
        finally:
            if response:  response.close()
            end_time: float = time.time()
            delta: float = end_time - start_time
            logger.info("HTTP GET duration: %.3f seconds", delta)
        return result

    def post(self, url: str, payload: dict, raise_error: bool = True) -> dict:
        if logger.isEnabledFor(logging.INFO):
            logger.info("HTTP POST request:  url=%s;  payload=%s", url, json.dumps(payload))

        time_out: float = float(os.environ.get('HTTP_POST_TIMEOUT', '60'))
        response = None
        start_time: float = time.time()
        MAX_RETRY: int = int(os.environ.get('MAX_RETRY', '3'))

        try:
            paas_token: str = os.environ.get('PAAS', 'False')
            paas: bool = to_bool(paas_token)

            auth_token: str = os.environ.get('AUTH_TOKEN')
            auth: bool = to_bool(auth_token)

            for retry in range(MAX_RETRY):
                try:
                    if paas:
                        encoded_credentials = generate_PaaS_authentication()
                        paas_head = {'Authorization': encoded_credentials}
                        response = requests.post(url, json=payload, timeout=time_out, headers=paas_head)
                    elif auth:
                        aws4_url_signature = get_aws4_signature_header(url, "POST", payload)
                        response = requests.post(url, headers=aws4_url_signature[1], json=payload, timeout=time_out)
                    else:
                        response = requests.post(url, json=payload, timeout=time_out)

                    if logger.isEnabledFor(logging.DEBUG):
                        try:
                            json_str: str = json.dumps(response)
                        except(TypeError, OverflowError):
                            json_str: str = "Raw response object is not json-serializable."
                        logger.debug("HTTP POST raw response: %s", json_str)

                    result = response.json()

                    if response.status_code == requests.codes.ok:
                        return result
                    else:
                        raise Exception(f"Error When Calling '{url}' with {response.status_code} Status Code")

                except Exception as ex:
                    logger.info("HTTP POST failed: %s", str(ex))
                    if retry >= MAX_RETRY - 1:
                        logger.info("Maximum Limit Exceeded")
                        raise RuntimeError(f"Error when calling '{url}'")
                    else:
                        logger.info("HTTP Post Request Total Attempts " + str(retry + 1))

        except Exception as ex:
            logger.info("HTTP POST failed: %s", str(ex))
            raise
        finally:
            if response:  response.close()
            end_time: float = time.time()
            delta: float = end_time - start_time
            logger.info("HTTP POST duration: %.3f seconds", delta)
        return result

    def put(self, url: str, payload: dict, raise_error: bool = True) -> dict:
        if logger.isEnabledFor(logging.INFO):
            logger.info("HTTP PUT request:  url=%s;  payload=%s", url, json.dumps(payload))

        time_out: float = float(os.environ.get('HTTP_PUT_TIMEOUT', '120'))
        response = None
        start_time: float = time.time()
        paas_token: str = os.environ.get('PAAS', 'False')
        paas: bool = to_bool(paas_token)

        try:
            if paas:
                encoded_credentials = generate_PaaS_authentication()
                paas_head = {'Authorization': encoded_credentials}
                response = requests.put(url, json=payload, timeout=time_out, headers=paas_head)
            else:
                response = requests.put(url, json=payload, timeout=time_out)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("HTTP PUT raw response: %s", json.dumps(response))

            result = response.json()
            if logger.isEnabledFor(logging.INFO):
                logger.info("HTTP PUT response:  %s", json.dumps(result))

            if raise_error and response.status_code != requests.codes.ok:
                raise RuntimeError(f"Error when calling '{url}': {json.dumps(result)}")
        except Exception as ex:
            logger.info("HTTP PUT failed: %s", str(ex))
            raise
        finally:
            if response:  response.close()
            end_time: float = time.time()
            delta: float = end_time - start_time
            logger.info("HTTP PUT duration: %.3f seconds", delta)
        return result


class CEC2:
    """ Wrapper class for AWS EC2 calls """

    def __init__(self, ec2_instance_id: str):
        self.instance_id: str = ec2_instance_id

        self.stack_name: str = ""
        self.ssm_client = boto3.client('ssm', config=Config(connect_timeout=30, read_timeout=300, retries={'max_attempts': 10}))
        self.ssm_timeout = str(os.environ.get('SSM_TIMEOUT', '18000'))

    def launch_instance_if_not_exist(self, stack_name: str, template_url: str, instance_type: str, run_instance_id: str,
                                     application: str,
                                     ascend_tag: dict,
                                     core_volume: str = "100",
                                     purpose: str = "EC2 for GVAP Process") -> None:
        if self.instance_id:
            return

        cf_client = boto3.client("cloudformation",
                                 config=Config(connect_timeout=30, read_timeout=300, retries={'max_attempts': 10}))

        # manifest_variables
        client: str = get_alpha_numeric_string(ascend_tag['client'])
        account_number: str = get_alpha_numeric_string(ascend_tag['account_number'])
        project: str = get_alpha_numeric_string(ascend_tag['project'])
        map_migrated: str = str(os.environ.get('MAP_MIGRATED', 'd-server-00jvhy60km3hdk'))

        # create Cloud Formation stack
        start_time: float = time.time()
        instance_id: str = ""
        original_instance_types = instance_type
        for stack_attempt in range(int(str(os.environ.get("RETRY_CNT", 5)))):
            instance_type = random.choice(original_instance_types.split(","))
            logger.info("Creating a new ec2 instance of type %s ...", instance_type)
            try:
                response = cf_client.create_stack(
                    StackName=stack_name,
                    TemplateURL=template_url,
                    Parameters=[
                        {
                            'ParameterKey': 'pClient',
                            'ParameterValue': client
                        },
                        {
                            'ParameterKey': 'pProject',
                            'ParameterValue': project
                        },
                        {
                            'ParameterKey': 'pApplication',
                            'ParameterValue': application
                        },
                        {
                            'ParameterKey': 'pTeam',
                            'ParameterValue': 'Ascend Batch Products'
                        },
                        {
                            'ParameterKey': 'pEnvironment',
                            'ParameterValue': 'GVAP'
                        },
                        {
                            'ParameterKey': 'pPurpose',
                            'ParameterValue': purpose
                        },
                        {
                            'ParameterKey': 'pInstanceName',
                            'ParameterValue': stack_name
                        },
                        {
                            'ParameterKey': 'pInstanceType',
                            'ParameterValue': instance_type
                        },
                        {
                            'ParameterKey': 'pCoreStorageVolume',
                            'ParameterValue': core_volume
                        }
                    ],
                    Tags=[
                        {
                            'Key': 'Client',
                            'Value': client
                        },
                        {
                            'Key': 'AccountNumber',
                            'Value': account_number
                        },
                        {
                            'Key': 'Project',
                            'Value': project
                        },
                        {
                            'Key': 'Application',
                            'Value': application
                        },
                        {
                            'Key': 'ExecutionID',
                            'Value': run_instance_id
                        },
                        {
                            'Key': 'map-migrated',
                            'Value': map_migrated
                        }
                    ],
                    Capabilities=[
                        'CAPABILITY_IAM'
                    ]
                )
                waiter = cf_client.get_waiter('stack_create_complete')
                waiter.wait(
                    StackName=stack_name,
                    WaiterConfig={
                        'Delay': 60,
                        'MaxAttempts': 10
                    }
                )
                stack_resource: dict = cf_client.describe_stack_resources(StackName=response['StackId'])
                instance_id: str = stack_resource['StackResources'][0]['PhysicalResourceId']
                break
            except ClientError as ex:
                logger.info(f"Exception Occured: {traceback.print_exc}")
                logger.info(f"Deleting Stack: {stack_name}")
                try:
                    cf_client.delete_stack(StackName=stack_name)
                except:
                    pass
                interm_time: float = time.time()
                if interm_time-start_time <= int(os.environ.get("WAIT_TIME", 300)):# waiting for 300sec(5min) of time
                    time.sleep(30)
                    stack_name = stack_name + str(uuid.uuid4()).split('-')[0]
                    logger.info("Ec2 creation retry #:"+str(stack_attempt+1))
                    continue
                message = f"EC2 Creation turn around time took more than 10 minutes. So, please reach out to Dev and ensure to kill the EC2 created with name:{stack_name} in EC2 Dashboard, failure details: {ex.response['Error']['Message']}"
                raise RuntimeError(message)
        end_time: float = time.time()
        delta: float = end_time - start_time  # in seconds
        logger.info("It took %.3f seconds to create a new ec2." % delta)
        logger.info("New instance_id: %s", instance_id)
        self.instance_id = instance_id
        self.stack_name = stack_name

    def wait_until_instance_ready(self) -> None:
        if not self.instance_id:
            raise RuntimeError("Instance id is not set. Please launch instance first!")

        max_limit: float = 900.0  # Seconds.
        start_time: float = time.time()
        end_time: float = time.time()

        while (end_time - start_time) <= max_limit:
            resp = self.ssm_client.describe_instance_information(
                InstanceInformationFilterList=[
                    {
                        'key': 'InstanceIds',
                        'valueSet': [self.instance_id]
                    }
                ]
            )

            if len(resp['InstanceInformationList']) > 0 and resp['InstanceInformationList'][0][
                'PingStatus'] == 'Online':
                logger.info("It took %.3f seconds for EC2 instance to be ready.", end_time - start_time)
                return
            time.sleep(5)
            end_time = time.time()
        raise TimeoutError(f"EC2 Instance [{self.instance_id}] was not ready within {max_limit} seconds.")

    def execute_command(self, cmd: str) -> None:
        logger.info('Executing command: %s', cmd)

        # send command to EC2 instance
        ssm_client = boto3.client('ssm',
                                  config=Config(connect_timeout=30, read_timeout=300, retries={'max_attempts': 10}))
        send_command_response = ssm_client.send_command(
            InstanceIds=[self.instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={
                'commands': [cmd],
                'executionTimeout': [self.ssm_timeout]
            },
            TimeoutSeconds=int(self.ssm_timeout)
        )

        # wait for EC2 command to complete
        cmd_id = send_command_response['Command']['CommandId']
        logger.debug("Waiting for EC2 command to complete. CommandId=%s;  InstanceId=%s", cmd_id, self.instance_id)
        time.sleep(60)

        resp = ssm_client.get_command_invocation(
            CommandId=cmd_id,
            InstanceId=self.instance_id
        )
        status: str = resp['Status']
        logger.info('EC2 command status: %s', status)
        if not status in ["Success", "InProgress", "Pending"]:
            logger.info("Command %s Failed with the following Error %s", resp,
                        resp['StandardErrorContent'])
            raise Exception("Command %s Failed with the following Error %s", resp,
                            resp['StandardErrorContent'])

    #############################################
    # creating PGP EC2 from cloudformation
    #############################################

    def launch_pgp_instance_if_not_exist(self, stack_name: str, template_url: str, instance_type: str,
                                         run_instance_id: str, application: str,
                                         ascend_tag: dict, file_size: int) -> None:
        if self.instance_id:
            return

        cf_client = boto3.client("cloudformation",
                                 config=Config(connect_timeout=30, read_timeout=300, retries={'max_attempts': 10}))

        # manifest_variables
        client: str = get_alpha_numeric_string(ascend_tag['client'])
        account_number: str = get_alpha_numeric_string(ascend_tag['account_number'])
        project: str = get_alpha_numeric_string(ascend_tag['project'])
        map_migrated: str = str(os.environ.get('MAP_MIGRATED', 'd-server-00jvhy60km3hdk'))
        # create Cloud Formation stack
        logger.info("Creating a new ec2 instance of type %s ...", instance_type)
        start_time: float = time.time()
        core_size: str = str(file_size)
        instance_id: str = ""
        for stack_attempt in range(int(str(os.environ.get("RETRY_CNT", 5)))):
            try:
                response = cf_client.create_stack(
                    StackName=stack_name,
                    TemplateURL=template_url,
                    Parameters=[
                        {
                            'ParameterKey': 'pClient',
                            'ParameterValue': client
                        },
                        {
                            'ParameterKey': 'pProject',
                            'ParameterValue': project
                        },
                        {
                            'ParameterKey': 'pApplication',
                            'ParameterValue': application
                        },
                        {
                            'ParameterKey': 'pTeam',
                            'ParameterValue': 'Ascend Batch Products'
                        },
                        {
                            'ParameterKey': 'pEnvironment',
                            'ParameterValue': 'GVAP'
                        },
                        {
                            'ParameterKey': 'pPurpose',
                            'ParameterValue': 'launching ec2 instance for export to s3 applicaation'
                        },
                        {
                            'ParameterKey': 'pInstanceName',
                            'ParameterValue': stack_name
                        },
                        {
                            'ParameterKey': 'pInstanceType',
                            'ParameterValue': instance_type
                        },
                        {
                            'ParameterKey': 'pCoreStorageVolume',
                            'ParameterValue': core_size
                        }
                    ],
                    Tags=[
                        {
                            'Key': 'Client',
                            'Value': client
                        },
                        {
                            'Key': 'AccountNumber',
                            'Value': account_number
                        },
                        {
                            'Key': 'Project',
                            'Value': project
                        },
                        {
                            'Key': 'Application',
                            'Value': application
                        },
                        {
                            'Key': 'ExecutionID',
                            'Value': run_instance_id
                        },
                        {
                            'Key': 'map-migrated',
                            'Value': map_migrated
                        }
                    ],
                    Capabilities=[
                        'CAPABILITY_IAM'
                    ]
                )
                waiter = cf_client.get_waiter('stack_create_complete')
                waiter.wait(
                    StackName=stack_name,
                    WaiterConfig={
                        'Delay': 5,
                        'MaxAttempts': 200
                    }
                )
                stack_resource: dict = cf_client.describe_stack_resources(StackName=response['StackId'])
                instance_id: str = stack_resource['StackResources'][0]['PhysicalResourceId']
                break
            except ClientError as ex:
                logger.info(f"Exception Occured: {traceback.StackSummary}")
                logger.info(f"Deleting Stack: {stack_name}")
                try:
                    cf_client.delete_stack(StackName=stack_name)
                except:
                    interm_time: float = time.time()
                    if interm_time-start_time <= int(os.environ.get("WAIT_TIME", 300)):# waiting for 300sec(5min) of time
                        time.sleep(30)
                        stack_name = stack_name + str(uuid.uuid4()).split('-')[0]
                        logger.info("Ec2 creation retry #:"+str(stack_attempt+1))
                        continue
                message = f"EC2 Creation turn around time took more than 10 minutes. So, please reach out to Dev and ensure to kill the EC2 created with name:{stack_name} in EC2 Dashboard, failure details: {ex.response['Error']['Message']}"
                raise RuntimeError(message)
        end_time: float = time.time()
        delta: float = end_time - start_time  # in seconds
        logger.info("It took %.3f seconds to create a new ec2." % delta)
        logger.info("New instance_id: %s", instance_id)
        self.instance_id = instance_id
        self.stack_name = stack_name


class CEC2Fleet:
    """ Wrapper class for AWS EC2 calls """

    def __init__(self, fleet_id: str):
        if fleet_id:
            self.fleet_id: str = fleet_id
        self.ec2_instances = ''
        self.ec2_client = boto3.client('ec2',
                                       config=Config(connect_timeout=30, read_timeout=300,
                                                     retries={'max_attempts': 10}))

    def get_ec2_state(self, instance_id, waiter_action):
        waiter = self.ec2_client.get_waiter(waiter_action)
        waiter.wait(InstanceIds=[instance_id], WaiterConfig={'Delay': 60, 'MaxAttempts': 10})

    def delete_fleet_request(self, fleet_id):
        self.ec2_client.delete_fleets(FleetIds=[self.fleet_id, ],
                                      TerminateInstances=True)

    def create_fleet_request(self, params):
        s3: CS3 = CS3()
        for retry_cnt in range(params['retry_cnt']):
            bucket, bucket_key = parse_url(params['fleet_config_path'])
            config: dict = s3.get_object(bucket, bucket_key)[params['env_type']]
            config['TagSpecifications'][0]['Tags'][0]["Value"] = params.get("stack_name", config['TagSpecifications'][0]['Tags'][0][
                                                                     "Value"]) + f"{params['caller_id']+'-GVAP'}"
            config["TargetCapacitySpecification"]['TotalTargetCapacity'] = int(str(params['totalUnits']))
            try:
                if params.get("template_version"):
                    config['LaunchTemplateConfigs'][0]["LaunchTemplateSpecification"]["LaunchTemplateId"] = params.get("template_id")
                    config['LaunchTemplateConfigs'][0]["LaunchTemplateSpecification"]["Version"] = params.get("template_version")
                if params.get('fleet_request_type') != "spot":
                    config["TargetCapacitySpecification"]['OnDemandTargetCapacity'] = int(str(params['totalUnits']))
                    response = self.ec2_client.create_fleet(LaunchTemplateConfigs=config['LaunchTemplateConfigs'],
                                                            TargetCapacitySpecification=config[
                                                                'TargetCapacitySpecification'],
                                                            Type=config['Type'],
                                                            OnDemandOptions=config['OnDemandOptions'],
                                                            TagSpecifications=config['TagSpecifications'],
                                                            )
                else:
                    response = self.ec2_client.create_fleet(LaunchTemplateConfigs=config['LaunchTemplateConfigs'],
                                                            TargetCapacitySpecification=config[
                                                                'TargetCapacitySpecification'],
                                                            Type=config['Type'],
                                                            TagSpecifications=config['TagSpecifications'],
                                                            )
                self.fleet_id = response['FleetId']
                logger.info(f"Created Fleet Id: {self.fleet_id}")
                if params.get('fleet_request_type') != "spot":
                    if response['Errors']:
                        logger.info(f"Fleet Error {response['Errors'][0]['ErrorMessage']}")
                        raise RuntimeError(f"Fleet Error {response['Errors'][0]['ErrorMessage']}")
                instance_properties: list = list()
                if response['FleetId'] and response['Instances']:
                    for each_instanceId in list(response['Instances'][0]['InstanceIds']):
                        ec2_obj = boto3.resource('ec2', region_name='us-east-1',
                                                 config=Config(connect_timeout=30, read_timeout=300,
                                                               retries={'max_attempts': 10}))
                        instance = ec2_obj.Instance(each_instanceId)
                        self.get_ec2_state(each_instanceId, 'instance_status_ok')
                        instance.reload()
                        CoresCount = instance.cpu_options.get('CoreCount') * instance.cpu_options.get(
                            'ThreadsPerCore')
                        logger.info(f"Created Instance Id: {each_instanceId}, Created Cpu Cores {CoresCount}")
                        instance_properties.append(each_instanceId + '|' + str(CoresCount))
                        self.ec2_instances = instance_properties
                break
            except RuntimeError as ex:
                logger.info(f"Deleting the fleet Id: {self.fleet_id} as there is fleet failure")
                self.ec2_client.delete_fleets(FleetIds=[self.fleet_id],
                                              TerminateInstances=True)
                if params['retry_cnt'] != retry_cnt + 1:
                    time.sleep(30)
                    continue
                raise RuntimeError(f"Fleet Error while creating the fleet EC2 {traceback.StackSummary}")


class SparkArguments:

    def __init__(self, executor_cores: int, executor_memory: int, executor_instances: int, parallelism: int,
                 shuffle_partitions: int, executor_memory_overhead: int,
                 executor_failures: int, max_result_size: int, min_num_executors: int, max_num_executors: int,
                 conf_arguments: ConfAndArguments, driver_memory_overhead: int = 2, deploy_mode: str = 'cluster'):
        self.executor_cores: int = executor_cores
        self.executor_memory: int = executor_memory
        self.executor_instances: int = executor_instances
        self.parallelism: int = parallelism
        self.shuffle_partitions: int = shuffle_partitions
        self.executor_memory_overhead: int = executor_memory_overhead
        self.driver_memory_overhead: int = driver_memory_overhead
        self.deploy_mode: str = deploy_mode
        self.executor_failures: int = executor_failures
        self.max_result_size = max_result_size
        self.min_num_executors = min_num_executors
        self.max_num_executors = max_num_executors
        self.conf_arguments = conf_arguments
        pass


###############################################################################
# Exceptions
###############################################################################

class SunriseTestError(Exception):
    pass


class SunriseTimeoutError(Exception):
    pass


###############################################################################
# Security
###############################################################################

def get_aws4_signature_header(url: str, method: str, payload: dict = {}) -> list:
    """ Function for signing GET and POST requests AWS Version 4 """
    sts = boto3.client('sts', config=Config(connect_timeout=30, read_timeout=300, retries={'max_attempts': 10}))
    role_arn: str = os.environ['ROLE_ARN']

    # ************* REQUEST VALUES *************
    # method = 'GET'
    # service = 'execute-api'
    # host = '5qnfrk52ec.execute-api.us-east-1.amazonaws.com'
    # region = 'us-east-1'
    # endpoint = 'https://5qnfrk52ec.execute-api.us-east-1.amazonaws.com/prod/models/debt_to_income_insight/2020-02-10/false'
    # request_parameters = 'application_name=GVAP&client_name=Sophina%20Quach%28TEST%29'
    # shekars example

    # ************* REQUEST VALUES RECIEVED  *************

    parsed_url = urllib.parse.urlparse(url)
    host = parsed_url.netloc
    path = parsed_url.path

    if method == 'GET':
        # get query string
        qstring = parsed_url.query
        # parse query string into list of (k,v)
        query_params = urllib.parse.parse_qsl(qstring)
        # sort list to sort query parameters by name
        query_params.sort()
        # form query string sorted
        qstring_sorted = urllib.parse.urlencode(query_params, quote_via=urllib.parse.quote)
        endpoint = urllib.parse.urlunparse(
            [parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', qstring_sorted, ''])
        canonical_querystring = qstring_sorted
        payload_hash = hashlib.sha256(('').encode('utf-8')).hexdigest()

    elif method == 'POST':
        # incoming payload is a dictionary convert to string for generating signature
        payload = json.dumps(payload)
        endpoint = url
        canonical_querystring = ''
        payload_hash = hashlib.sha256(payload.encode('utf-8')).hexdigest()

    region = 'us-east-1'
    service = 'execute-api'

    def sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    def getSignatureKey(key, dateStamp, regionName, serviceName):
        kDate = sign(('AWS4' + key).encode('utf-8'), dateStamp)
        kRegion = sign(kDate, regionName)
        kService = sign(kRegion, serviceName)
        kSigning = sign(kService, 'aws4_request')
        return kSigning

    temp = sts.assume_role(RoleArn=role_arn, RoleSessionName='APIGW')
    access_key = temp['Credentials']['AccessKeyId']

    secret_key = temp['Credentials']['SecretAccessKey']

    session_token = temp['Credentials']['SessionToken']

    if access_key is None or secret_key is None:
        logger.info("NO ACCESS KEY IS AVAILABLE")
        raise

    t = datetime.datetime.utcnow()
    amzdate = t.strftime('%Y%m%dT%H%M%SZ')
    datestamp = t.strftime('%Y%m%d')  # Date w/o time, used in credential scope

    canonical_uri = path

    canonical_headers = 'host:' + host + '\n' + 'x-amz-date:' + \
                        amzdate + '\n'
    signed_headers = 'host;x-amz-date'

    canonical_request = method + '\n' + canonical_uri + '\n' + canonical_querystring + \
                        '\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash

    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = datestamp + '/' + region + \
                       '/' + service + '/' + 'aws4_request'
    string_to_sign = algorithm + '\n' + amzdate + '\n' + credential_scope + \
                     '\n' + hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()

    signing_key = getSignatureKey(secret_key, datestamp, region, service)

    signature = hmac.new(signing_key, (string_to_sign).encode(
        'utf-8'), hashlib.sha256).hexdigest()

    authorization_header = algorithm + ' ' + 'Credential=' + access_key + '/' + \
                           credential_scope + ', ' + 'SignedHeaders=' + \
                           signed_headers + ', ' + 'Signature=' + signature

    headers = {'x-amz-date': amzdate, 'Authorization': authorization_header,
               'X-Amz-Security-Token': session_token}

    return [endpoint, headers]


###############################################################################
# Logger
###############################################################################

def __init_logger(request_id: str, caller_id: str, log_level: str = "INFO", log_methods: str = "CONSOLE",
                  cloud_watch_prefix: str = None, app_name: str="") -> Logger:
    """
        LOG_LEVEL  = CRITICAL/ERROR/WARNING/INFO/DEBUG
        LOG_METHOD = CONSOLE/FILE/CLOUD_WATCH
            CONSOLE - mainly used for Lambda Functions
            FILE or CLOUD_WATCH - mainly used for applications running in EC2 instances.
    """
    # validation
    if log_level not in {'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'}:
        log_level: str = "INFO"

    log_method_set: set = set()
    log_method_list: list = log_methods.split(",")
    for lm in log_method_list:
        if lm not in {'CONSOLE', 'FILE', 'CLOUD_WATCH'}:
            lm = 'CONSOLE'
        log_method_set.add(lm)

    # configure settings
    if app_name:
        app_name: str = app_name
    else:
        app_name: str = sys.argv[0].split('/')[-1].split('.')[0]
    # print('app_name:', app_name)

    logging.basicConfig()
    root_logger: Logger = logging.getLogger()
    root_logger.setLevel(logging.ERROR)  # reset root logger's log level

    sub_logger: Logger = logging.getLogger('com.experian.cis.sunrise')
    sub_logger.propagate = 0
    sub_logger.setLevel(log_level)

    while sub_logger.hasHandlers():
        sub_logger.removeHandler(sub_logger.handlers[0])

    for lm in log_method_set:
        log_method: str = lm.strip()

        if 'CONSOLE' == log_method:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter('[LOGGING] %(message)s'))
            # handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
        elif 'FILE' == log_method:
            log_file_name: str = f"{app_name}.log"
            handler = RotatingFileHandler(log_file_name, maxBytes=8_388_608, backupCount=1)
            handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
        elif 'CLOUD_WATCH' == log_method:
            cw_prefix: str = cloud_watch_prefix.strip() if cloud_watch_prefix else ''
            log_group: str = posixpath.join(cw_prefix, app_name)

            timestamp: str = str(datetime.datetime.now(datetime.timezone.utc))
            timestamp: str = timestamp.split('+')[0].replace('-', '').replace(':', '').replace('.', '_').replace(' ',
                                                                                                                 '_')
            timestamp: str = f"{timestamp}#{request_id}"
            log_stream: str = f"{caller_id.strip()}#{request_id}" if caller_id and caller_id.strip() else timestamp

            handler = watchtower.CloudWatchLogHandler(log_group=log_group, stream_name=log_stream)
        else:
            raise RuntimeError(f"Log method [{log_method}] is not implemented yet.")

        sub_logger.addHandler(handler)

    # print('log_level:', sub_logger.level)
    # print('log_handlers:', sub_logger.handlers)
    return sub_logger


def initialize_logger() -> Logger:
    """
        Default logger
        mainly for Lambda Functions
    """
    log_level: str = os.environ.get("LOG_LEVEL", "ERROR")
    log_methods: str = os.environ.get("LOG_METHODS", "CONSOLE")
    caller_id: str = os.environ.get("CALLER_ID")
    cw_prefix: str = os.environ.get("CLOUDWATCH_PREFIX")
    app_name: str = os.environ.get("APP_NAME", "")
    request_id: str = str(uuid.uuid4()).replace('-', '')
    return __init_logger(request_id, caller_id, log_level, log_methods, cw_prefix,app_name)


def reset_logger_for_ec2(caller_id: str, request_id: str = None, log_level: str = "INFO",
                         log_methods: str = "CONSOLE,CLOUD_WATCH",
                         cloud_watch_prefix: str = "/aws/ec2", app_name: str="") -> Logger:
    """ mainly for applications running in EC2 instances """
    if not request_id:
        request_id: str = str(uuid.uuid4()).replace('-', '')
    return __init_logger(request_id, caller_id, log_level, log_methods, cloud_watch_prefix, app_name)


def log_msg_info(msg: str):
    if logger.isEnabledFor(logging.INFO):
        logger.info(msg)


def log_msg_debug(msg: str):
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(msg)


def log_dict_info(fmt: str, content: dict):
    if logger.isEnabledFor(logging.INFO):
        logger.info(fmt, json.dumps(content))


def log_dict_debug(fmt: str, content: dict):
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(fmt, json.dumps(content))


###############################################################################
# EC2 application related
###############################################################################

def set_ec2_command_arguments(caller_id: str, notify_status_location: str, ec2_file_path: str,
                              shut_down_instance: bool) -> str:
    cmd_args: str = \
        f" --caller_id {caller_id}" + \
        f" --notify_status_location  {notify_status_location}" + \
        f" --ec2_file_path {ec2_file_path}" + \
        f" --ec2_app_log_level {'INFO'}" + \
        f" --ec2_app_log_methods {'CONSOLE,CLOUD_WATCH'}" + \
        f" --s3_sse {S3_SSE_KMS}" + \
        f" --shut_down {shut_down_instance}"
    return cmd_args


def get_ec2_command_arguments() -> Namespace:
    """
        The input arguments have already been validated in ec2 driver. So it is safe.
    """

    # parse and validate input arguments
    parser: ArgumentParser = ArgumentParser()

    parser.add_argument('--request_id', type=str, required=True)
    parser.add_argument('--caller_id', type=str, required=True)
    parser.add_argument('--notify_status_location', type=str, required=True)
    parser.add_argument('--ec2_file_path', type=str, required=True)
    parser.add_argument('--ec2_app_log_level', type=str, required=True)
    parser.add_argument('--ec2_app_log_methods', type=str, required=True)
    parser.add_argument('--s3_sse', type=str, default=S3_SSE_KMS, required=False)
    parser.add_argument('--shut_down', default=False, type=lambda x: (str(x).lower() == 'true'), required=True)

    args: Namespace = parser.parse_args()
    return args


###############################################################################
# Common functions
###############################################################################

def parse_url(url: str) -> (str, str):
    # allow_fragments=False is for allowing special characters like  #, @, or : in the url
    url_obj = urllib.parse.urlparse(url, allow_fragments=False)
    bucket: str = url_obj.netloc
    objkey: str = url_obj.path.lstrip('/')
    return bucket.strip(), objkey.strip()


def join_path(s: str, *argv) -> str:
    """ Handle the '/' char at the end of the path. """
    ss = s.strip()
    for arg in argv:
        if not ss.endswith('/'):  ss += '/'
        ss += str(arg).strip()
    return ss


def sha256_hash_code(base_date: str) -> str:
    period: str = base_date.replace('-', '') + '0000'
    hash_code: str = hashlib.sha256(str.encode(period + '\n')).hexdigest()[:6]
    return hash_code


def parse_date(date: str) -> (str, str, str):
    """
        date in in format like 2013-07-31
        return (year, month, day)
    """
    arr: list = date.split('-')
    if len(arr) != 3:
        raise RuntimeError("date should be in format like: 2013-07-31")
    return arr[0], arr[1], arr[2]


def to_bool(val: str) -> bool:
    return val and val.strip().lower() == 'true'


def get_alpha_numeric_string(s: str) -> str:
    if s.isalnum():
        return s
    chars: list = [c for c in s if c.isalnum()]
    return "".join(chars)


def generate_PaaS_authentication():
    access_key = os.environ['AWS_ACCESS_KEY_ID']
    secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
    session_token = os.environ['AWS_SESSION_TOKEN']

    # format string for PaaS API specification
    unencoded_temp_creds = "aws_access_key_id={0}\n" \
                           "aws_secret_access_key={1}\n" \
                           "aws_session_token={2}".format(access_key, secret_key, session_token)
    # base64 encoding temp creds for PaaS API Specification
    encoded_temp_creds = unencoded_temp_creds.encode('UTF-8')
    encoded_bytes = base64.b64encode(encoded_temp_creds)
    encoded_credentials = encoded_bytes.decode('UTF-8')
    return encoded_credentials


def save_config_file(s3: CS3, config: dict, config_file_location: str):
    caller_id: str = config['caller_id']
    config_file_path: str = join_path(config_file_location, caller_id) + ".json"
    content: str = json.dumps(config, indent=4)
    s3_resource = boto3.resource('s3')
    config_bucket, config_key = parse_url(config_file_path)
    obj = s3_resource.Object(config_bucket, config_key)
    kms_id = get_kms_key_id()
    obj.put(Body=str.encode(content), ServerSideEncryption='aws:kms', SSEKMSKeyId=kms_id,
            ACL="bucket-owner-full-control")
    return config_file_path


def check_emr_launch(event: dict):
    control_params: dict = event.get('control_params')
    if not control_params:
        return True
    launch_emr: bool = control_params.get('launch_emr_cluster', True)
    return launch_emr


def parse_cluster_name(cluster_name: str) -> (bool, str, int, str, str):
    arr = cluster_name.split('-')
    if len(arr) < 4 or not arr[1].isdigit():
        return False, '', 0, '', ''
    instance_type: str = arr[0]
    num_nodes: int = int(arr[1])
    run_instance_id: str = arr[2]
    feature: str = arr[3]
    return True, instance_type, num_nodes, run_instance_id, feature


def get_run_instance_id(caller_id: str) -> str:
    if caller_id.isalnum():
        return caller_id
    chars: list = [c for c in caller_id if c.isalnum()]
    return "".join(chars)


def get_size(bucket, path):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    total_size = 0
    bz_flag: bool = False
    for obj in my_bucket.objects.filter(Prefix=path):
        if ".bz2" in obj.key or ".gz" in obj.key:
            bz_flag = True
        total_size = total_size + obj.size
    return total_size, bz_flag


def get_cluster_name(args: dict) -> str:
    request_id: str = str(uuid.uuid4()).split("-")[0]
    emr_cluster_name_keyword: str = os.environ['EMR_CLUSTER_NAME_KEYWORD']
    cluster_name: str = f"{args['application']}_{args['execution_id']}_{emr_cluster_name_keyword}_{request_id}"
    return cluster_name


def prepare_spark_submit_step(spark_args: SparkArguments):
    logger.info("Spark Arguments : %s", spark_args)
    spark_command_arg_list: list = ["spark-submit",
                                    "--master", "yarn",
                                    "--deploy-mode", spark_args.deploy_mode,
                                    "--class", spark_args.conf_arguments.class_name
                                    ]
    if spark_args.conf_arguments.file != "":
        spark_command_arg_list += ["--files", spark_args.conf_arguments.file]

    if len(spark_args.conf_arguments.additional_conf) > 0:
        spark_command_arg_list += spark_args.conf_arguments.additional_conf

    if spark_args.conf_arguments.dynamic_allocation:
        spark_command_arg_list += ["--conf", "spark.dynamicAllocation.enabled=true",
                                   "--conf", "spark.shuffle.service.enabled=true",
                                   "--conf", "spark.dynamicAllocation.executorIdleTimeout=60s",
                                   "--conf", "spark.dynamicAllocation.cachedExecutorIdleTimeout=60s",
                                   "--conf", f"spark.dynamicAllocation.initialExecutors={spark_args.min_num_executors}",
                                   "--conf", f"spark.dynamicAllocation.minExecutors={spark_args.min_num_executors}",
                                   "--conf", f"spark.dynamicAllocation.maxExecutors={spark_args.max_num_executors}"]
    else:
        spark_command_arg_list += ["--conf", "spark.shuffle.service.enabled=true",
                                   "--conf", "spark.dynamicAllocation.enabled=false"]

    spark_command_arg_list += ["--conf", f"spark.executor.cores={spark_args.executor_cores}",
                               "--conf", f"spark.executor.memory={spark_args.executor_memory}g",
                               "--conf", f"spark.executor.instances={spark_args.executor_instances}",
                               "--conf", f"spark.default.parallelism={spark_args.parallelism}",
                               "--conf", f"spark.sql.shuffle.partitions={spark_args.shuffle_partitions}",
                               "--conf", f"spark.driver.maxResultSize={spark_args.max_result_size}g",
                               "--conf", f"spark.executor.memoryOverhead={spark_args.executor_memory_overhead}g",
                               "--conf", f"spark.yarn.max.executor.failures={spark_args.executor_failures}",
                               "--conf", f"spark.driver.cores={spark_args.executor_cores}",
                               "--conf", f"spark.driver.memory={spark_args.executor_memory}g",
                               "--conf", f"spark.driver.memoryOverhead={spark_args.driver_memory_overhead}g",
                               "--conf", "spark.network.timeout=10000000",
                               "--conf", "spark.sql.autoBroadcastJoinThreshold=104857600",
                               "--conf", "spark.task.maxfailures=10"
                               ]

    if spark_args.conf_arguments.dependency_jars_path is not None and spark_args.conf_arguments.dependency_jars_path != "":
        spark_command_arg_list += ["--jars", spark_args.conf_arguments.dependency_jars_path]

    if spark_args.conf_arguments.main_jar_path is None or spark_args.conf_arguments.main_jar_path == "":
        raise RuntimeError("Main jar path is not provided")

    spark_command_arg_list += [spark_args.conf_arguments.main_jar_path]

    if len(spark_args.conf_arguments.parameters) > 0:
        spark_command_arg_list += spark_args.conf_arguments.parameters

    return spark_command_arg_list


########################
# generating the kms id
########################
def get_kms_key_id():
    kms_client = boto3.client('kms', config=Config(connect_timeout=30, read_timeout=300, retries={'max_attempts': 10}))
    kms_key_id = ''
    key_aliases = kms_client.list_aliases(Limit=100)
    for i in key_aliases['Aliases']:
        if 'gvsu_cmk' in i['AliasName']:
            kms_key_id = i['TargetKeyId']
    return kms_key_id


# file size human readable
def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


###########################################################
# This is for checking the storage class of s3 file
# #########################################################
def storage_class_check(s3: CS3, s3_url: str):
    flg_check: bool = True
    try:
        src_bucket, src_folder_key = parse_url(s3_url)
        if s3.s3fs.isfile(s3_url):
            obj_resp = s3.get_head_object(src_bucket, src_folder_key)
            logger.info("Storage class Response: %s", obj_resp.get('StorageClass', 'STANDARD'))
            if obj_resp.get('StorageClass', 'STANDARD') and obj_resp.get('StorageClass', 'STANDARD') != "STANDARD":
                flg_check = False
        else:
            src_file: dict = next(s3.list_files(src_bucket, src_folder_key))
            if src_file["StorageClass"] != "STANDARD":
                flg_check = False
        return flg_check

    except:
        raise SunriseTestError(
            f"There is problem while checking the s3 storage type checking or source not exists {traceback.format_exc()}")


def get_core_volume_size(core_volume: float):
    core_volume = math.ceil(core_volume)
    if core_volume <= 500:
        core_volume = '500'
    elif 500 < core_volume <= 750:
        core_volume = '750'
    elif 750 < core_volume <= 1000:
        core_volume = '1000'
    elif 1000 < core_volume <= 1250:
        core_volume = '1250'
    elif 1250 < core_volume <= 1500:
        core_volume = '2000'
    elif 1500 < core_volume <= 1750:
        core_volume = '2250'
    elif 1750 < core_volume <= 2000:
        core_volume = '2500'
    elif 2000 < core_volume <= 2250:
        core_volume = '2750'
    elif 2250 < core_volume <= 2500:
        core_volume = '3250'
    elif 2500 < core_volume <= 2750:
        core_volume = '3500'
    elif 2750 < core_volume <= 3000:
        core_volume = '4000'
    else:
        core_volume = '5000'
    return core_volume


def prepare_stamp_success_file_content(content_list_dict: list):
    json_list_dict = list()
    if content_list_dict:
        for each_source in content_list_dict:
            file_size = 0
            if each_source.get('source_type',"") == 'STS':
                file_size = each_source.get('file_size', 0)
            else:
                bucket, bucket_key = parse_url(each_source['deliver_landing_name'])
                size = get_size(bucket, bucket_key)[0]
                file_size = sizeof_fmt(size)

            json_list_dict.append({"deliver_landing_name": each_source['deliver_landing_name'],
                                   "file_size": file_size,
                                   "partitions_count": each_source['partitions_count'],
                                   "records_count": each_source['records_count'],
                                   "success_file_path": each_source['deliver_landing_name']+'.success'})

    return json.dumps(json_list_dict)


def write_failed_files(manifest_obj: dict, err_msg: str):
    s3: CS3 = CS3()
    output_url: str = manifest_obj['notify_status_location']
    caller_id: str = manifest_obj['caller_id']
    failed_path: str = join_path(output_url, caller_id) + ".failed"
    s3.put_object_by_url(failed_path, err_msg)
    logger.info("Error message has been saved to S3 path '%s'", failed_path)
    pass


# convert bytes to gb
def convert_bytes_2_gb(bytes_size: int):
    gbInBytes = 1024 * 1024 * 1024
    sizeinGB = round((bytes_size / gbInBytes), 2)
    return sizeinGB

##################################
# get emr status from aes payload
##################################
def get_emr_status(aes: dict):
    cluster_id: str = aes['resources'][0]['id']
    client = boto3.client('emr', 'us-east-1')
    response = client.describe_cluster(ClusterId=cluster_id)
    status = response['Cluster']['Status']['State']
    return status

##################################
# generating ascend status payload
##################################
def create_aes_payload(manifest: dict, env: str, app_type: str, current_step: str, notes: str) -> dict:
    payload = dict()
    instance_id, task_id, process_id = manifest['caller_id'].split("_")
    payload['executionId'] = manifest['caller_id']
    payload['runInstanceId'] = instance_id
    payload['environmentId'] = env
    payload['serviceId'] = app_type
    payload['currentStep'] = current_step
    payload['accountNumber'] = manifest['ascend_tag']['account_number']
    payload['clientId'] = manifest['ascend_tag']['client']
    payload['projectId'] = manifest['ascend_tag']['project']
    payload['processBoxId'] = process_id
    payload['taskId'] = task_id
    payload['notes'] = notes
    payload['startTs'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    payload['status'] = "EXECUTING"
    payload['recordCount'] = 0
    payload['resources'] = [{"type": "", "id": "", "permanent": ""}]
    return payload

def process_restartability(request: dict, app_type: str, current_step: str, notes: str):
    try:
        api_client = restartability.CRestApiClient()
        find_aes_response = api_client.find_aes(request['caller_id'])
        if find_aes_response is not None:
            updated_aes = find_aes_response
            if not updated_aes['resources'][0]['permanent']:
                status = get_emr_status(updated_aes)
                if status.upper() != 'TERMINATED':
                    logger.info('EMR is already running with id %s', updated_aes['resources'][0]['id'])
                    return False
            updated_aes['updateTs'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
            update_aes_response = api_client.update_aes(updated_aes)
            logger.info('update_aes_response %s', update_aes_response)
        else:
            env = os.environ['ENVIRONMENT_ID'].strip()
            aes = create_aes_payload(request, env, app_type, current_step, notes)
            create_aes_response = api_client.create_aes(aes)
            logger.info('create_aes_response %s', create_aes_response)
    except Exception as ex:
        err_msg: str = f"Unable to process restartability :\n{traceback.format_exc()}"
        raise Exception(err_msg)
    return True



###############################################################################
# Global variables
###############################################################################

logger: Logger = initialize_logger()