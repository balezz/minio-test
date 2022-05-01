# Test case 1: measure time for upload and read one file with variable size
import pickle

from minio import Minio
import time
import os
import logging
from datetime import datetime

BUCKET_NAME = 'test-bucket-1'
S3_FILE_NAME = 'test_file.zip'
TMP_FILE = './data/tmp/tmp.zip'
LOG_FILE = './logs/update-delete-1.log'

logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG)


def make_bucket(client):
    found = client.bucket_exists(BUCKET_NAME)
    if not found:
        client.make_bucket(BUCKET_NAME)
    else:
        logging.info(f'{BUCKET_NAME} already exists.')


def make_file(name, size):
    with open(name, 'wb') as f:
        rand_bytes = os.urandom(size)
        f.write(rand_bytes)


def upload_read(file_name):
    logging.info('-'*80)
    logging.info(datetime.now())
    file_size = os.path.getsize(file_name)
    logging.info(f'File size = {file_size // (2**20)} MB')

    t = time.time()
    client = Minio("datalake.website:9000",
                   access_key='tester-1',
                   secret_key='testerpass',
                   secure=False)
    buckets = client.list_buckets()
    t0 = time.time()
    logging.info(f'Buckets: {[b.name for b in buckets]}')
    logging.info(f'Connection time={(t0 - t):.3f} s. Start uploading')
    # -----------------
    client.fput_object(BUCKET_NAME, S3_FILE_NAME, file_name)
    # -----------------
    t1 = time.time()
    upload_time = t1 - t0
    logging.info(f'Upload time = {upload_time:.3f} s, start reading')
    # -----------------
    client.fget_object(BUCKET_NAME, S3_FILE_NAME, TMP_FILE)
    # -----------------
    t2 = time.time()
    reading_time = t2 - t1
    logging.info(f'Reading time = {reading_time:.3f} start deleting')
    # -----------------
    client.remove_object(BUCKET_NAME, S3_FILE_NAME)
    # -----------------
    t3 = time.time()
    remove_time = t3 - t2
    # -----------------
    logging.info(f'Remove time = {remove_time:.3f} s')
    os.remove(TMP_FILE)
    return upload_time, reading_time, remove_time


if __name__ == '__main__':
    result = {}
    for n in range(14):
        size = 2**(n+20)
        test_file_name = f'data/{2**n}M.zip'
        make_file(test_file_name, size)
        result[size] = upload_read(test_file_name)

    with open('logs/result.pickle', 'wb') as handle:
        pickle.dump(result, handle, protocol=pickle.HIGHEST_PROTOCOL)
    logging.info('Test passed!')
