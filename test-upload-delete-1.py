from minio import Minio
import time
import os

BUCKET_NAME = 'test-bucket-1'
LOCAL_FILE_PATH = './data/data/sdd-lacmus-version.zip'
S3_FILE_NAME = 'sdd-lacmus-version.zip'
TMP_FILE = './data/tmp/tmp.zip'

client = Minio("datalake.website:9000",
               access_key='tester-1',
               secret_key='testerpass',
               secure=False)

found = client.bucket_exists(BUCKET_NAME)

if not found:
    client.make_bucket(BUCKET_NAME)
else:
    print(f'{BUCKET_NAME} already exists.')

file_size = os.path.getsize(LOCAL_FILE_PATH)
print(f'File size = {file_size // 1024} kB')
print('Start uploading')
t0 = time.time()
# -----------------
client.fput_object(BUCKET_NAME, S3_FILE_NAME, LOCAL_FILE_PATH)
# -----------------
t1 = time.time()
print('Upload success, start reading')
# -----------------
client.fget_object(BUCKET_NAME, S3_FILE_NAME, TMP_FILE)
# -----------------
t2 = time.time()
print('Reading success, start deleting')
# -----------------
client.remove_object(BUCKET_NAME, S3_FILE_NAME)
# -----------------
t3 = time.time()
# -----------------
print(f'Upload time = {t1 - t0}')
print(f'Read time =   {t2 - t1}')
print(f'Remove time = {t3 - t2}')
os.remove(TMP_FILE)

# Upload time = 197 s
# Read time =   274 s
# Remove time = 0.026 s
