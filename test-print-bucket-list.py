from minio import Minio

client = Minio("datalake.website:9000",
               access_key='tester-1',
               secret_key='testerpass',
               secure=False)

buckets = client.list_buckets()
print(buckets)
