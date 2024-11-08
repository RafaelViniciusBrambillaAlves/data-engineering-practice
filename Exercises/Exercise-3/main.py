import boto3
import time
import io
import gzip


def download_gz_file(bucket_name, key):
    # s3_client = boto3.client('s3', aws_access_key_id='fake-access-key', aws_secret_access_key='fake-secret-key')
    s3_client = boto3.client('s3', region_name='us-east-1')

    gz_buffer = io.BytesIO()

    s3_client.download_fileobj(bucket_name, key, gz_buffer)
    gz_buffer.seek(0)

    with gzip.open(gz_buffer, 'rt') as f:
        content = f.read()

    return content

def main():
    bucket_name = "commoncrawl"
    key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    file_content = download_gz_file(bucket_name, key)
    print(file_content[:1000])
    print('boaaaaaaaaaaaa')

    time.sleep(100)



if __name__ == "__main__":
    main()
