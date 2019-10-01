import sys
import boto3


def main(s3_bucket: str):
    if s3_bucket is None or len(s3_bucket) == 0:
        raise Exception("Invalid s3_bucket")

    for key in generate_matching_s3_keys(s3_bucket):
        print(key)


def generate_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """

    s3 = boto3.client('s3')
    print("\n\n")

    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


if __name__ == "__main__":
    if len(sys.argv) != 2:
        message = "Invalid number of command line arguments supplied." \
                  "\nUsage:  " \
                  "\npython3 aws_s3_util.py <S3_bucket>"
        raise Exception(message)

    print(sys.argv[1:])
    s3_bucket = sys.argv[1]
    print("\n S3 bucket: ", s3_bucket)

    main(s3_bucket)
