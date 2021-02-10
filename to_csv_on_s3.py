from io import StringIO
import boto3

def to_csv_on_s3(dataframe, bucketName, fileName, index=False):
    """ 
    Write a dataframe to a CSV on S3
    Example:
    filename = "raw_data/test.csv"
    bucketName = "jpx-future-bucket"
    """
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=index)
    client = boto3.client('s3')
    response = client.put_object(
        ACL = 'private',
        Body = csv_buffer.getvalue(),
        Bucket=bucketName,
        Key=fileName
    )