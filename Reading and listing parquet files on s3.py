import boto3
import pandas as pd
import io
from IPython.display import display

bucketName = 'deneme13'
client = boto3.client('s3')
dfs = []

response = client.list_objects_v2(Bucket=bucketName)

if 'Contents' in response:
    file_names = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    print(file_names)
    for filename in file_names:
        buffer = io.BytesIO()
        response = client.get_object(Bucket=bucketName, Key=filename)
        buffer.write(response['Body'].read())
        buffer.seek(0)
        df = pd.read_parquet(buffer)
        dfs.append(df)
    combined_df = pd.concat(dfs)
    display(combined_df)
    
    
        
else:
    print("S3 bucketında Parquet dosyası bulunamadı.")
    