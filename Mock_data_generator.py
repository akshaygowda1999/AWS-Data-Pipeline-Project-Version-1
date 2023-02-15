import boto3
import pandas as pd
from io import BytesIO
import json
import random as r 


s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print(event)
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]
        print(bucket_name)
        print(s3_file_name)
        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
        print(resp['Body'])
        global df #making the variable globle 
        global df3
        df = pd.read_csv(resp['Body'], sep=',')
        df3 = df.sample(frac = 1) #shuffling data
        print('after split',df3.shape)
        print(df3.head(2))
     
    except Exception as err:
        print(err)
    
    return {
        # 'statusCode': 200,
        'body': json.loads(json.dumps(list((df3.sample(frac=0.25, random_state= r.randint(1,100)).reset_index(drop=True)).T.to_dict().values()))) #json.dumps(json.loads(( df_s3_data.to_json(orient="records"))))
        
       
    }