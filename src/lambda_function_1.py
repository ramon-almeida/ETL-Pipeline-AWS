import pandas as pd
import boto3, json, logging, re, os
from datetime import datetime, timedelta
import pytz


first_trigger_bucket = os.environ['FirstTriggerBucket']
second_trigger_bucket= os.environ['SecondTriggerBucket']

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(f'Event structure: {event}')

    #initialising vars and calling s3 bucket
    s3 = boto3.client('s3')

    #Deadline to consider recent
    # days_ago = 7
    timezone = pytz.timezone('UTC')
    limit_time = datetime.now(timezone)# - timedelta(days=days_ago)
    
    # list to store the results
    source_files = []


    # Set up the pagination variables for source bucket
    source_paginator = s3.get_paginator('list_objects_v2')
    source_page_iterator = source_paginator.paginate(Bucket=first_trigger_bucket, Prefix='')

    # Iterate through the source bucket pages and add the contents to the list
    for page in source_page_iterator:
        for obj in page['Contents']:
            if obj['Key'].endswith('.csv'):
                if obj['LastModified'].date() == limit_time.date():
                    source_files.append(obj['Key'])

        
    
    for element in source_files:
        print(element)
        
        res = re.findall(('[^/]+\.csv'), element)
        s3.download_file(first_trigger_bucket, element, "/tmp/" + res[0])
        
        df=pd.DataFrame(pd.read_csv("/tmp/" + res[0], names=["order_time", "store_name", "customer_name", "items", "total_price", "payment_type", "card_number"]))
        df = df.drop(columns = ['customer_name', 'card_number'])
        df['order_time'] = pd.to_datetime(df['order_time'], format="%d/%m/%Y %H:%M")
        
        df.to_csv('/tmp/'+res[0], header=False, index=False) # save transformed dataFrame to the same file name in /tmp/
        s3.upload_file('/tmp/'+ res[0], second_trigger_bucket, element)
        
