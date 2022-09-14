
import boto3
import datetime

#s#_client = boto3.client('s3')

def verify_email_identity():
    ses_client = boto3.client("ses", region_name="us-west-2")
    response = ses_client.verify_email_identity(
        EmailAddress="jbirchler@usgs.gov"
    )
    print(response)

def send_email_alert(Data):
    ses_client = boto3.client("ses", region_name="us-west-2")
    CHARSET = "UTF-8"

    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                "jbirchler@usgs.gov",
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": Data,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Isla Verde Email Alert",
            },
        },
        Source="eswanson@contractor.usgs.gov"
        )



def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket("cmgp-coastcam")
    prefix = 'cameras/islaverde/products'
    
    time_check = datetime.datetime.now() - datetime.timedelta(hours = 1, minutes = 0)
    time_check_plus1 = datetime.datetime.now()
    
    #flag used to see if files have been recently uploaded to S3
    newFilesExist = False
    for file in bucket.objects.filter(Prefix= prefix):
        #compare dates 
        if file.last_modified.replace(tzinfo = None) > time_check:
            newFilesExist = True
            break
        
    if not newFilesExist:
        timestamp = datetime.datetime.now()
        alert = 'ALERT: no files recently uploaded for Isla Verde between {} UTC and {} UTC!\nemail timestamp: '.format(time_check, time_check_plus1) + str(timestamp)
        send_email_alert(alert)

    
    
