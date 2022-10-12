#Email alert LAmbda function for Dream Inn, CA. Triggers once a day at 10 AM local and checks if any new imagery made it to the /latest folder. Does so by checking the csv file
#in the station's folder. Function searches /latest folder to see if there is imagery with an epoch timestamp greater than what is in the csv file. If not, camera has not uploaded imagery
#and email alert is sent out
'''
Send an email alert once daily for CACO-01 coastcam. Send a tally of total images collected between both cameas for that day, as well as the first and last files for each camera. Append the tally
to a csv file on S3 (keep track of tally for each day).
'''


import boto3
import datetime
import calendar
import csv
import dateutil
from dateutil import tz

# def verify_email_identity():
#     ses_client = boto3.client("ses", region_name="us-west-2")
#     response = ses_client.verify_email_identity(
#         EmailAddress="edailey@usgs.gov"
#     )
#     print(response)
    

def send_email_alert(Data):
    ses_client = boto3.client("ses", region_name="us-west-2")
    CHARSET = "UTF-8"

    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                'dnowacki@usgs.gov', 'edailey@usgs.gov', 'eswanson@contractor.usgs.gov',
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
                "Data": "Dream Inn Email Alert",
            },
        },
        Source="eswanson@contractor.usgs.gov"
        )
        
 
def unix2dt(unixnumber, timezone='utc'):
    """
    Get local time from unix number
    Input:
        unixnumber (string) - string containing unix time (aka epoch)
    Outputs:
        dateTimeString (string) - datetime string in the local user's timezone 
        dateTimeObject (datetime) - datetime object in the local user's timezone
        tzone (dateutil.tz) - dateutil timezone object
    """
    
    if timezone.lower() == 'eastern':
        tzone = tz.gettz('America/New_York')
    elif timezone.lower() == 'pacific':
        tzone = tz.gettz('America/Los_Angeles')
    elif timezone.lower() == 'utc':
        tzone = tz.gettz('UTC')
        
    # replace last digit with zero
    ts = int( unixnumber[:-1]+'0')
    dateTimeObj =  datetime.datetime.utcfromtimestamp(ts)
    #convert from UTC to local time zone on user's machine
    dateTimeObj = dateTimeObj.replace(tzinfo=datetime.timezone.utc).astimezone(tz=tzone)
    dateTimeStr = dateTimeObj.strftime('%Y-%m-%d %H:%M:%S')
    return dateTimeStr, dateTimeObj, tzone       

### MAIN ###

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket("cmgp-coastcam")
    
    #read csv from S3 to get latest times from /latest and /products
    bucket_name = 'cmgp-coastcam'
    csv_key = "cameras/dreaminn/dreaminn_most_recent_time.csv"
    
    #need to download csv to temp folder
    download_path = '/tmp/dreaminn_most_recent_time.csv'
    with open(download_path, 'wb') as csv_file:
        s3.download_fileobj('cmgp-coastcam', csv_key, csv_file)
        
    with open(download_path, 'r') as csv_file:
        csvreader = csv.reader(csv_file)
        
        for i, row in enumerate(csvreader):
            #first row is just headers
            if i == 1:
                #store epoch times for newwest imagery
                newest_latest = row[0]
                newest_latest_str, newest_latest_obj, tzone = unix2dt(str(newest_latest), timezone='utc')
                print('csv time:', newest_latest)
                
    latest_prefix = "cameras/dreaminn/latest"
    
    #search /latest for file with more recent epoch time than what is in csv
    cameraTurnedOn = False
    for file in bucket.objects.filter(Prefix=latest_prefix):
        
        #if file has been modified since the time listed in the csv, camera is turned on and working
        if file.last_modified.replace(tzinfo=None) > newest_latest_obj.replace(tzinfo=None):
            
            cameraTurnedOn = True
            newest_latest_obj = file.last_modified
            recent_time = int(newest_latest_obj.timestamp())
            recent_time = str(recent_time)
            print('new time:', recent_time)
            
    #update csv with most recent time
    if cameraTurnedOn:
        with open(download_path, 'w', encoding='utf-8-sig') as csvfile:
            header = ['latest']
            value = [recent_time]
            writer = csv.writer(csvfile)
            writer.writerow(header)
            writer.writerow(value)
            
        with open(download_path, 'rb') as new_csv:
            s3.upload_fileobj(new_csv, 'cmgp-coastcam', csv_key)
            
    else:
        #camera didn't turn on, send email alert
        timestamp = datetime.datetime.now()
        send_email_alert('Camera did not turn on today: {}\nEmail timestamp: {}'.format(timestamp.strftime('%Y-%d-%m'), timestamp.strftime('%Y-%m-%d %H:%M:%S')))
