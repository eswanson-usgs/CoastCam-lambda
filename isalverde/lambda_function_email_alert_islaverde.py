
import boto3
import datetime
import calendar

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
    
    time_check = datetime.datetime.now() - datetime.timedelta(hours = 1, minutes = 0)
    time_check_plus1 = datetime.datetime.now() #equal to now's timestamp
    
    year = str(time_check.year)
    month = str(time_check.month)
    day = str(time_check.day)
    
    #day format for new filepath will have to be in format ddd_mmm.nn
    #timetuple() method returns tuple with several date and time attributes. tm_yday is the (attribute) day of the year
    day_of_year = str(datetime.date(int(year), int(month), int(day)).timetuple().tm_yday)

    #can use built-in calendar attribute month_name[month] to get month name from a number. Month cannot have leading zeros
    month_word = calendar.month_name[int(month)]
    #month in the mmm word form
    month_formatted = month_word[0:3] 

    #add leading zeros
    if int(day_of_year) < 10 and (len(day_of_year) == 1):
        new_format_day = "00" + day_of_year + "_" + month_formatted + "." + day
    elif (int(day_of_year) >= 10) and (int(day_of_year) < 100) and (len(day_of_year) == 2):
        new_format_day = "0" + day_of_year + "_" + month_formatted + "." + day
    else:
        new_format_day = day_of_year + "_" + month_formatted + "." + day
    
    prefix = 'cameras/islaverde/c1/' + year + '/' + new_format_day
    
    #flag used to see if files have been recently uploaded to S3
    newFilesExist = False
    for file in bucket.objects.filter(Prefix= prefix):
        #compare dates 
        if file.last_modified.replace(tzinfo = None) > time_check:
            newFilesExist = True
            print(file)
            break
        
    if not newFilesExist:
        timestamp = datetime.datetime.now()
        alert = 'ALERT: no files recently uploaded for Isla Verde between {} UTC and {} UTC!\nemail timestamp: '.format(time_check, time_check_plus1) + str(timestamp)
        send_email_alert(alert)

    
    
