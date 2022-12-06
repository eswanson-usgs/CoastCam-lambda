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

#s#_client = boto3.client('s3')

# def verify_email_identity():
#     ses_client = boto3.client("ses", region_name="us-west-2")
#     response = ses_client.verify_email_identity(
#         EmailAddress="jbirchler@usgs.gov"
#     )
#     print(response)

def send_email_alert(Data):
    ses_client = boto3.client("ses", region_name="us-west-2")
    CHARSET = "UTF-8"

    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                #"csherwood@usgs.gov",
                'eswanson@contractor.usgs.gov'
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
                "Data": "CACO-01 Daily Email Alert",
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
        
        
def get_times(filepath):
    '''
    Given a S3 filepath, parse and return three time strings: epoch, GMT, and local (EST)
    Inputs:
        filepath (string) - path to file in S3
    Outputs:
        epoch (string) - epoch timestamp
        GMT (string) - GMT timestamp string
        EST (string) - EST timestamp string
    '''
    
    filename = filepath.split('/')[-1]
    epoch = filename.split('.')[0]
    GMT, obj, tz = unix2dt(epoch, timezone='utc')
    EST, obj, tz = unix2dt(epoch, timezone='eastern')
    
    return epoch, GMT, EST


def lambda_handler(event, context):
    
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket("cmgp-coastcam")
    
    now = datetime.datetime.now() #equal to now's timestamp
    year = str(now.year)
    month = str(now.month)
    day = str(now.day)
    
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
    
    c1_prefix = 'cameras/caco-01/c1/' + year + '/' + new_format_day
    c2_prefix = 'cameras/caco-01/c2/' + year + '/' + new_format_day
    
    ########PLACEHOLDER FOR TESTING
    # c1_prefix = 'cameras/caco-01/c1/' + year + '/274_Oct.01'
    # c2_prefix = 'cameras/caco-01/c2/' + year + '/274_Oct.01'
    
    timestamp = datetime.datetime.now()
    today = timestamp.strftime('%Y-%m-%d')
    
    #tally up files for current day for each camera
    tally = 0
    for file in bucket.objects.filter(Prefix=c1_prefix):
        
        #for first file 
        if tally == 0:
            c1_first_file = file.key
            
        tally = tally + 1
    
    if tally != 0:    
        c1_last_file = file.key
        
        #get timestamps in epoch, GMT, and local (EST) for the first and last files for each camera
        c1_first_epoch, c1_first_gmt, c1_first_est = get_times(c1_first_file)
        c1_last_epoch, c1_last_gmt, c1_last_est = get_times(c1_last_file)
    else:
        c1_first_file = None
    
    i = 0    
    for i, file in enumerate(bucket.objects.filter(Prefix=c2_prefix)):
        
        if i == 0:
            c2_first_file = file.key
            
        tally = tally + 1
    
    if i != 0:
        c2_last_file = file.key
        
        c2_first_epoch, c2_first_gmt, c2_first_est = get_times(c2_first_file)
        c2_last_epoch, c2_last_gmt, c2_last_est = get_times(c2_last_file)
    else:
        c2_first_file = None
        
    if tally != 0:
        
        #format email
        email = f"Total number of images for CACO-01 on {today}: {tally}\n"
        #if file exist for c1
        if c1_first_file != None:
            email = email + f"Time(s) for c1 first image: {c1_first_epoch} (epoch), {c1_first_gmt} GMT, {c1_first_est} EST\nTime(s) for c1 last image: {c1_last_epoch} (epoch), {c1_last_gmt} GMT, {c1_last_est} EST\n"
        #if file exist for c2
        if c2_first_file != None:
            email = email + f"Time(s) for c2 first image: {c2_first_epoch} (epoch), {c2_first_gmt} GMT, {c2_first_est} EST\nTime(s) for c2 last image: {c2_last_epoch} (epoch), {c2_last_gmt} GMT, {c2_last_est} EST"
                
    #no files found for today    
    else:
        email = 'ALERT: no files uploaded for CACO-01 on {}!\nemail timestamp: '.format(today) + str(timestamp)
        
    print(email)
    send_email_alert(email)
    
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    
    #need to download csv to temp folder then edit
    csv_path = "cameras/caco-01/caco-01_daily_tally.csv"
    download_path = '/tmp/caco-01_daily_tally.csv'
    with open(download_path, 'wb') as csv_file:
        s3.download_fileobj('cmgp-coastcam', csv_path, csv_file)
        
    #append image tally to csv file then upload to S3
    with open(download_path, 'a', encoding='utf-8-sig') as csvfile:
        fields = [today, tally]
        writer = csv.writer(csvfile)
        writer.writerow(fields)
        
    with open(download_path, 'rb') as new_csv:
        s3.upload_fileobj(new_csv, 'cmgp-coastcam', csv_path)
        
