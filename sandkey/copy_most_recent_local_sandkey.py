#copy files to S3 only for most recent hour

import os
import datetime
import fsspec

SITE = "sandkey"
DIRCAM = os.path.join('C:\\Users\\camerauser',SITE)
now = datetime.datetime.now()

########################################################
os.chdir('C:\\Users\\camerauser\\sandkey\\toBeSent\\c2')
DIR = os.getcwd()

file_system = fsspec.filesystem('s3', profile='coastcam')

# loop through all days/directories to process images (on D: drive)
files=os.listdir(DIR)
for ii, _ in enumerate(files):
    #only copy files from most recent day (today). len(files) - 1 because ii starts at 0 and len() starts at 1
    if ii == len(files) -1:
        name=files[ii]
        doy=int(name[0:3])
        
        YEAR = now.year
        if doy == 1:
           YEAR = YEAR - 1
        
        YD = datetime.datetime.strptime("%4d %03d" % (YEAR, doy),'%Y %j')
        
        YEAR = str(YEAR)
        MONTH = str(YD.month)
        DAY = str(YD.day)

        
        print("Processing files from %s/%s/%s" % (MONTH, DAY, YEAR))
        
        HOURDIR=os.path.join(DIR,name)
        hours=os.listdir(HOURDIR)
        for iii, HOUR in enumerate(hours):
            #only print most recent hour. len(houra) - 1 because ii starts at 0 and len() starts at 1. Skip .txt files
            if (iii == len(hours) - 1) and not HOUR.endswith('.txt'):
                print(HOUR)

                FILEDIR = os.path.join(HOURDIR, HOUR)
                
                hour_files = os.listdir(FILEDIR)
                for file in hour_files:
                    local_path = os.path.join(FILEDIR, file)
                    destination_filepath = "s3://cmgp-coastcam/cameras/sandkey/products/" + file
                    file_system.upload(local_path, destination_filepath)
            else:
                continue

    else:
        continue
 
