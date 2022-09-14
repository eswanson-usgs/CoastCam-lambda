"""
Eric Swanson
The purpose of this script is to convert the S3 filepath for all images in a folder in the Madeira Beach
CoastCam bucket. This usees a test S3 bucket and not the public-facing cmgp-CoastCam bucket.
The old filepath is in the format s3:/cmgp-coastcam/cameras/[station]/products/[long filename].
The new filepath is in the format s3:/cmgp-coastcam/cameras/[station]/[camera]/[year]/[day]/[longfilename].
day is the format ddd_mmm.nn. ddd is 3-digit number describing day in the year.
mmm is 3 letter abbreviation of month. nn is 2 digit number of day of month.

This script splits up the filepath of the old path to be used in the new path. The elements used in the
new path are the [station] and [long filename]. Then it splits up the filename to get elements used in the new path.
[unix datetime] is used to get [year], [day], and [camera]. unix2datetime() converts the unix time in the filename
to a human-readable datetime object and string. Once the new filepath is created, the S3 buckets are accessed using
fsspec and the image is copied from one path to another use the fsspec copy() method. This is done using the function
copy_s3_image(). Only common image type files will be copied. Images are copied using multithreading
in the concurrent.futures module.
write2csv() is used to write the source and destination filepath to a csv file.
"""

##### IMPORT #####
import os
import time
#will need fs3 package to use s3 in fsspec
import fsspec 
import imageio
import calendar
import datetime
import csv
import concurrent.futures

##### FUNCTIONS #####
def unix2datetime(unixnumber):
    """
    Developed from unix2dts by Chris Sherwood. Updates by Eric Swanson.
    Get datetime object and string (in UTC) from unix/epoch time. datetime object is "aware",
    meaning it always references a specific point in time rather than a time relative to local time.
    datetime object will be the same regardless of what timezone the function is run in.
    Input:
        unixnumber - string containing unix time (aka epoch)
    Returns:
        date_time_string, date_time_object in utc
    """

    # images other than "snaps" end in 1, 2,...but these are not part of the time stamp.
    # replace with zero
    time_stamp = int( unixnumber[:-1]+'0')
    date_time_obj =  datetime.datetime.fromtimestamp(time_stamp, tz=datetime.timezone.utc)
    date_time_str = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')
    return date_time_str, date_time_obj

def check_image(file):
    """
    Check if the file is an image (of the proper type)
    Input:
        file - (string) filepath of the file to be checked
    Output:
        isImage - (bool) variable saying whether or not file is an image
    """
    
    common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']
    
    isImage = False
    for image_type in common_image_list:
        if file.endswith(image_type):
            isImage = True
    return isImage
        

def copy_s3_image(source_filepath):
    """
    Copy an image file from its old filepath in the S3 bucket with the format
    s3://[bucket]/cameras/[station]/products/[long filename]. to a new filepath with the format
    s3://[bucket]/cameras/[station]/[camera]/[year]/[day]/[filename]
    day is in the format day is the format ddd_mmm.nn. ddd is 3-digit number describing day in the year.
    mmm is 3 letter abbreviation of month. nn is 2 digit number of day of month.
    New filepath is created and returned as a string by this function.
    Input:
        source_filepath - (string) current filepath of image where the image will be copied from.
    Output:
        destination_filepath - (string) new filepath image is copied to.
    """
    
    isImage = check_image(source_filepath)

    if isImage == True:
        source_filepath = "s3://" + source_filepath
        old_path_elements = source_filepath.split("/")

        #remove empty space elements from the list
        #list will have 5 elements: "[bucket]", "cameras", "[station]", "products", "[image filename]"
        for elements in old_path_elements:
            if len(elements) == 0: 
                old_path_elements.remove(elements)

        bucket = old_path_elements[1]
        station = old_path_elements[3]
        filename = old_path_elements[-1]

        filename_elements = filename.split(".")
        #check to see if filename is properly formatted
        if len(filename_elements) != 11:
            return 'Not properly formatted. Not copied.'
        else:
            image_unix_time = filename_elements[0]
            image_camera = filename_elements[7] 
            image_type = filename_elements[8]
            image_file_type = filename_elements[-1]

            #convert unix time to date-time str in the format "yyyy-mm-dd HH:MM:SS"
            image_date_time, date_time_obj = unix2datetime(image_unix_time) 
            year = image_date_time[0:4]
            month = image_date_time[5:7]
            day = image_date_time[8:10]
            
            #day format for new filepath will have to be in format ddd_mmm.nn
            #timetuple() method returns tuple with several date and time attributes. tm_yday is the (attribute) day of the year
            day_of_year = str(datetime.date(int(year), int(month), int(day)).timetuple().tm_yday)

            #can use built-in calendar attribute month_name[month] to get month name from a number. Month cannot have leading zeros
            month_word = calendar.month_name[int(month)]
            #month in the mmm word form
            month_formatted = month_word[0:3]

            if int(day_of_year) < 10 and (len(day_of_year) == 1):
                new_format_day = "00" + day_of_year + "_" + month_formatted + "." + day
            elif (int(day_of_year) >= 10) and (int(day_of_year) < 100) and (len(day_of_year) == 2):
                new_format_day = "0" + day_of_year + "_" + month_formatted + "." + day
            else:
                new_format_day = day_of_year + "_" + month_formatted + "." + day

            date_timestamp = filename_elements[3]
            old_timestamp = date_timestamp
            date_elements = date_timestamp.split('_')
            day_of_month = date_elements[0]
            hour = date_elements[1]
            minute = date_elements[2]
            second = date_elements[3]

            needZero = False
            if len(day_of_month) == 1:
                needZero = True
                day_of_month =  '0' + day_of_month
            if len(hour) == 1:
                needZero = True
                hour = '0' + hour
            if len(minute) == 1:
                needZero = True
                minute = '0' + minute
            if len(second) == 1:
                needZero = True
                second = '0' + second

            new_filepath = "s3:/" + "/" + bucket + "/cameras/" + station + "/" + image_camera + "/" + year + "/" + new_format_day + "/" #file not included

            if needZero:
                date_timestamp = f"{day_of_month}_{hour}_{minute}_{second}"

                #reformat filename
                new_filename = filename.replace(old_timestamp, date_timestamp)
                destination_filepath = new_filepath + new_filename
            else:
                destination_filepath = new_filepath + filename
            

            #Use fsspec to copy image from old path to new path
            file_system = fsspec.filesystem('s3', profile='coastcam')
            file_system.copy(source_filepath, destination_filepath)
            return destination_filepath
    #if not image, return blank string. Will be used to determine if file copy needs to be logged in csv
    else:
        return 'Not an image. Not copied.'
    

##### MAIN #####
print("start:", datetime.datetime.now())
#source folder filepath with format s3:/cmgp-coastcam/cameras/[station]/products/[filename]
source_folder = "s3://cmgp-coastcam/cameras/madeira_beach/products"  

#station madeira_beach for testing
file_system = fsspec.filesystem('s3', profile='coastcam')
image_list = file_system.glob(source_folder+'/*')

common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

#used to track copied images in a csv
csv_path = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam-lambda/madeira_beach/csv/"
csv_list = []

### for images already in the cameras/date format filepath on S3 ###
source_folder = "s3://cmgp-coastcam/cameras/madeira_beach"
camera_list = file_system.glob(source_folder+'/c*')

for cam_path in camera_list:
    cam_path = 's3://' + cam_path

    year_list = file_system.glob(cam_path+'/*')
    for year_path in year_list:
        if year_path.endswith('merge'):
            merge_year_list = file_system.glob('s3://'+year_path+'/*')
            for merge_year_path in merge_year_list:
                merge_year_path = 's3://' + merge_year_path

                day_list = file_system.glob(merge_year_path+'/*')
                for day_path in day_list:
                    day_path = 's3://' + day_path

                    image_list = file_system.glob(day_path+'/*')
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        results = executor.map(copy_s3_image, image_list)

print("end:", datetime.datetime.now())




