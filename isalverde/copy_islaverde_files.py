"""
Eric Swanson
Purpose: Copy images for Isla Verde in the CoastCam S3 bucket from the /products folder to the appropriately formated camera/date folders.
"""

##### REQUIERD PACKAGES #####
import numpy as np
import os
import time
#will need fs3 package to use s3 in fsspec
import fsspec 
import numpy as np
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
        

def copy_islaverde_image(source_filepath):
    """
    Copy an image file from its old filepath in the S3 bucket with the format
    s3://[bucket]/cameras/[station]/products/[filename]. to a new filepath with the format
    s3://[bucket]/cameras/[station]/[camera]/[year]/[day]/raw/[filename]
    day is in the format day is the format ddd_mmm.nn. ddd is 3-digit number describing day in the year.
    mmm is 3 letter abbreviation of month. nn is 2 digit number of day of month.
    New filepath is created and returned as a string by this function.
    Input:
        source_filepath - (string) current filepath of image where the image will be copied from.
    Output:
        destination_filepath - (string) new filepath image is copied to. --optional--
    """
    
    isImage = check_image(source_filepath)

    if isImage == True:
        source_filepath = "s3://" + source_filepath
        old_path_elements = source_filepath.split("/")

        #remove empty space elements from the list
        for elements in old_path_elements:
            if len(elements) == 0: 
                old_path_elements.remove(elements)

        bucket = old_path_elements[1]
        station = old_path_elements[3]
        filename = old_path_elements[5]
        
        filename_elements = filename.split(".")
        image_unix_time = filename_elements[0]
        cam_num = filename_elements[7]

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

        #add leading zeros
        if int(day_of_year) < 10 and (len(day_of_year) == 1):
            new_format_day = "00" + day_of_year + "_" + month_formatted + "." + day
        elif (int(day_of_year) >= 10) and (int(day_of_year) < 100) and (len(day_of_year) == 2):
            new_format_day = "0" + day_of_year + "_" + month_formatted + "." + day
        else:
            new_format_day = day_of_year + "_" + month_formatted + "." + day
        
        new_filepath = "s3://" + bucket + "/cameras/" + station + "/" + cam_num + "/" + year + "/" + new_format_day + "/raw/" #file not included
        destination_filepath = new_filepath + filename

        #Use fsspec to copy image from old path to new path
        file_system = fsspec.filesystem('s3', profile='coastcam')
        #file_system.copy(source_filepath, new_product_filepath)
        file_system.copy(source_filepath, destination_filepath)

        #return new_product_filepath
        return destination_filepath
    #if not image, return blank string. Will be used to determine if file copy needs to be logged in csv
    else:
        return 'Not an image. Not copied.'


##### MAIN #####
print("start:", datetime.datetime.now())
#source folder filepath with format s3:/cmgp-coastcam/cameras/[station]/products/[filename]
source_folder = "s3://cmgp-coastcam/cameras/islaverde/products/"  

file_system = fsspec.filesystem('s3', profile='coastcam')
image_list = file_system.glob(source_folder+'/*')

common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

#loop through images and copy
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(copy_islaverde_image, image_list)

print("end:", datetime.datetime.now())








