"""
Eric Swanson
Purpose: Convert the filename for an images at Madeira Beach from the epoch only format to the argus format filename.Add leading zeros to the
date timestamps.

Description:
The purpose of this script is to convert the S3 filepath for all images in a folder in the
USGS Madeira Beach CoastCam bucket. Convert 
The old filepath is in the format s3:/cmgp-coastcam/cameras/madeira_beach/products/[long filename].
The new filepath is in the format s3:/cmgp-coastcam/cameras/madeira_beach]/[camera]/[year]/[day]/raw/[longfilename].
day is the format ddd_mmm.nn. ddd is 3-digit number describing day in the year.
mmm is 3 letter abbreviation of month. nn is 2 digit number of day of month.

The old filenames are in the format
[unix datetime].[camera#].[image type].[file extension]
The new filenames are in the format
[unix datetime].[Day of week].[Month].[DayOfMonth]_[Hour]_[Minute]_[second].[timezone].[Year].madbeach.[Camera#].[image type].[file extension]
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
        

def add_leading_zeros(args):
    """
    Change the filename of an image file in S3 to add leading zeros to the date timestamp of the filename.
    Input:
        args (tuple) contains the following:
            source_filepath - (string) current filepath of image where the image will be copied from.
            station (string) - station shortName
    Output:
        destination_filepath - (string) new filepath image is copied to.
    """

    source_filepath = args[0]
    station = args[1]
    
    isImage = check_image(source_filepath)

    if isImage == True:
        source_filepath = "s3://" + source_filepath
        old_path_elements = source_filepath.split("/")

        #remove empty space elements from the list
        for elements in old_path_elements:
            if len(elements) == 0: 
                old_path_elements.remove(elements)

        filename = old_path_elements[-1]

        filename_elements = filename.split(".")

        #if filepath not already converted
        if len(filename_elements) == 10:
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

            if needZero:
                date_timestamp = f"{day_of_month}_{hour}_{minute}_{second}"

                #reformat filename
                new_filename = filename.replace(old_timestamp, date_timestamp)
                new_filepath = source_filepath.replace(filename, '') + new_filename

                #Use fsspec to copy image from old name to new name. Delete old file
                file_system = fsspec.filesystem('s3', profile='coastcam')
                file_system.copy(source_filepath, new_filepath)
                file_system.delete(source_filepath)
                return new_filepath
            else:
                return("no leading zeros")

    #if not image, return blank string. Will be used to determine if file copy needs to be logged in csv
    else:
        return 'Not an image. Not copied.'
    

##### MAIN #####
print("start:", datetime.datetime.now())

common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

station = 'madbeach'

file_system = fsspec.filesystem('s3', profile='coastcam')

### for images in /products folder in S3 ###
source_folder = "s3://cmgp-coastcam/cameras/madeira_beach/products"                        
image_list = file_system.glob(source_folder+'/*')
args = ((image, station) for image in image_list)
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(add_leading_zeros, args)

##for result in results:
##    print(result)
    
print("end:", datetime.datetime.now())








