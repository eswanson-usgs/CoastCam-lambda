"""
Eric Swanson
Purpose: Convert the filename for an images at Nuvuk from the epoch only format to the argus format filename.

Description:
The purpose of this script is to convert the S3 filepath for all images in a folder in the
USGS CoastCam bucket. Convert 
The old filepath is in the format s3:/cmgp-coastcam/cameras/nuvuk/products/[long filename].
The new filepath is in the format s3:/cmgp-coastcam/cameras/nuvuk]/[camera]/[year]/[day]/raw/[longfilename].
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
        

def epoch2argus(args):
    """
    Change the filename of an image file in S3 from the epoch-only format to the argus-style format. Functions copies the image to
    the same folder in S3
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

        
        bucket = old_path_elements[1]
        filename = old_path_elements[-1]

        filename_elements = filename.split(".")

        #if filepath not already converted
        if len(filename_elements) == 4:
            image_unix_time = filename_elements[0]
            cam_num = filename_elements[1]
            image_type = filename_elements[2]
            #special case for merge images:
            if cam_num == 'timex':
                cam_num = 'cx'
                image_type = 'timex.merge'
            image_file_type = filename_elements[3]

            #convert unix time to date-time str in the format "yyyy-mm-dd HH:MM:SS"
            image_date_time, date_time_obj = unix2datetime(image_unix_time)
            year = date_time_obj.year
            month = date_time_obj.month
            day = date_time_obj.day
            hour = date_time_obj.hour
            minute = date_time_obj.minute
            second = date_time_obj.second

            timezone = 'GMT'

            day_of_week = calendar.day_name[date_time_obj.weekday()]
            day_of_week = day_of_week[0:3]
            
            #day format for new filepath will have to be in format ddd_mmm.nn
            #timetuple() method returns tuple with several date and time attributes. tm_yday is the (attribute) day of the year
            day_of_year = str(datetime.date(int(year), int(month), int(day)).timetuple().tm_yday)

            #can use built-in calendar attribute month_name[month] to get month name from a number. Month cannot have leading zeros
            month_word = calendar.month_name[int(month)]
            #month in the mmm word form
            month_formatted = month_word[0:3] 

            #reformat filename
            new_filename = f"{image_unix_time}.{day_of_week}.{month_formatted}.{day}_{hour}_{minute}_{second}.{timezone}.{year}.{station}.{cam_num}.{image_type}.{image_file_type}"
            new_filepath = source_filepath.replace(filename, '') + new_filename

            #Use fsspec to copy image from old name to new name. Delete old file
            file_system = fsspec.filesystem('s3', profile='coastcam')
            file_system.copy(source_filepath, new_filepath)
            file_system.delete(source_filepath)

            #return new_product_filepath
            return new_filepath
    #if not image, return blank string. Will be used to determine if file copy needs to be logged in csv
    else:
        return 'Not an image. Not copied.'
    

##### MAIN #####
print("start:", datetime.datetime.now())

common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

station = 'nuvuk'

file_system = fsspec.filesystem('s3', profile='coastcam')

### for images already in the cameras/date format filepath on S3 ###
source_folder = "s3://cmgp-coastcam/cameras/nuvuk"
camera_list = file_system.glob(source_folder+'/c*')

for cam_path in camera_list:
    if cam_path.endswith('calibration'):
        continue
    
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
                    args = ((image, station) for image in image_list)
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        results = executor.map(epoch2argus, args)

        else:
            day_list = file_system.glob(year_path+'/*')
            for day_path in day_list:
                day_path = 's3://' + day_path

                image_list = file_system.glob(day_path+'/raw/*')
                args = ((image, station) for image in image_list)
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    results = executor.map(epoch2argus, args)

### for images in /products folder in S3 ###
source_folder = "s3://cmgp-coastcam/cameras/nuvuk/products"                        
image_list = file_system.glob(source_folder+'/*')
args = ((image, station) for image in image_list)
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(epoch2argus, args)

print("end:", datetime.datetime.now())








