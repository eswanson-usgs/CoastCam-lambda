"""
Eric Swanson
Purpose: Convert the filename for an image at Madeira beach from the epoch only format to the argus format filename.

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
        

def change_image_name(source_filepath):
    """
    Change the filename of an image file in S3 from the epoch-only format to the argus-style format. Functions copies the image to
    the same folder in S3
    Input:
        source_filepath - (string) current filepath of image where the image will be copied from.
    Output:
        destination_filepath - (string) new filepath image is copied to.
    """
    
    isImage = check_image(source_filepath)

    if isImage == True:
        source_filepath = "s3://" + source_filepath
        old_path_elements = source_filepath.split("/")

##        #remove empty space elements from the list
##        for elements in old_path_elements:
##            if len(elements) == 0: 
##                old_path_elements.remove(elements)
##
##
##        bucket = old_path_elements[1]
##        station = old_path_elements[3]
##        filename = old_path_elements[5]

##        filename_elements = filename.split(".")
##        if len(filename_elements) == 4:
##            return "Skipped. Filename already converted"
##        else:
##            image_unix_time = filename_elements[0]
##            image_camera = filename_elements[7]
##            image_type = filename_elements[8]
##            image_file_type = filename_elements[9]
##
##            #convert unix time to date-time str in the format "yyyy-mm-dd HH:MM:SS"
##            image_date_time, date_time_obj = unix2datetime(image_unix_time) 
##            year = image_date_time[0:4]
##            month = image_date_time[5:7]
##            day = image_date_time[8:10]
##            
##            #day format for new filepath will have to be in format ddd_mmm.nn
##            #timetuple() method returns tuple with several date and time attributes. tm_yday is the (attribute) day of the year
##            day_of_year = str(datetime.date(int(year), int(month), int(day)).timetuple().tm_yday)
##
##            #can use built-in calendar attribute month_name[month] to get month name from a number. Month cannot have leading zeros
##            month_word = calendar.month_name[int(month)]
##            #month in the mmm word form
##            month_formatted = month_word[0:3] 
##
##            new_format_day = day_of_year + "_" + month_formatted + "." + day
##
##            #reformat camera number
##            cam_num = "c" + str(image_camera.split("Camera")[1])
##
##            #reformat filename
##            new_filename = image_unix_time + "." + cam_num + "." + image_type + "." + image_file_type
##            print(new_filename)
##            
##            new_filepath = "s3://" + bucket + "/cameras/" + station + "/" + cam_num + "/" + year + "/" + new_format_day + "/raw/" #file not included
##            destination_filepath = new_filepath + new_filename
##
##            ###RENAME FILES IN /PRODUCTS FILPEATH ON S3
##            new_product_filepath = "s3://cmgp-coastcam/cameras/madeira_beach/products/" + new_filename
##
##            #Use fsspec to copy image from old path to new path
##            file_system = fsspec.filesystem('s3', profile='coastcam')
##            #file_system.copy(source_filepath, new_product_filepath)
##            file_system.copy(source_filepath, new_product_filepath)
##
##            #return new_product_filepath
##            return destination_filepath
##    #if not image, return blank string. Will be used to determine if file copy needs to be logged in csv
##    else:
        return 'Not an image. Not copied.'


def write2csv(csv_list, csv_path):
    """
    Write data pertaining to the copied image files to a csv speified by the user.
    Input:
        csv_list - list (of lists) containing data to be written to csv. Each list includes source filepath & destination filepath
        csv_path - desried location of generated csv file
    Return:
        None. However, csv file will appear in filepath the user specified.
    """

    #header
    fieldnames = ['source filepath', 'destination filepath'] 

    now = datetime.datetime.now()
    now_string = now.strftime("%d-%m-%Y %H_%M_%S")
    csv_name = csv_path + 'image copy log ' + now_string + '.csv'

    with open(csv_name, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(csv_list)
    return 


##### MAIN #####
print("start:", datetime.datetime.now())
#source folder filepath with format s3:/cmgp-coastcam/cameras/[station]/products/[filename]
source_folder = "s3://cmgp-coastcam/cameras/madeira_beach/products/"  

file_system = fsspec.filesystem('s3', profile='coastcam')
image_list = file_system.glob(source_folder+'/*')

common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

#used to track copied images in a csv
csv_path = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam/s3_filepaths/csv/"
csv_list = []

#loop through folder of images
for image in image_list:
    for image_type in common_image_list: 
        isImage = False 
        if image.endswith(image_type):
            isImage = True
            break
    if image.endswith('.txt') or isImage == False:
        continue
    else:
        source_filepath = image
        dest_filepath = chnage_image_name(source_filepath)

##        csv_entry = [source_filepath, dest_filepath]
##        csv_list.append(csv_entry)

##now = datetime.datetime.now()
##now_string = now.strftime("%d-%m-%Y %H_%M_%S")
##csv_name = 'image copy log ' + now_string + '.csv'
##write2csv(csv_list, csv_path)
print("end:", datetime.datetime.now())








