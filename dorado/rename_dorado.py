"""
Eric Swanson
Purpose: Convert the filename for an image at Dorado to the proper argus-style format.
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
        

def rename_dorado(args):
    """
    Rename a file for the Dorado CoastCam in S3. Functions copies the image to the same folder in S3.
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
        if len(filename_elements) == 10:

            #Use fsspec to copy image from old name to new name. Delete old file
            file_system = fsspec.filesystem('s3', profile='coastcam')
            
            image_unix_time = filename_elements[0]
            cam_num = filename_elements[7]
            if cam_num == 'Camera1':
                new_filepath = source_filepath.replace('Camera1', 'c1')
                file_system.copy(source_filepath, new_filepath)
                file_system.delete(source_filepath)
            elif cam_num == 'Camera2':
                new_filepath = source_filepath.replace('Camera2', 'c2')
                file_system.copy(source_filepath, new_filepath)
                file_system.delete(source_filepath)
                
    #if not image, return blank string. Will be used to determine if file copy needs to be logged in csv
    else:
        return 'Not copied.'
    

##### MAIN #####
print("start:", datetime.datetime.now())

common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

station = 'dorado'

file_system = fsspec.filesystem('s3', profile='coastcam')

source_folder = "s3://cmgp-coastcam/cameras/dorado/products"                        
image_list = file_system.glob(source_folder+'/*')
args = ((image, station) for image in image_list)
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(rename_dorado, args)
        
print("end:", datetime.datetime.now())








