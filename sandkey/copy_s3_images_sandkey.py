"""
Eric Swanson
The purpose of this script is to convert the S3 filepath for all logs files (.txt) for the Sand Key station.
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
        

def copy_s3_text(source_filepath):
    """
    Copy a text file from the /products foledr to the /logs folder.
    Input:
        source_filepath - (string) current filepath of the file where the file will be copied from.
    Output:
        dest_filepath - (string) new filepath the file is copied to.
    """
    
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
    #check to see if filename is for text file
    if filename.endswith('.txt'):
        dest_filepath = "s3://cmgp-coastcam/cameras/sandkey/logs/" + filename

        #copy file
        file_system = fsspec.filesystem('s3', profile='coastcam')
        file_system.copy(source_filepath, dest_filepath)
        return dest_filepath

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
source_folder = "s3://cmgp-coastcam/cameras/sandkey/products"  

#station sandkey for testing
file_system = fsspec.filesystem('s3', profile='coastcam')
image_list = file_system.glob(source_folder+'/*')

common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

#used to track copied images in a csv
csv_path = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam-lambda/sandkey/csv/"
csv_list = []

#ProcessPoolExecutor is used for multithreading (allows multiple instances of function to be run at once)
#result contains filepaths that were copied (in order that they exist in image_list)
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(copy_s3_text, image_list)

    #iterate over outputted destination filepaths. Need to iterate  i because results is a generator, not a list.
    #create csv entry pairs from source and destination filepaths. This includes non-image files.
    i = 0
    for dest_filepath in results:
        #print(dest_filepath)
        source_filepath = "s3://" + image_list[i]
        csv_entry = [source_filepath, dest_filepath]
        csv_list.append(csv_entry)
        i = i + 1
        
now = datetime.datetime.now()
now_string = now.strftime("%d-%m-%Y %H_%M_%S")
csv_name = 'image copy log ' + now_string + '.csv'
write2csv(csv_list, csv_path)
print("end:", datetime.datetime.now())




