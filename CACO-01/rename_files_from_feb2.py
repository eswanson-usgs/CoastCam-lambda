"""
Eric Swanson
Copy files eroneously copied into the 022_Feb.02 folder into the right folder for CACO-01
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
        
        #elif filepath not already converted
        if len(filename_elements) == 10:
            image_unix_time = filename_elements[0]

            image_camera = filename_elements[7]
            image_type = filename_elements[8]
            image_file_type = filename_elements[-1]

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

            #add leading zero to new filename if necessary
            if len(str(day)) == 1:
                needZero = True
                day =  '0' + str(day)
            if len(str(hour)) == 1:
                needZero = True
                hour = '0' + str(hour)
            if len(str(minute)) == 1:
                needZero = True
                minute = '0' + str(minute)
            if len(str(second)) == 1:
                needZero = True
                second = '0' + str(second)

            if int(day_of_year) < 10 and (len(day_of_year) == 1):
                new_format_day = "00" + day_of_year + "_" + month_formatted + "." + str(day)
            elif (int(day_of_year) >= 10) and (int(day_of_year) < 100) and (len(day_of_year) == 2):
                new_format_day = "0" + day_of_year + "_" + month_formatted + "." + str(day)
            else:
                new_format_day = day_of_year + "_" + month_formatted + "." + str(day)

            #reformat filename
            new_filename = f"{image_unix_time}.{day_of_week}.{month_formatted}.{day}_{hour}_{minute}_{second}.{timezone}.{year}.{station}.{image_camera}.{image_type}.{image_file_type}"
            new_filepath = "s3://" + bucket + "/cameras/" + station + "/" + image_camera + "/" + str(year) + "/" + new_format_day + "/" #file not included

            destination_filepath = new_filepath + new_filename

##            #Use fsspec to copy image from old name to new name. Delete old file
            file_system = fsspec.filesystem('s3', profile='coastcam')
            file_system.copy(source_filepath, destination_filepath)

            #return new_product_filepath
            return destination_filepath
    #if not image, return blank string. Will be used to determine if file copy needs to be logged in csv
    else:
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
source_folder = "s3://cmgp-coastcam/cameras/caco-01/c2/2023/033_Feb.02/"  

#station caco-01 for testing
file_system = fsspec.filesystem('s3', profile='coastcam')
image_list = file_system.glob(source_folder+'/*')

common_image_list = ['.tif', '.tiff', '.bmp', 'jpg', '.jpeg', '.gif', '.png', '.eps', 'raw', 'cr2', '.nef', '.orf', '.sr2']

#used to track copied images in a csv
csv_path = "C:/Users/eswanson/OneDrive - DOI/Documents/GitHub/CoastCam-lambda/caco-01/csv/"
csv_list = []

#ProcessPoolExecutor is used for multithreading (allows multiple instances of function to be run at once)
#result contains filepaths that were copied (in order that they exist in image_list)
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(copy_s3_image, image_list)

    #iterate over outputted destination filepaths. Need to iterate  i because results is a generator, not a list.
    #create csv entry pairs from source and destination filepaths. This includes non-image files.
    i = 0
    for dest_filepath in results:
        print(dest_filepath)
        source_filepath = "s3://" + image_list[i]
        csv_entry = [source_filepath, dest_filepath]
        csv_list.append(csv_entry)
        i = i + 1
        
now = datetime.datetime.now()
now_string = now.strftime("%d-%m-%Y %H_%M_%S")
csv_name = 'image copy log ' + now_string + '.csv'
write2csv(csv_list, csv_path)
print("end:", datetime.datetime.now())




