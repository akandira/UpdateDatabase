"""
This script extracts lab record file names present in the target directory and 
sends it to reporttype_detect function

@author: Anvitha Kandiraju
"""

import configparser
from os import walk
import processing as process

# get target folder name from config file
config = configparser.ConfigParser()
config.sections()
config.read('../config.ini')
folder_path = config['FolderPath']['path']

# read all the files from target directory and create a list
file_names = []
for (dirpath, dirnames, filenames) in walk(folder_path):
    file_names.extend(filenames)
    break

# send each file from the file_names list to reporttype_detect function
for file in file_names:
    process.reporttype_detect(folder_path + file)


