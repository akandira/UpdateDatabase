"""
This script monitors a directory for new files and triggers a reporttype_detect
function

@author: Anvitha Kandiraju
"""

import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import configparser
import processing as process

# parse configuration file
config = configparser.ConfigParser()
config.sections()
config.read('../config.ini')
folder_path = config['FolderPath']['path']


# Function to notify on new file
class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        """ Detect new file"""
        process.reporttype_detect(event.src_path)


observer = Observer()  # create observer
event_handler = NewFileHandler()  # create event handler

# set observer to use created handler in directory
observer.schedule(event_handler, path=folder_path)
observer.start()

# sleep until keyboard interrupt, then stop + rejoin the observer
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()

observer.join()