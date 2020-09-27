## Organization:
**Processing:** This folder contains scripts that process the lab data and upload it to a PostgreSQL database. This folder also contains a Watchdog script to detect new files in a folder and automatically upload the new lab reports to database.

 - startup.py: script that should be run on startup to transfer records
   from a folder to database
   
  - watchdog.py: script that monitors target directory and uploads any new reports to the database
  
   - processing.py: contains helper functions to process lab reports

**Visualization:** This folder contains visualization related script and Jupyter Notebook to visualize results.
- app.py:  Helper functions to visualize data in the database
- VisualizationNotebook: Notebook to query database and visualize results
 
 **config.ini:** Configuration file to provide user defined settings
 
 **requirements.txt:** file that contains dependencies for successful functioning of the application
 ## How to run
The following assumes that you have Python 3.8.x installed on your system and have access to Jupyter Notebook. If not, please install a compatible environment on your system such as Anaconda and setup Jupyter Notebook. Additionally, the code assumes the PostgreSQL tables for Material Procurement, Ball milling, and Hot press are already present in your database. (If not, please use ```\i processdb.sql``` in the ```psql``` shell to import the tables from the processdb.sql file). 

 Step-1 : Run below command to download all the dependent libraries to the Python environment.
 ```
 pip install -r requirements.txt
```
 Step-2 Edit the config.ini file to provide user settings for your system.

![config](https://user-images.githubusercontent.com/43352808/93659630-16558680-f9fc-11ea-98f6-0718c5401a2a.png)

Step-3: Run startup.py script to load lab reports from target folder to the database. Check the log file generated in the logfile path specified to get status of the upload.
 ![logfileCapture](https://user-images.githubusercontent.com/43352808/93659703-15712480-f9fd-11ea-9d69-b08dd771abef.PNG) 
 
 Step-4: Open Visualization Notebook and run the cells to see visualization and perform database queries. The graphs are interactive feel free to hover, zoom in, etc. to see property values!
 ![fig1](https://user-images.githubusercontent.com/43352808/93717853-4486b380-fb2d-11ea-8625-1bec0c44986a.PNG)
 ![fig3](https://user-images.githubusercontent.com/43352808/93659859-8a912980-f9fe-11ea-9e9b-2c87135836a3.PNG)
 ![fig2](https://user-images.githubusercontent.com/43352808/93659899-b0b6c980-f9fe-11ea-85b2-4a4bdc71223a.PNG)

## Challenge Part II: 

What information would you gather from the X-Materials’ team to design an
integration pipeline?
1) How much data do we anticipate to be handling via the ETL pipeline? How many users do we anticipate? What portions of the data must be made available to all users and should certain users have administrator privileges? 
2) Is there a possibility to add new type of lab reports in future, i.e. unrelated to the Hall measurements or the ICP measurements?
2) Some of the database relationships were not clear, for example some of the materials were missing Hall measurements or ICP reports. Having an understanding about the collection of reports, how often they're collected, how many materials/runs, size etc. helps in designing the database structure as well as efficient relations between tables, e.g. using FOREIGN/PRIMARY KEYs as well as INDEXing for optimal searching. 
3) From Visualization standpoint, database properties showed very low correlation between the columns. This could be due to a relatively small amount of data or could also be because of missing key parameters in database. Having this information can help create informed analysis tools/visualization tools. 
4) Are the devices that perform the measurements networked? Would we need to consider IoT (Internet of Things)-type approaches to continually monitor the instruments and/or extract data from measurements so that lab operators may not need to perform data records? 

What are potential bottlenecks or roadblocks you might run into?

Broadly, the most significant roadblocks for an integrated pipeline would be if the type or format of data is changed frequently, # of measurements change, recording of parameters is inconsistent (e.g. different units are used for the same measurement):
- changes in format of report, e.g. adding new parameters
- Additionally, depending on the company and its interests, if the security of data/lab reports is important, we would have to think carefully about authorized access to data, which is better to incorporate throughout the system from the beginning.

The potential bottlenecks would be related to the size of data and # of users accessing a web-based app, for example: 
- large number of users requesting DB queries could slow down web-based apps
- backups of data/DBs might need to be maintained for reliability
- DB design might need more partitioning schemas, etc. if # of reports is expected to grow dramatically

Specifically addressing the bottlenecks of the current approach: 
1) On startup, each file is being individually processed and then uploaded to the database. This can be very slow if there are a large number of records.  A bulk or batch operation, e.g. bulk copy to database, could be a more efficient solution in that case. We would have to redesign the data pipeline to integrate these steps, e.g. using Apache Spark/Hadoop for large-scale batch processing. 
2) Jupyter  Notebook based solutions, while easy and accessible to a broad audience, is not a great tool for visualization of data by entire organization. Creating a stand-alone graphical user interface (based on PyQt, for example) or a web-based solution using Plotly/Dash is more helpful/extensible. 
3) The Watchdog script must be configured to run as a daemon in the background so that people don't accidentally close it--in its current form, a separate Python shell must remain open to keep the watchdog running. A qualified replacement would a program such as Apache Airflow or an hourly CRON job, depending on the company's needs.
4) For the Python implementation itself, aggressive testing is needed to ensure minimal errors in a production environment. This could be, for example, unit and integration testing for all the various functionalities. 

What would a possible solution design look like?

The design would depend on answers to questions raised in the first part, of course. But if we anticipate a small or medium-sized company generating ~10 GB of data per day across the various measurements, the following ETL pipeline might be considered:

On a dedicated computer or a small cluster of 2-4 computers: 

1) Run an Apache Spark script to batch-process the historical data first and enter it into a PostgreSQL database. Relational databases here make a lot of sense. Moreover reports coming from a laboratory typically contain consistent formats and information. Therefore, batch processing is amenable and efficient. 

2) Build a Plotly/Dash or Tableau based visualization and analysis web-application that is accessible to the users in the company's intranet with potential extension to off-site access. 

3) Setup Apache Airflow to monitor new entries every hour (or a user-defined interval) and process them to be entered into the database. 

Within this ingestion pipeline, we can further make the following improvements to the prototyped solution here: 
- write generalized scripts, e.g. an object-oriented programming approach, to organize reports, named LabReport, with each report type as child class that provides modular extensions of the current code, more agility to handle various data sets, and easy/efficient handling. 
- package the entire code as an application
- create a user interface to configure settings for the scripts. 
- run watchdog as a deamon in the background. 
- ensure comprehensive logging throughout the pipeline to identify errors, warnings, incompatibilities to aid in quick fixes

How would you manage the project implementation?

Stage one would be to gather requirements from the customer and come up with design.

Stage two would be to show a mock up design to the customer to get their approval.

Stage three after consolidating, design a pipeline and perform tests to check integrity.

Stage four would be deployment and make any modifications if required
maintain the system.  

Constant monitoring of the operational logs and user feedback would enable corrections and adjustments to the aforementioned implementation. 
