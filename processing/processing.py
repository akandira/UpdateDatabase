"""
This scripts contains helper functions that help process lab data and insert it 
to database.

@author: Anvitha Kandiraju
"""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Float, String, MetaData, Boolean
import fnmatch
import logging
import configparser

# parse configuration file to get parameters
config = configparser.ConfigParser()
config.sections()
config.read('../config.ini')

postgresql_dbname = config['PostgresDB']['db_name']
postgresql_host = config['PostgresDB']['host']
postgresql_port = config['PostgresDB']['port']
postgresql_user = config['PostgresDB']['user']
postgresql_pw = config['PostgresDB']['pw']

log_filename = config['logfile']['log_filename']
postgresql_hall_table = config['PostgresTables']['hall_table']
postgresql_icp_table = config['PostgresTables']['icp_table']

# database connection
engine_url = 'postgresql://{}:{}@{}:{}/{}'.format(postgresql_user,
        postgresql_pw, postgresql_host, postgresql_port,
        postgresql_dbname)

# log file format
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s  :%(levelname)s  :%(message)s')


def clean_df(df, colnames=['ID', 'Value']):
    """Strip white spaces from column names in Pandas DataFrame"""
    df[colnames[0]] = df[colnames[0]].str.strip()
    df[colnames[0]] = df[colnames[0]].str.replace(' ', '_')
    df[colnames[1]] = df[colnames[1]].str.strip()
    return df


def add_uniqueid(df, p_type,colnames=['ID', 'Value'],uid='material_uid'):
    """Add a unique ID for each entry based on Material ID"""
    uniqueid_name = p_type.lower() + '_uid'
    uniqueid_val = p_type + '-' + list(df.loc[df[colnames[0]] == uid,
            colnames[1]])[0]
    uniqueid_row = pd.DataFrame([[uniqueid_name, uniqueid_val]],
                                columns=colnames)
    df = df.append(uniqueid_row, ignore_index=True)
    return df


def add_processtype(df,colnames=['ID', 'Value'],uid='material_uid'):
    """Add process type of sample into DataFrame 
        processes can be BM: Ball Milling or HP: Hot Press"""

    id_name = 'process_type'
    
    if "BM" in list(df.loc[df[colnames[0]] == uid,colnames[1]])[0]:
        id_val = 'Ball milling'
    elif "HP" in list(df.loc[df[colnames[0]] == uid,colnames[1]])[0]:
        id_val = 'Hot process'
    else:
        id_val ='unknown'       
    processtypeid_row = pd.DataFrame([[id_name, id_val]],
                                columns=colnames)
    df = df.append(processtypeid_row, ignore_index=True)
    return df


def get_units(row_id, colnames=['ID', 'Value']):
    """Extract units from quantitative columns in DataFrame"""
    if '(' in row_id:
        row_id = row_id.split('(')
        units_id = row_id[0] + 'units'
        units_val = (row_id[1])[:-1]
        units_row = pd.DataFrame([[units_id, units_val]],
                                 columns=colnames)
        return units_row
    else:
        return None


def rename_row(row_id):
    """Rename rows to lowercase without units"""
    row_id = row_id.lower()
    if '(' in row_id:
        row_id = row_id.split('_(')[0]
    return row_id


def set_valuetype(dicts):
    """Assign numerical and boolean data proper data type"""
    for keys in dicts:
        if dicts[keys] == 'True':
            dicts[keys] = True
        else:
            try:
                dicts[keys] = float(dicts[keys])
            except:
                pass
    return dicts


def process_report(filepath, report_type, colnames=['ID', 'Value']):
    """Read data from text file and process it.
    This function does the following: 
        1) read from text file into a Pandas DataFrame
        2) cleans and organizes the data in the DataFrame
        3) assigns a process type to the lab report
        4) returns the Pandas DF"""


   # read lab report to data frame
    df = pd.read_table(filepath, engine='python', skiprows=2,
                       names=colnames, skipinitialspace=True)

   # strip additional space from data frame
    df = clean_df(df)

   # extract units from data
    units_rows = [get_units(x) for x in df[colnames[0]]]

   # append units to original data
    units_df = pd.concat(units_rows)
    df = df.append(units_df, ignore_index=True)

   # rename rows to remove units from columname and change to lower case
    df[colnames[0]] = df[colnames[0]].apply(lambda x: rename_row(x))

   # extract unique id or primary key for row
    df = add_uniqueid(df, report_type)
        
    # app process_type row
    df = add_processtype(df)

   # change dataframe to dict for easy conversion of
   # numerical values and boolean values to appropriate type
    data_dict = df.set_index(colnames[0])[colnames[1]].to_dict()
    set_valuetype(data_dict)

   # obtain processed dataframe
    df_processed = pd.DataFrame.from_dict([data_dict])
    return df_processed


def insert_hallreport(df_processed):
    """Insert Hall lab report into database.
       This function takes a prepared DF from process_report 
       pertaining to a Hall measurement and inserts that into
       a PostgreSQL table. """

    # create connection to sql database
    engine = create_engine(engine_url)

    # if table doesnot exist create a new table
    if not engine.has_table(postgresql_hall_table):
        meta = MetaData()
        Table(
            postgresql_hall_table,
            meta,
            Column('hall_uid', String(length=20), primary_key=True,
                   nullable=False),
            Column('material_uid', String(length=20)),
            Column('process_type', String(length=20)),
            Column('measurement', String(length=10)),
            Column('probe_resistance', Float),
            Column('gas_flow_rate', Float),
            Column('gas_type', String(length=10)),
            Column('probe_material', String(length=10)),
            Column('current', Float),
            Column('field_strength', Float),
            Column('sample_position', Float),
            Column('magnet_reversal', Boolean),
            Column('probe_resistance_units', String(length=10)),
            Column('gas_flow_rate_units', String(length=10)),
            Column('current_units', String(length=10)),
            Column('field_strength_units', String(length=10)),
            )
        meta.create_all(engine)

    # add processed dataframe to SQL database
    df_processed.to_sql(postgresql_hall_table, engine,
                        if_exists='append', index=False)

    # after adding the record to database, return unique id for logging
    hall_uid = df_processed.iloc[0]['hall_uid']
    return hall_uid


def insert_icpreport(df_processed):
    """Insert ICP lab report into database."""

    # create connection to sql database
    engine = create_engine(engine_url)

    # if table does not exist create a new table
    if not engine.has_table(postgresql_icp_table):
        meta = MetaData()
        Table(
            postgresql_icp_table,
            meta,
            Column('icp_uid', String(length=20), primary_key=True,
                   nullable=False),
            Column('material_uid', String(length=20)),
            Column('process_type', String(length=20)),
            Column('measurement', String(length=10)),
            Column('pb_concentration', Float),
            Column('sn_concentration', Float),
            Column('o_concentration', Float),
            Column('gas_flow_rate', Float),
            Column('gas_type', String(length=10)),
            Column('plasma_temperature', Float),
            Column('detector_temperature', Float),
            Column('field_strength', Float),
            Column('plasma_observation', String(length=10)),
            Column('radio_frequency', Float),
            Column('gas_flow_rate_units', String(length=10)),
            Column('plasma_temperature_units', String(length=10)),
            Column('detector_temperature_units', String(length=10)),
            Column('field_strength_units', String(length=10)),
            Column('radio_frequency_units', String(length=10)),
            )
        meta.create_all(engine)

    # add processed dataframe to SQL database
    df_processed.to_sql(postgresql_icp_table, engine, if_exists='append'
                        , index=False)

    # after adding the record to database, return unique id for logging
    icp_uid = df_processed.iloc[0]['icp_uid']
    return icp_uid


def reporttype_detect(path):
    """Determine type of record: Hall or ICP based on file prefix."""

    # obtain file name for processing
    filename = path.split('/')[-1]

    # log obtained file name for record
    logging.info('{} file received for processing'.format(filename))

    # detect if the report is Hall type or ICP type and call report handler function
    # log error if file format is not Hall or ICP type
    if fnmatch.fnmatch(filename, 'Hall-*.txt'):
        report_handler(path, filename, 'HALL')
    elif fnmatch.fnmatch(filename, 'ICP-*.txt'):
        report_handler(path, filename, 'ICP')
    else:        
        logging.error('cannot detect report type in file: {}'.format(filename))



def report_handler(path, filename, report_type):
    """Handle report based on type of measurement"""
   
     # try to process report to create a processed data frame
     # log info if the file is processed succefully.
     # If any exception arises in processing data record it in the log file     
    try:
        df_processed = process_report(path, report_type)       
        logging.info('{} processed succesfully'.format(filename))
    except Exception as processing_error:   
        logging.error('processing failed with error: {}'.format(processing_error))
        return

    # if report type is Hall insert into Hall table
    if report_type == 'HALL':

        # try insertion of processed data frame to hall table
        # log succesful insertion with uid
        # If exception arises in inserting the record log the error

        try:
            uid = insert_hallreport(df_processed)               
            logging.info('file inserted succesfully with record_uid: {}'.format(uid))
        except Exception as insertion_error:         
            logging.error('insertion failed: {}'.format(str(insertion_error)))

    # if report type is ICP insert into ICP table
    else:

        # try insertion of processed data frame to hall table
        # log succesful insertion with uid
        # If exception arrises in inserting the record log the error

        try:
            uid = insert_icpreport(df_processed)            
            logging.info('file inserted succesfully with record_uid: {}'.format(uid))
        except Exception as insertion_error:        
            logging.error('insertion failed: {}'.format(str(insertion_error)))
  



