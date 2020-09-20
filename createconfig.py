# -*- coding: utf-8 -*-
"""

"""

import configparser

config = configparser.ConfigParser()

config['FolderPath'] = {'path':'C:/Users/chema/Desktop/test/'}

config['PostgresDB'] = {'db_name':'de_chall',
                        'host': 'localhost',
                        'port':'5432',                       
                        'user': 'ak_user',
                        'pw':'ak_password'
                        }

config['PostgresTables'] = {'hall_table': 'hall_measurement',
                            'icp_table': 'icp_measurement'}

config['logfile'] = {'log_filename':'lab_update.log'}

with open('config.ini', 'w') as configfile:
 config.write(configfile)