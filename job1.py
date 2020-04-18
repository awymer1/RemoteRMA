#!/usr/bin/anaconda3/bin/python

"""
Usage:

Description:

Options:
"""

__title__ = 'item_week_data_file_load_wk1.py'
__author__ = 'Mahesh Tirupati'
__date_created__ = '20190925'

#
# Importing external modules
#

import sys
import os
import json
import csv
import shutil
import logging
import snowflake.connector as sfc

from datetime import datetime as dt, timedelta
from logging.handlers import RotatingFileHandler
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

#
# Set path and importing reusable modules
#

job_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = job_dir.split("/jobs")
parms_dir = parent_dir[0]+"/parms"
script_dir = parent_dir[0]+"/scripts"
curout_dir = parent_dir[0]+"/curout"
histout_dir = parent_dir[0]+"/histout"
module_dir = parent_dir[0]+"/modules"

path=sys.path.insert(0, module_dir)
from get_param import *
from initialize_logging import *
from already_running import *
from execute_sql import *
from get_results_of_last_query import *
from get_private_key import *
from connect_to_snowflake_db import *

path=sys.path.insert(0, script_dir)
from item_week_data_file_load_sql import *



#
# Set default Values
#

process_name = "ITEM_WEEK_DATA_FILE_LOAD_WK1"
parent_process_name = "ITEM_WEEK_DATA_FILE_LOAD"
sql_script_name ="item_week_data_file_load.py"
lock_file = '{0}/{1}.lock'.format(curout_dir, process_name)
log_file = '{0}/{1}.log'.format(curout_dir, process_name)


#
# Parameter File reading
#

path=sys.path.insert(0, parms_dir)

with open('{0}/parms.json'.format(parms_dir)) as parms:
    parameters = json.load(parms)

#
# Initialize Logging and log default values and parameter
#


debug_mode = get_param(parameters,parent_process_name,process_name, 'debug_mode')
initialize_logging(log_file,curout_dir,process_name,debug_mode)
logger = logging.getLogger(__name__)


logger.info('Job executon started : ' + dt.now().strftime('%m_%d_%Y_%H_%M_%S'))
logger.info('Directory Details:')
logger.info('Job Directory:{0}'.format(job_dir))
logger.info('Parms Directory:{0}'.format(parms_dir))
logger.info('Script Directory:{0}'.format(script_dir))
logger.info('Curout Directory:{0}'.format(curout_dir))
logger.info('Histout Directory:{0}'.format(histout_dir))
logger.info('Module Directory:{0}'.format(module_dir))


sf_account = get_param(parameters,parent_process_name, process_name, 'snowflake_account')
sf_user = get_param(parameters,parent_process_name, process_name, 'user_id')
key_file = get_param(parameters,parent_process_name, process_name, 'key_file')
pkb = get_private_key(key_file)
sf_role = get_param(parameters,parent_process_name, process_name, 'snowflake_role')
sf_warehouse = get_param(parameters,parent_process_name, process_name, 'snowflake_warehouse')
sf_conn_db = get_param(parameters,parent_process_name, process_name, 'snowflake_conn_db')
v_db_owner_d_mer_stg = get_param(parameters,parent_process_name,process_name, 'db_owner_d_mer_stg')
v_db_owner_d_mer_ods = get_param(parameters,parent_process_name, process_name, 'db_owner_d_mer_ods')
v_schema_owner_xrbia_dm = get_param(parameters,parent_process_name, process_name, 'schema_owner_xrbia_dm')
v_schema_owner_jcprms = get_param(parameters,parent_process_name, process_name, 'schema_owner_jcprms')
v_aws_internal_stage_path = get_param(parameters,parent_process_name, process_name, 'aws_internal_stage_path')
v_file_path = get_param(parameters,parent_process_name, process_name, 'file_path')
v_file_pattern_wk1 = get_param(parameters,parent_process_name, process_name, 'file_pattern_wk1')
v_file_format  = get_param(parameters,parent_process_name, process_name, 'file_format')
v_skip_lines  = get_param(parameters,parent_process_name, process_name, 'skip_lines')
v_on_error  = get_param(parameters,parent_process_name, process_name, 'on_error')
v_purge_flg = get_param(parameters,parent_process_name, process_name, 'purge_flg')



#
# Check for lock file in curout directory.  If found, exit, else create one.
#

if already_running(lock_file):
    exit()


#
# Connect to Database for Main Transaction
#


connection = connect_to_snowflake_db(sf_account,sf_user,pkb,sf_role,sf_warehouse,sf_conn_db)

logger.info('Connected to snowflake database for main transaction')
logger.info('Account: {0}, User: {1}, Role: {2}, Warehouse: {3}, Database: {4}'.format(sf_account, sf_user, sf_role, sf_warehouse, sf_conn_db))
logger.info('Read data from sql script:  ' + sql_script_name + ' from path: ' + script_dir)

#
# CLEAR FILES IN SNOWFLAKE INTERNAL STAGE
#

clear_aws_internal_stage = clear_aws_internal_stage_sql.format(aws_internal_stage_path=v_aws_internal_stage_path)
logger.info('command to be executed for clearing previous files in internal stage: ' + clear_aws_internal_stage)
execute_sql(clear_aws_internal_stage, connection, False, 'DML', 'Execute REMOVE')



#
# TRUNCATE DATA FROM STAGE TABLE
#

truncate_query = sql_truncate.format(db_owner_d_mer_stg=v_db_owner_d_mer_stg,db_owner_d_mer_ods=v_db_owner_d_mer_ods,schema_owner_xrbia_dm=v_schema_owner_xrbia_dm,schema_owner_jcprms=v_schema_owner_jcprms)
logger.info('Truncate Query to be executed: ' + truncate_query)
logger.info('Truncate Query execution : start')
execute_sql(truncate_query, connection, False, 'DML', 'Execute Truncate SQL')
logger.info('Truncate Query execution : complete')


#
# PUT FILES INTO SNOWFLAKE INTERNAL STAGE
#

put_data_into_aws = put_data_into_aws_sql.format(aws_internal_stage_path=v_aws_internal_stage_path,file_path=v_file_path,file_pattern=v_file_pattern_wk1)
logger.info('PUT command to be executed: ' + put_data_into_aws)
execute_sql(put_data_into_aws, connection, False, 'DML', 'Execute PUT')

#
# COPY DATA FROM SNOWFLAKE INTERNAL STAGE TO SNOWFLAKE DATABASE
#

copy_data_into_stage_table = copy_data_into_stage_table_sql.format(db_owner_d_mer_stg=v_db_owner_d_mer_stg,schema_owner_jcprms=v_schema_owner_jcprms,aws_internal_stage_path=v_aws_internal_stage_path,file_format=v_file_format,skip_lines=v_skip_lines,on_error=v_on_error,purge_flg=v_purge_flg)
logger.info('COPY command to be executed: ' + copy_data_into_stage_table)
execute_sql(copy_data_into_stage_table, connection, False, 'DML', 'Execute COPY')

total_files = 0
total_copied = 0

sql_result = get_results_of_last_query()
copy_results = execute_sql(sql_result, connection, False, 'FETCHALL', 'Execute Get Results SQL')
if copy_results[0][0] != 'Copy executed with 0 files processed.':
        for copied_file in set(copy_results):
               # if copied_file[1] == 'LOAD_SKIPPED':
               #     logger.info('File: {0} Status:{1} Rows:{2} Rows Loaded:{3}'.format(copied_file[0], copied_file[1], copied_file[2], copied_file[3]))
               # else:
                    logger.info('File: {0} Status:{1} Rows:{2} Rows Loaded:{3}'.format(copied_file[0], copied_file[1], copied_file[2], copied_file[3]))
                    total_files += 1
                    total_copied += copied_file[3]
logger.info('No of files copied: ' + str(total_files))
logger.info('No of total rows loaded: ' + str(total_copied))

#
# DELETE DATA FROM MAIN TABLE
#

logger.info('Begin start')
execute_sql('BEGIN', connection, False, 'DML', 'Begin Transaction')
logger.info('Begin end')
delete_query = sql_delete.format(db_owner_d_mer_stg=v_db_owner_d_mer_stg,db_owner_d_mer_ods=v_db_owner_d_mer_ods,schema_owner_xrbia_dm=v_schema_owner_xrbia_dm,schema_owner_jcprms=v_schema_owner_jcprms)
logger.info('Delete Query to be executed: ' + delete_query)
logger.info('Delete Query execution : start')
execute_sql(delete_query, connection, False, 'DML', 'Execute Delete SQL')
logger.info('Delete Query execution : complete')

#
# Get the run status of the query
#

sql_result = get_results_of_last_query()
affected_rows = execute_sql(sql_result, connection, False, 'FETCHALL', 'Execute Get Results SQL')
logger.info('Affected rows:{0}'.format(affected_rows[0][0]))
logger.info('Commit start')
execute_sql('COMMIT', connection, False, 'DML', 'Commit Transaction')
logger.info('Commit complete')


#
# INSERT DATA INTO MAIN TABLE
#

logger.info('Begin start')
execute_sql('BEGIN', connection, False, 'DML', 'Begin Transaction')
logger.info('Begin end')
insert_query = sql_insert.format(db_owner_d_mer_stg=v_db_owner_d_mer_stg,db_owner_d_mer_ods=v_db_owner_d_mer_ods,schema_owner_xrbia_dm=v_schema_owner_xrbia_dm,schema_owner_jcprms=v_schema_owner_jcprms)
logger.info('Insert Query to be executed: ' + insert_query)
logger.info('Insert Query execution : start')
execute_sql(insert_query, connection, False, 'DML', 'Execute Insert SQL')
logger.info('Insert Query execution : complete')

#
# Get the run status of the query
#

sql_result = get_results_of_last_query()
affected_rows = execute_sql(sql_result, connection, False, 'FETCHALL', 'Execute Get Results SQL')
logger.info('Affected rows:{0}'.format(affected_rows[0][0]))
logger.info('Commit start')
execute_sql('COMMIT', connection, False, 'DML', 'Commit Transaction')
logger.info('Commit complete')


v_list_dir_files = os.listdir(v_file_path)
for file in v_list_dir_files:
 # logger.info('file processing:' + str(file))
  if file.startswith(v_file_pattern_wk1):
     os.remove(os.path.join(v_file_path, file))
     logger.info('file deleted:'+ str(file)+' from path:' + str(v_file_path))

#
# Close Database Connection, arhcive the log file to histout and remove lock file
#

archive_log_file=(histout_dir + '/{0}.log'.format( process_name) + dt.now().strftime('%m_%d_%Y_%H_%M_%S'))

shutil.move(log_file,archive_log_file)

logger.info('Log file : ' + log_file + ' moved to path ' + histout_dir)

logger.info('Job execution completed : ' + dt.now().strftime('%m_%d_%Y_%H_%M_%S'))

os.remove(lock_file)

connection.close()

