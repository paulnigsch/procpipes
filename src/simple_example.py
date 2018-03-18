

import unittest
import logging
import os,sys,inspect
import operator, functools
import hashlib
import os.path
import datetime
import time

from azure_batch import *


import configparser
azure_config = configparser.ConfigParser()
if os.path.isfile(os.path.dirname(__file__) + '/azure_batch.config_user'):
    print("reading user configuration file")
    azure_config.read(os.path.dirname(__file__) + '/azure_batch.config_user')
else:
    print("reading default configuration file")
    azure_config.read(os.path.dirname(__file__) + '/azure_batch.config')


#### storage option
STORAGE_ACCOUNT_NAME = azure_config['STORAGE']['STORAGE_ACCOUNT_NAME']
STORAGE_ACCOUNT_KEY = azure_config['STORAGE']['STORAGE_ACCOUNT_KEY']

CONTAINER_NAME_EXEC = azure_config['STORAGE']['CONTAINER_NAME_EXEC']
CONTAINER_NAME_INPUT = azure_config['STORAGE']['CONTAINER_NAME_INPUT']
CONTAINER_NAME_OUTPUT = azure_config['STORAGE']['CONTAINER_NAME_OUTPUT']

#### BATCH options
BATCH_ACCOUNT_NAME = azure_config['BATCH']['BATCH_ACCOUNT_NAME']
BATCH_ACCOUNT_KEY = azure_config['BATCH']['BATCH_ACCOUNT_KEY']
BATCH_ACCOUNT_URL = azure_config['BATCH']['BATCH_ACCOUNT_URL']

POOL_ID = azure_config['BATCH']['POOL_ID']
JOB_ID = azure_config['BATCH']['JOB_ID']
TASK_ID_PREFIX = azure_config['BATCH']['TASK_ID_PREFIX']

def uploadFiles(input_files, exec_file ):
    blob_client = createBlobClient(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)

    # create container
    createContainer(blob_client, CONTAINER_NAME_EXEC)
    createContainer(blob_client, CONTAINER_NAME_INPUT)
    createContainer(blob_client, CONTAINER_NAME_OUTPUT)

    # upload
    input_files_res = []
    for f in input_files:
        res_file = upload_file_to_container(blob_client, CONTAINER_NAME_INPUT,f)
        input_files_res.append(res_file)
    
    exec_file_res = []
    for f in exec_file:
        res_file = upload_file_to_container(blob_client, CONTAINER_NAME_EXEC,f)
        exec_file_res.append(res_file)
    
    return blob_client, input_files_res, exec_file_res

def setupBatchPoolsAndJobs(exec_file_res):

    batch_client, _ = createBatchClient(BATCH_ACCOUNT_NAME, BATCH_ACCOUNT_KEY, BATCH_ACCOUNT_URL)

    ###### POOL CREATION
    startup_shell_cmds = [
        # copy all execution resource files
        'cp -p {} $AZ_BATCH_NODE_SHARED_DIR'.format(" ".join( [ f.file_path for f in exec_file_res]  )),
        # Install pip
        'curl -fSsL https://bootstrap.pypa.io/get-pip.py | python',
        'pip install azure-storage==0.32.0'
        ]

   
    #node_dedicated_count= 0
    #node_low_prio_count= 1
    #node_VM_size='STANDARD_A1'

    # check if the pool already exists
    found_pool = batch_client.pool.exists(POOL_ID)

    if not found_pool:
        createUbuntu1604Pool(batch_client, POOL_ID, startup_shell_cmds, exec_file_res)

    else:
        logging.warning("there is already a pool with pool_id %s running ... hope that it is already properly set up" % POOL_ID)

        
    
    ##### JOB creation

       
    create_job = JOB_ID not in [ j.id for j in batch_client.job.list()] 

    if create_job:
        createJob(batch_client, JOB_ID, POOL_ID)
    else:
        logging.warning("job already exists")

    return batch_client


def main():
    
    ######################################################################
    # storage
    input_files = ['test/data/lorem_ipsum.txt']
    exec_files = [ "azure_copy_to_blob.py"]
    
    blob_client, input_files_res, exec_file_res = uploadFiles(input_files, exec_files)


    ######################################################################
    # batch Pool and Job setup
    batch_client = setupBatchPoolsAndJobs(exec_file_res)

    
    ######################################################################
    #### create Task

    TASK_ID = TASK_ID_PREFIX + str(int( time.time()))

    # check if tasks exists ... task are not updated/overwritten!
    for t in batch_client.task.list(JOB_ID):
        if  t.id == TASK_ID:
            logging.fatal("there exsits already a task %s in job %s" %(TASK_ID, JOB_ID))
            assert( t.id !=TASK_ID)


    res_file_exec = upload_file_to_container(blob_client, CONTAINER_NAME_EXEC,"simple_example_exec.py")
  
    task_commands = [\
        'echo "task start"',
        'python ${AZ_BATCH_TASK_WORKING_DIR}/simple_example_exec.py --inputfile lorem_ipsum.txt --outputfile results.txt',
        'python ${AZ_BATCH_TASK_WORKING_DIR}/azure_copy_to_blob.py --outfile  ${AZ_BATCH_TASK_WORKING_DIR}/results.txt --storageaccount %s --storagecontainer %s --account_key %s' % (STORAGE_ACCOUNT_NAME, CONTAINER_NAME_OUTPUT, STORAGE_ACCOUNT_KEY),
        'echo "task end"'
        ]

    task_resource_files = input_files_res +  exec_file_res  + [ res_file_exec] 
    createTask(batch_client, TASK_ID, JOB_ID, task_commands, task_resource_files )

    ######################################################################
    #### waiting
    logger.info("wait for task to finish")
    waitForTasksToComplete(batch_client, JOB_ID, TASK_ID, 0)

     ######################################################################
     #### retrieving results
    tmpdir = "output"
    if not os.path.exists(tmpdir):
        os.mkdir(tmpdir)
    
    download_blobs_from_container(blob_client, CONTAINER_NAME_OUTPUT, tmpdir)

    logger.info("finished downloading")

    print("==========================\ncontent of results.txt")
    with open( tmpdir + "/results.txt") as f:
        lines = f.read()
        print(lines)

if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()
