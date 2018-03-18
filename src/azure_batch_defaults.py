

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

DOCKER_VM_IMAGE = azure_config['BATCH']['DOCKER_VM_IMAGE']

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
        'chown _azbatch:_azbatchgrp /var/run/docker.sock;',
        'ls -l /var/run/docker.sock;',
        'docker login --username %s --password %s %s' %(azure_config['DOCKER']['username'], azure_config['DOCKER']['password'], azure_config['DOCKER']['registry'], ),
        'docker pull %s' % azure_config['DOCKER']['image'], 
        'echo "node for pool %s created"' % POOL_ID,
        ]

   
    #node_dedicated_count= 0
    #node_low_prio_count= 1
    #node_VM_size='STANDARD_A1'

    # check if the pool already exists
    found_pool = batch_client.pool.exists(POOL_ID)

    if not found_pool:
        #createUbuntu1604Pool(batch_client, POOL_ID, startup_shell_cmds, exec_file_res, vm_image_id = DOCKER_VM_IMAGE)
        raise RuntimeError('There is no POOL with the specified ID. Automatic Pool creating is not supported at the moment. please use an existing pool')
    else:
        logging.warning("there is already a pool with pool_id %s running ... hope that it is already properly set up" % POOL_ID)

        
    
    ##### JOB creation

       
    create_job = JOB_ID not in [ j.id for j in batch_client.job.list()] 

    if create_job:
        createJob(batch_client, JOB_ID, POOL_ID)
    else:
        logging.warning("job already exists")


    return batch_client


