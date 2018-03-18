

import unittest
import logging
import os,sys,inspect
import operator, functools
import hashlib
import os.path
import datetime
import time

sys.path.insert(1, os.path.join(sys.path[0], '..'))
from azure_batch import *

if os.path.isfile(os.path.dirname(__file__) + '/azure_batch.config_user'):
    print("reading user configuration file")
    azure_config.read(os.path.dirname(__file__) + '/azure_batch.config_user')
else:
    print("reading default configuration file")
    azure_config.read(os.path.dirname(__file__) + '/azure_batch.config')



#### storage option
STORAGE_ACCOUNT_NAME = azure_config['STORAGE']['STORAGE_ACCOUNT_NAME']
STORAGE_ACCOUNT_KEY = azure_config['STORAGE']['STORAGE_ACCOUNT_KEY']

CONTAINER_NAME_EXEC = "testexecutables"
CONTAINER_NAME_DATA = "testdata"
CONTAINER_NAME_OUTPUT = "output"

#### BATCH options
BATCH_ACCOUNT_NAME = azure_config['BATCH']['BATCH_ACCOUNT_NAME']
BATCH_ACCOUNT_KEY = azure_config['BATCH']['BATCH_ACCOUNT_KEY']
BATCH_ACCOUNT_URL = azure_config['BATCH']['BATCH_ACCOUNT_URL']

POOL_ID = "testPoolV3"
JOB_ID = "testJobV3"
TASK_ID = "testTaskV3"

class TestAzureBatchLogic(unittest.TestCase):


    def test_azure_blob_upload_download(self):

        blob_client = createBlobClient(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)

        # create container
        res = createContainer(blob_client, CONTAINER_NAME_EXEC)
        res = createContainer(blob_client, CONTAINER_NAME_DATA)

        # upload
        lorem_ipsum_res_file = upload_file_to_container(blob_client, CONTAINER_NAME_DATA, 'data/lorem_ipsum.txt')

        # create tmp dir
        tmpdir = 'testout'
        if not os.path.exists(tmpdir):
            os.mkdir(tmpdir)

        # download
        download_blobs_from_container(blob_client, CONTAINER_NAME_DATA, tmpdir)


        # verify integrity
        test_file =  tmpdir + '/lorem_ipsum.txt'
        with open( test_file, 'rb') as f:
            hash_file = hashlib.md5(f.read()).hexdigest()

        self.assertEqual(hash_file, 'db89bb5ceab87f9c0fcc2ab36c189c2c')

        # cleanup
        if os.path.exists(test_file):
            os.remove(test_file)
        
        os.rmdir("testout")


    def test_azure_batch(self):

        blob_client = createBlobClient(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)

        # create container
        res = createContainer(blob_client, CONTAINER_NAME_EXEC)
        res = createContainer(blob_client, CONTAINER_NAME_DATA)
        res = createContainer(blob_client, CONTAINER_NAME_OUTPUT)

        # upload
        lorem_ipsum_res_file = upload_file_to_container(blob_client, CONTAINER_NAME_DATA, 'data/lorem_ipsum.txt')
        copy_to_blob_res_file = upload_file_to_container(blob_client, CONTAINER_NAME_EXEC, '../azure_copy_to_blob.py')



        batch_client, _ = createBatchClient(BATCH_ACCOUNT_NAME, BATCH_ACCOUNT_KEY, BATCH_ACCOUNT_URL)

        ######################################################################3
        ###### POOL CREATIO
        startup_shell_cmds = [
            # copy the blob copy file
            'cp -p {} $AZ_BATCH_NODE_SHARED_DIR'.format(copy_to_blob_res_file.file_path),
            # Install pip
            'curl -fSsL https://bootstrap.pypa.io/get-pip.py | python',
            'pip install azure-storage==0.32.0']
        

        start_up_resource_files = [copy_to_blob_res_file]
        node_dedicated_count=0
        node_low_prio_count=1
        node_VM_size='STANDARD_A1'


        # check if the pool already exists
        found_pool = batch_client.pool.exists(POOL_ID)

        if not found_pool:
            createUbuntu1604Pool(batch_client, POOL_ID, startup_shell_cmds,
                                start_up_resource_files,  node_dedicated_count, 
                                node_low_prio_count, node_VM_size)

        # check if creation succeeded        
        self.assertTrue(batch_client.pool.exists(POOL_ID))

        ######################################################################
        ##### create Job

       
        create_job = JOB_ID not in [ j.id for j in batch_client.job.list()] 

        if create_job:
            createJob(batch_client, JOB_ID, POOL_ID)

        found_job = JOB_ID  in [ j.id for j in batch_client.job.list()] 
        self.assertTrue(found_job)

        ######################################################################
        #### create Task

        # check if exists ... task are not updated/overwritten!
        for j in batch_client.task.list(JOB_ID):
            self.assertNotEqual(j.id,TASK_ID )


        task_commands = [\
                         "echo helloWorld1 > ${AZ_BATCH_TASK_WORKING_DIR}/lorem_ipsum.txt",
                        'python ${AZ_BATCH_NODE_SHARED_DIR}/azure_copy_to_blob.py --outfile  ${AZ_BATCH_TASK_WORKING_DIR}/lorem_ipsum.txt --storageaccount %s --storagecontainer %s --account_key %s' % (STORAGE_ACCOUNT_NAME, CONTAINER_NAME_OUTPUT, STORAGE_ACCOUNT_KEY)
                         ]

        task_resource_files = [lorem_ipsum_res_file]
        createTask(batch_client, TASK_ID, JOB_ID, task_commands, task_resource_files )

        found_task = False
        for j in batch_client.task.list(JOB_ID):
            if j.id == TASK_ID:
                found_task = True        
                break

            
        self.assertTrue(found_task)


        print("wait for task to finish")
        waitForTasksToComplete(batch_client, JOB_ID, TASK_ID, 120)

        tmpdir = "tmp"
        if not os.path.exists(tmpdir):
            os.mkdir(tmpdir)
        download_blobs_from_container(blob_client, CONTAINER_NAME_OUTPUT, tmpdir)

        with open( tmpdir + "/lorem_ipsum.txt") as f:
            lines = f.readline()
            self.assertEqual(lines, "helloWorld1\n")


        ### verify results


if __name__ == '__main__':
    raise RuntimeError("this test is disable")
    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)
    unittest.main()