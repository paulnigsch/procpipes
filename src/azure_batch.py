# azure batch related functions for managing tasks

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
import os
import sys
import datetime 
import time
import logging


# stollen from github example
def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if (batch_exception.error and batch_exception.error.message and
            batch_exception.error.message.value):
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')

def createBlobClient(account_name, account_key):

    blob_client = azureblob.BlockBlobService(
        account_name=account_name,
        account_key=account_key)

    return blob_client


def createContainer(blob_client, container_name):
    return blob_client.create_container(container_name, fail_on_exist=False)

def upload_file_to_container(block_blob_client, container_name, file_path):
    """
    Uploads a local file to an Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """

    blob_name = os.path.basename(file_path)

    logging.info('Uploading file {} to container [{}]...'.format(file_path, container_name))

    block_blob_client.create_blob_from_path(container_name,
                                            blob_name,
                                            file_path)

    sas_token = block_blob_client.generate_blob_shared_access_signature(
        container_name,
        blob_name,
        permission=azureblob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    sas_url = block_blob_client.make_blob_url(container_name, blob_name, sas_token=sas_token)

    return batchmodels.ResourceFile(file_path=blob_name, blob_source=sas_url)


def download_blobs_from_container(block_blob_client, container_name, directory_path):
    """
    Downloads all blobs from the specified Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param container_name: The Azure Blob storage container from which to
     download files.
    :param directory_path: The local directory to which to download the files.
    """
    logging.info('Downloading all files from container [{}]...'.format(container_name))

    container_blobs = block_blob_client.list_blobs(container_name)

    for blob in container_blobs.items:
        destination_file_path = os.path.join(directory_path, blob.name)

        block_blob_client.get_blob_to_path(container_name,
                                           blob.name,
                                           destination_file_path)

        logging.info('Downloaded blob [{}] from container [{}] to {}'.format(
            blob.name,
            container_name,
            destination_file_path))

    logging.info('Download complete!')


def createBatchClient(account_name, account_key, account_url):
     ## batch stuff
    credentials = batchauth.SharedKeyCredentials(account_name, account_key)
    batch_client = batch.BatchServiceClient( credentials, base_url=account_url)
    return batch_client, credentials


def createUbuntu1604Pool(batch_client, pool_id, startup_shell_cmds, resource_files,
        node_dedicated_count=0, node_low_prio_count=1, node_VM_size='STANDARD_A1', vm_image_id = None):

    if startup_shell_cmds == str:

        if startup_shell_cmds == "":
            startup_shell_cmds = "echo"

        startup_shell_cmds = [startup_shell_cmds]

    logging.info("creating pool")
    user = batchmodels.AutoUserSpecification(
        scope=batchmodels.AutoUserScope.pool,
        elevation_level=batchmodels.ElevationLevel.admin)

    full_command = '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format( ';'.join(startup_shell_cmds) )

    start_task = batch.models.StartTask(
        command_line=full_command,
        user_identity=batchmodels.UserIdentity(auto_user=user),
        wait_for_success=True,
        resource_files=resource_files)

    # vm image config 
    if vm_image_id == None:
        for i in  batch_client.account.list_node_agent_skus():
            if i.id == 'batch.node.ubuntu 16.04' :
                sku_to_use = i.id
                image_ref_to_use=  i.verified_image_references
                    
        vm_configuration = batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref_to_use[0],
            node_agent_sku_id=sku_to_use)
    else:
        print("using custom image: %s" % vm_image_id)

        custom_image = batchmodels.ImageReference(
            offer='UbuntuServer',
            publisher='Canonical',
            sku='16.04-LTS',
            version='latest',
            virtual_machine_image_id=vm_image_id,
        )
        print(custom_image)

        vm_configuration = batchmodels.VirtualMachineConfiguration(
            node_agent_sku_id = 'batch.node.ubuntu 16.04',
            image_reference = custom_image)

    # gather pool params
    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=vm_configuration,
        vm_size=node_VM_size,
        enable_auto_scale = False,
        target_dedicated_nodes= node_dedicated_count,
        target_low_priority_nodes  = node_low_prio_count,
        start_task=start_task
    )

    batch_client.pool.add(new_pool)
    logging.info("pool creation finished")


def createJob(batch_client, job_id, pool_id):
    job = batch.models.JobAddParameter(job_id,batch.models.PoolInformation(pool_id=pool_id))

    try:
        batch_client.job.add(job)
    except batchmodels.batch_error.BatchErrorException as err:
        print_batch_exception(err)
        raise

def createTask(batch_client, task_id, job_id, task_command, job_resource_files ):
    
    if type( task_command ) == str:
        task_command = (task_command, )

    full_command = '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format( ';'.join(task_command) )
    logging.info( "create task %s; command  '%s' " % (task_id, full_command))

    #container_settings = batch.models.TaskContainerSettings(image_name='someimage')
    
    task_definition = batch.models.TaskAddParameter(
        task_id,
        full_command,
        resource_files=job_resource_files,
        #container_settings=container_settings
        )


    try:
        batch_client.task.add_collection(job_id, [task_definition,])
    except batchmodels.batch_error.BatchErrorException as err:
        print_batch_exception(err)
        raise



def waitForJobTasksToComplete(batch_service_client, job_id, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))
    

def waitForTasksToComplete(batch_client, job_id, task_id, timeout_sec):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """

    if timeout_sec <= 0:
        no_timeout = True
    else:
        no_timeout = False

    timeout_expiration = datetime.datetime.now() + datetime.timedelta(seconds=timeout_sec)

    # 
    while no_timeout or (datetime.datetime.now() < timeout_expiration):

        try:
            task = batch_client.task.get(job_id=job_id, task_id=task_id)
        except:
            raise RuntimeError("ERROR: task state could note be retrieved")

        if task.state == batchmodels.TaskState.completed:
            if task.execution_info.exit_code != 0:
                raise RuntimeError("Task failed with return value %i" % task.execution_info.exit_code)
    
            return
        else:
            time.sleep(10)
        
            


    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout_sec))
    