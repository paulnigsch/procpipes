import logging
import time
import dill
import pickle
import os

import azure_batch  as ab

from azure_batch_defaults import uploadFiles, setupBatchPoolsAndJobs, createTask, JOB_ID, STORAGE_ACCOUNT_NAME, CONTAINER_NAME_OUTPUT, STORAGE_ACCOUNT_KEY, waitForTasksToComplete

from azure_batch_defaults import azure_config

class TaskFunct(object):
    def __init__(self, f, id, parent_id, description, dump_fct, runOnAzure, input_files, output_files):
        self.proc_id = id
        self.parent_id = parent_id
        self.f = f
        self.desc = description
        self.dump_fct = dump_fct
        self.runOnAzure = runOnAzure
        self.input_files = input_files
        self.output_files = output_files


    def execute(self, input_data):
        """
        standard calc
        """
        logging.info("STARTING calc: %s" % self.desc)
        start_time = time.time()
        retval =  self.f(input_data)
        end_time = time.time()
        logging.info("   FINISHED calc: %s (runtime %f sec)" % (self.desc, end_time - start_time) )
        return retval

    def serializeFunction(self):
        import tempfile

        temp_file = tempfile.NamedTemporaryFile(delete=False)
        dill.dump(self.f, temp_file)
        temp_file.close()

        logging.info("serialized function: %s", temp_file.name)

        print("serialized function: %s" % temp_file.name)
        

        #os.unlink(f.name)
        #os.path.exists(f.name)
        
        return temp_file.name

    def checkFileExists(self, files):
        for f in files:
            if not os.path.exists(f):
                raise RuntimeError("input file %s does not exist" % f)

    def executeOnAzure(self, input_data):
        """

        """
        print("----------------------------------")
        print("running task on azure")
       
        # serialize functions
        ser_file_name = self.serializeFunction()


        ######################################################################
        # storage

        input_file = "input.pickle"
        output_file = "output.pickle"

        with open(input_file, 'wb') as f:
            pickle.dump(input_data, f)

      
        base_path = os.path.dirname(__file__)
        exec_files = [os.path.realpath(base_path + '/task_azure_executor.py'), os.path.realpath(base_path +  '/azure_copy_to_blob.py'), ser_file_name]

        blob_client, input_files_res, exec_file_res = uploadFiles([input_file] + self.input_files, exec_files)

        batch_client = setupBatchPoolsAndJobs(exec_file_res)

        TASK_ID = "task" + str(int( time.time()))
        
        print(TASK_ID)
        print("----------------------------------")

        
        docker_command = 'python task_azure_executor.py --task_binary %s --datainput %s --dataoutput %s' %(os.path.basename(ser_file_name), input_file, output_file)
        docker_image = azure_config['DOCKER']['image']
        def execCmdInDocker( cmd, docker_image = docker_image):
            return 'docker run -v ${AZ_BATCH_TASK_WORKING_DIR}:/wd -w /wd %s %s' % (docker_image, cmd)


        upload_output_cmd = 'python ${AZ_BATCH_TASK_WORKING_DIR}/azure_copy_to_blob.py --outfile  %s --storageaccount %s --storagecontainer %s --account_key %s' % (output_file ,STORAGE_ACCOUNT_NAME, CONTAINER_NAME_OUTPUT, STORAGE_ACCOUNT_KEY),

        task_commands = [\
            'echo "task start; user: $USER"',
            'docker login --username %s --password %s %s' %(azure_config['DOCKER']['username'], azure_config['DOCKER']['password'], azure_config['DOCKER']['registry'], ),
            'docker pull %s' % docker_image, 
            execCmdInDocker(docker_command),
            'echo "task end, uploading files to output"',
            execCmdInDocker('python azure_copy_to_blob.py  --storageaccount %s --storagecontainer %s --account_key %s output.pickle %s' % (STORAGE_ACCOUNT_NAME, CONTAINER_NAME_OUTPUT, STORAGE_ACCOUNT_KEY, " ".join(self.output_files) )),
        ]


        task_resource_files = input_files_res +  exec_file_res 

        createTask(batch_client, TASK_ID, JOB_ID, task_commands, task_resource_files )

        waitForTasksToComplete(batch_client, JOB_ID, TASK_ID, 0)

        #### retrieving results
        tmpdir = "output"
        if not os.path.exists(tmpdir):
            os.mkdir(tmpdir)

        ab.download_blobs_from_container(blob_client, CONTAINER_NAME_OUTPUT, tmpdir)

        
        with open( tmpdir + '/' + output_file, 'rb') as f:
            res = pickle.load(f)

        return res


    def __call__(self, input_data):

        if self.runOnAzure:
           retval = self.executeOnAzure(input_data)

        else:
            
            self.checkFileExists(self.input_files)

            retval = self.execute(input_data)
            
            if self.dump_fct != False:
                self.dump_fct(retval)

            self.checkFileExists(self.output_files)

        return retval



class Task(object):
    # the decorator element
    def __init__(self, id, parent, dump_fct=False, input_files = [], output_files = [], description =  "", runOnAzure=False):
        self.proc_id = id 
        self.parent = parent
        self.desc = description
        self.f = None
        self.dump_fct = dump_fct
        self.input_files = input_files
        self.output_files = output_files
        self.runOnAzure = runOnAzure



    def __call__(self, f):
        self.f = f

        return TaskFunct(f, self.proc_id, self.parent, self.desc, self.dump_fct, self.runOnAzure, self.input_files, self.output_files)
        

def runPipelineSeqenced(tasks_sequence):
    # execute seqential
    in_data = None
    for t in tasks_sequence:
        out_data = t(in_data)

        in_data = out_data

    return out_data
