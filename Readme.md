# Readme

Procpipe is a small (and at the moment rather stupid) python framework for runing sequential tasks. 
The main feature is that one can easily offload the tasks to the Microsoft Azure Batch service.

At the current state it is merely a case study for offloading python functions to Azure. Hence, the configuration is rather cumbersome at the moment (and errors are not good documented :) )!

The python code is serialized with the `dill` package, upload to Azure, executed and the results are retrieved.

## Usage

```python 
from tasks import runPipelineSeqenced, Task

    
@Task(id=1, parent=None, runOnAzure=True)
def initialFunction(input_data):
    return "some data"

@Task(id=2, parent=1, , runOnAzure=True)
def someOtherFunction(input_data):
    # input_data is output of parent
    ...

# setup list and run pipeline
task_list = [readRawInput, processWords, sumNumbers]
result_data = runPipelineSeqenced(task_list)
```


More examples can be found in the `test` folder.

## Configuration (for Azure)

All configuration options reside in `azure_batch.config`. 
Alternatively one can create the file `azure_batch.config_user` which will override the options.

A valid Azure Batch Account is required. Furthermore, it is assumed that it is possible to execute docker commands on the nodes.

Jobs and Tasks are created automatically.

The docker sections in the configuration must specify a image containing the python packges `dill` and `azure` (see the `Dockerfile` for an example of a basic image).

The usage of docker containers is due to the fact that dill only supports serializing between the same python versions.
Using docker gives us more control about which version to use.

Note:the DOCKER_VM_IMAGE parameter is not used at the moment.