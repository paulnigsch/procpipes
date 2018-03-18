

import unittest
import logging
import os,sys,inspect
import operator, functools
import hashlib
import os.path

sys.path.insert(1, os.path.join(sys.path[0], '..'))
from tasks import *


class TestTasks(unittest.TestCase):


    def test_sequenced(self):
    

        @Task(id=0, parent=None, description="rootTest")
        def rootTest(input_data):
            logging.info(input_data)
            input_data = 0
            return input_data + 1

        @Task(id=1, parent=0, description="rootTest")
        def ChildTest(input_data):
            logging.info(input_data)
            return input_data * 2


        task_list = [rootTest, ChildTest]
        result_data = runPipelineSeqenced(task_list)
        self.assertEqual(result_data, 2)



    def test_sequenced_with_dumps(self):

        dump_file1= 'out1' 
        dump_file2= 'out2'


        if os.path.exists(dump_file1):
            os.remove(dump_file1)
        if os.path.exists(dump_file2):
            os.remove(dump_file2)
            
        
        @Task(id=0,
            parent=None,
            description="input file reader",
            dump_fct=False)
        def readRawInput(input_data):

            with open("data/lorem_ipsum.txt") as f:
                output = f.readlines()
                output = "".join(output)

            return output

        def dumpToFile(data, file_name):
            with open(file_name,  'w') as f:
                f.write(data)

        @Task(id=1,
            parent=0,
            description="count single word file reader", 
            dump_fct=lambda data : dumpToFile(str(data), dump_file1) )
        def processWords(input_data):
             d = input_data.split(" ")

             output = [ len(w) for w in d]

             return output

        @Task(id=2,
            parent=1,
            description="sum up word counts",
            dump_fct=lambda data : dumpToFile(str(data), dump_file2) )
        def sumNumbers(input_data):
             return functools.reduce( operator.add, input_data, 0 )


        # setup list and run pipeline
        task_list = [readRawInput, processWords, sumNumbers]
        result_data = runPipelineSeqenced(task_list)

        # check result
        self.assertEqual(result_data, 377)

        # check the generated files
        with open(dump_file1, 'rb') as f:
            hash_file1 = hashlib.md5(f.read()).hexdigest()

        with open(dump_file2, 'rb') as f:
            hash_file2 = hashlib.md5(f.read()).hexdigest()

        self.assertEqual(hash_file1, '5a69cb276441889a977e0fe35360257a')
        self.assertEqual(hash_file2, 'd34ab169b70c9dcd35e62896010cd9ff')

        # cleanup
        os.remove(dump_file1)
        os.remove(dump_file2)

if __name__ == '__main__':
    unittest.main()