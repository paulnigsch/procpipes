

import unittest
import logging
import os,sys,inspect
import operator, functools
import hashlib
import os.path

sys.path.insert(1, os.path.join(sys.path[0], '..'))
from tasks import *


class TestTransformation(unittest.TestCase):

    def test_azure(self):
    

        @Task(id=0, parent=None, description="rootTest", runOnAzure=True)
        def rootTest(input_data):
            logging.info(input_data)
            input_data = 0
            return input_data + 1

        @Task(id=1, parent=0, description="childTest", runOnAzure=True)
        def ChildTest(input_data):
            logging.info(input_data)
            return input_data * 2

        task_list = [rootTest, ChildTest]
        result_data = runPipelineSeqenced(task_list)
        self.assertEqual(result_data, 2)

    def test_output_files(self):

        @Task(id=0, parent=None, description="write an output file", output_files=["task_output"], runOnAzure=True)
        def outputTest(input_data):
            with open("task_output", 'w') as f:
                f.write("this is a testfile\nwith two lines")
            
            return "output successfull written"


        task_list = [outputTest, ]
        result_data = runPipelineSeqenced(task_list)
        self.assertEqual(result_data, "output successfull written")

    def test_input_output_files(self):
        
        @Task(id=0, parent=None, description="write an output file", output_files=["task1_output"], runOnAzure=True)
        def writerTask(input_data):
            filename ="task1_output" 
            with open(filename, 'w') as f:
                f.write("input for the second task\nlalalal")
            
            return filename

       
        
        @Task(id=1, parent=0, description="reads an input file and reverese the lines order", input_files=["output/task1_output"], output_files=["task2_output"], runOnAzure=True)
        def outputTest(input_data):
            with open("task1_output", 'r') as f:
                lines = f.readlines()
            
            lines.reverse()
            with open("task2_output", 'w') as f:
                for l in lines:
                    f.write(l + '\n')
                
            return "output2 successfull written"


        task_list = [writerTask, outputTest]
        result_data = runPipelineSeqenced(task_list)
        self.assertEqual(result_data, "output2 successfull written")
        
        with open('output/task2_output', 'r') as f :
            file_data = f.read(1000)
        
        self.assertEqual(file_data, 'lalalal\ninput for the second task\n\n')

    def test_pandas_on_azure(self):

        @Task(id=0, parent=None, description="write an output file", output_files=["pandas_azure_test.pkl"], runOnAzure=True)
        def writerDataFrame(input_data):
            
            filename = "pandas_azure_test.pkl"

            import pandas as pd

            data = [
                [1,2,3,4],
                [1,2,3,4],
                [1,2,3,4],
                [1,2,3,4],
            ]
            df = pd.DataFrame(data)
            df.columns = ['col1', 'col2', 'col3', 'col4']
            
            
            df.to_pickle(filename)
            
            return filename

        task_list = [writerDataFrame, ]
        result_data = runPipelineSeqenced(task_list)

        import pandas as pd

        df = pd.read_pickle('output/pandas_azure_test.pkl')

        self.assertTrue( ( df['col1'] == 1 ).all() )
        self.assertTrue( ( df['col2'] == 2 ).all() )
        self.assertTrue( ( df['col3'] == 3 ).all() )
        self.assertTrue( ( df['col4'] == 4 ).all() )

        logging.info("test_pandas_on_azure finished")

if __name__ == '__main__':
    unittest.main()