import dill
import argparse
import logging
import pickle

def main(cmd_args):

    task_serialization_file = cmd_args.task_binary

    data_input_path = cmd_args.datainput

    with open(data_input_path, 'rb') as f:
        fct_input = pickle.load(f)

    data_output = cmd_args.dataoutput

    with open(task_serialization_file, 'rb') as f:
        compute_task = dill.load(f)

    ret_val = compute_task(fct_input)


    with open( data_output, 'wb'  ) as f :
        pickle.dump(ret_val, f)


if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument('--task_binary', required=True,
                        help='path to dill serialized task')

    parser.add_argument('--datainput', required=True,
                        help='path data file read as input')
    parser.add_argument('--dataoutput', required=True,
                        help='path for results')


    args = parser.parse_args()        


    main(args)
