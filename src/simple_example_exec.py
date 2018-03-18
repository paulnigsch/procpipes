# simple analysis script


import logging
import argparse





def main(inputfile, outputfile):

    logger.info("reading output")
    with open(inputfile) as f:
        data = f.readlines()

    logger.info("starting calculation")
    line_lengths = []
    length_sum = 0
    for line in data:

        ll = len(line.split(" "))
        line_lengths.append( ll )

        length_sum += ll

    line_lengths.sort()

    logger.info("writing results")
    with open(outputfile, 'w') as f:
        f.write("total words in the file %i\n" % length_sum)
        f.write("shortes line has %i words\n" % line_lengths[0])
        f.write("longest line has %i words\n" % line_lengths[-1])




if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--inputfile', required=True, help='file containing initial data')
    parser.add_argument('--outputfile', required=True, help='file containing file to store the data')
    args = parser.parse_args()

    logger.info("init finished, starting main")
    main(args.inputfile, args.outputfile)