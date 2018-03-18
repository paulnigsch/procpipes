# adopted from:
# python_tutorial_task.py - Batch Python SDK tutorial sample
#
# Copyright (c) Microsoft Corporation
#
import argparse
import os
import logging

import azure.storage.blob as azureblob

if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('files', metavar='f', nargs='+', 
                        help='files to upload')

    parser.add_argument('--storageaccount', required=True,
                        help='target storage acount name')

    parser.add_argument('--account_key', required=True,
                        help='storage account key')

    parser.add_argument('--storagecontainer', required=True,
                        help='target storage container')


    args = parser.parse_args()

    output_files = args.files

    logging.info("============================================")
    logging.info("storage accout: {}".format(args.storageaccount) )
    logging.info("storage account_key: {}".format(args.account_key) )

    for file in output_files:
        blob_name = os.path.basename(file)
        output_file_path = os.path.realpath(file)
        
        logging.info('Uploading file {} to container [{}]/{}...'.format(
            output_file_path,
            args.storagecontainer,
            blob_name))

        # Create the blob client using the container's SAS token.
        blob_client = azureblob.BlockBlobService(account_name=args.storageaccount,
                                                account_key=args.account_key)


        blob_client.create_blob_from_path(args.storagecontainer,
                                        blob_name,
                                        output_file_path)