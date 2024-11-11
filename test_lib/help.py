import os
import logging

def read_spark_sql_source_code(filePath):

    # logging config
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
    if os.path.exists(filePath): 
        with open(filePath, 'r') as file:
            code = file.read()
    else:
        logging.error(f'{filePath} does not exist, please check it again!')
        code = ''
    return code