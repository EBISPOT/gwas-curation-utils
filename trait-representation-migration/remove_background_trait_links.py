# Activate Python venv for the script - uncomment to run script on commandline
activate_this_file = "/path/to/bin/activate_this.py"
execfile(activate_this_file, dict(__file__ = activate_this_file))

import cx_Oracle
import contextlib
import argparse
import sys
from tqdm import tqdm
import csv
import os.path

sys.path.insert(0, '/path/to/gwas_data_sources')
import gwas_data_sources

import datetime


def read_file(filename):
    '''
    Read file.

    Args:
        filename: Name of file provided as a commandline argument.

    Returns:
        data: File object.
    '''
    data = []

    with open(filename, 'r') as file:
        lines = file.readlines()[1:]

        for line in lines:
            formatted_line = line.split('\t')

            study_accession = formatted_line[2].strip()

            background_column = formatted_line[4].strip()

            delimiter = '||'

            if not background_column == '':
                study_traits = {}
                if delimiter in background_column:
                    background_column = background_column.split(delimiter)
                    background_traits = [item.strip().lower() for item in background_column]
                else:
                    background_traits = background_column.strip().lower() 
     
                study_traits[study_accession] = background_traits
                data.append(study_traits)
            else:
                pass

    return data


def database_connection(DATABASE_NAME):
    '''
    Connect to the database and returns a cursor object.

    Args:
        database_name (str): The name of the database.

    Raises:
        DatabaseError: Error reponse if unable to connect to the database.

    Returns:
        cursor: Database cursor object.
    '''

    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)

        cursor = connection.cursor()

        return cursor

    except cx_Oracle.DatabaseError, exception:
        print exception


def get_efo_id_map(cursor):
    '''
    Get mapping of term labels to EFO IDs. 

    Args: 
        cursor: Database cursor object.


    Returns:
        efo_map: A mapping of term labels to it's EFO ID
    '''
    efo_map = {}

    efo_sql = '''
        SELECT ID, LOWER(TRAIT)
        FROM EFO_TRAIT
    '''

    cursor.execute(efo_sql)
    efo_data = cursor.fetchall()

    for row in tqdm(efo_data, desc='Build EFO map'):
        efo_map[row[0]] = row[1] 

    return efo_map


def _execute_query(cursor, query):
    '''
    Run the query.

    Args: 
        query (str): The query to run.

    Raises:
    '''

    cursor.execute(query)

    # commit or rollback changes
    # if args.mode == 'production':
    #     cursor.execute('COMMIT')
    # else:
    #     cursor.execute('ROLLBACK')
    pass


if __name__ == '__main__':
    '''
    Remove background trait link to Studies.
    '''

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', default='study_background_traits-ALL.txt', 
                        help='Name of data file (default: study_background_traits).')
    parser.add_argument('--database', default='DEV3', choices=['DEV3', 'SPOTPRO'], 
                        help='Run as (default: DEV3).')
    parser.add_argument('--mode', default='debug', choices=['debug', 'production'], 
                        help='Run as (default: debug).')
    args = parser.parse_args()

    global DATABASE_NAME
    DATABASE_NAME = args.database

    read_file(args.filename)

    # cursor = database_connection()
    
    # get_efo_id_map(cursor)

    # remove_study_efo_trait_links()
    


