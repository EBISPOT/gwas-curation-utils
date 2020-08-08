# Activate Python venv for the script - uncomment to run script on commandline
# activate_this_file = "/path/to/bin/activate_this.py"
# execfile(activate_this_file, dict(__file__ = activate_this_file))

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
        data_map: Dictionary with the STUDY_ID as the key and a 
        list of EFO_IDs as the dictionary value.
    '''

    with open(filename, 'r') as file:
        lines = file.readlines()[1:]

    return lines


def process_file_contents(file_data, efo_map, cursor):
    '''
    Extract fields of interest from the file and clean-up values.

    Args:
        file_data: Contents of the file.
    '''

    count = 0

    for line in file_data:
        formatted_line = line.split('\t')

        study_accession = formatted_line[2].strip()
        study_id = _get_study_id(study_accession)

        background_column = formatted_line[4].strip()

        delimiter = '||'

        if not background_column == '':
            count += 1
            # print('\nCount: {} StudyID: {} Accession: {}'.format(count, study_id, study_accession))
            
            if delimiter in background_column:
                background_column = background_column.split(delimiter)

                for background_trait in background_column:
                    # Get the EFO_ID
                    background_trait_id = efo_map[background_trait.strip().lower()]
                    # print('TID: {}'.format(background_trait_id))
                    _execute_delete_query(cursor, study_id, background_trait_id)
            else:
                # Get the EFO_ID
                background_trait_id = efo_map[background_column.strip().lower()]
                # print('TID: {}'.format(background_trait_id))
                _execute_delete_query(cursor, study_id, background_trait_id)
 

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

        return connection, cursor

    except cx_Oracle.DatabaseError, exception:
        print exception


def get_efo_id_map(cursor):
    '''
    Get mapping of term labels to EFO IDs. 

    Args: 
        cursor (object): Database cursor object.


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
        # Key is Trait, Value is ID
        efo_map[row[1]] = row[0]

    return efo_map


def _get_study_id(accession):
    '''
    Query the STUDY table with the accession to get the Study ID.
    
    Args: 
        accession (str): study accession parsed from input file

    Returns:
        study_id (int): STUDY.ID, the primary key for the row with the study accession.
    '''

    study_sql = '''
        SELECT ID
        FROM STUDY
        WHERE ACCESSION_ID = '{}'
    '''.format(accession)

    cursor.execute(study_sql)
    study_id = cursor.fetchone()

    return study_id[0]



def _execute_delete_query(cursor, study_id, efo_trait_id):
    '''
    Delete row from STUDY_EFO_TRAIT table.

    Args: 
        query (str): The query to run.

    Raises:
    '''

    study_efo_trait_delete_sql = '''
        DELETE FROM STUDY_EFO_TRAIT
        WHERE STUDY_ID = '{}' AND EFO_TRAIT_ID = '{}'
    '''.format(study_id, efo_trait_id)

    cursor.execute(study_efo_trait_delete_sql)

    # commit or rollback changes
    if args.mode == 'production':
        cursor.execute('COMMIT')
    else:
        cursor.execute('ROLLBACK')


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

    # Get database connection
    conn, cursor = database_connection(args.database)
    
    # Create map of Trait labels and EFO_IDs
    efo_map = get_efo_id_map(cursor)

    # Read data file
    data = read_file(args.filename)

    # Format column values and remove STUDY_EFO_TRAIT link
    process_file_contents(data, efo_map, cursor)

    # Close database connection
    conn.close()



