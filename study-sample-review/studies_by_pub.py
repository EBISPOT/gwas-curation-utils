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
import subprocess

sys.path.insert(0, '/path/to/gwas_data_sources')
import gwas_data_sources

import datetime


def get_curation_review_data(pmid, curator, file_name):
    '''
    Get data for level 1.
    '''

    # List of queries
    curation_level1_data_sql = """
        SELECT S.ID, S.ACCESSION_ID, P.PUBMED_ID, A.FULLNAME,  
        TO_CHAR(P.PUBLICATION_DATE, 'dd-mm-yyyy'), P.PUBLICATION, P.TITLE, 
        TO_CHAR(H.STUDY_ADDED_DATE, 'dd-mm-yyyy'), CS.STATUS, TO_CHAR(H.CATALOG_PUBLISH_DATE, 'dd-mm-yyyy'), 
        S.INITIAL_SAMPLE_SIZE, S.REPLICATE_SAMPLE_SIZE
        FROM STUDY S, HOUSEKEEPING H, PUBLICATION P, AUTHOR A, CURATION_STATUS CS
        WHERE S.HOUSEKEEPING_ID=H.ID and S.PUBLICATION_ID=P.ID and P.FIRST_AUTHOR_ID=A.ID
            and H.CURATION_STATUS_ID=CS.ID
            and P.PUBMED_ID= :pmid
    """


    study_dup_tag_sql = """
        SELECT N.TEXT_NOTE
        FROM STUDY S, NOTE N
        WHERE S.ID=N.STUDY_ID
          and N.NOTE_SUBJECT_ID=9
          and S.ID= :study_id
    """


    study_reported_trait_sql = """
        SELECT listagg(DT.TRAIT, ', ')  WITHIN GROUP (ORDER BY DT.TRAIT) 
        FROM STUDY S, STUDY_DISEASE_TRAIT SDT, DISEASE_TRAIT DT 
        WHERE S.ID=SDT.STUDY_ID and SDT.DISEASE_TRAIT_ID=DT.ID 
            and S.ID= :study_id
    """


    study_mapped_trait_sql = """
        SELECT listagg(ET.TRAIT, ', ')  WITHIN GROUP (ORDER BY ET.TRAIT), ET.SHORT_FORM
        FROM STUDY S, STUDY_EFO_TRAIT SETR, EFO_TRAIT ET
        WHERE S.ID=SETR.STUDY_ID and SETR.EFO_TRAIT_ID=ET.ID
          and S.ID= :study_id
        GROUP BY ET.SHORT_FORM
    """

    attr_list = ['STUDY_ID', 'STUDY_ACCCESSION', 'DUP_TAG', 'REPORTED_TRAIT',  'EFO_TRAIT', 'MAPPED_TRAIT', 'PUBMED_ID', 'FIRST_AUTHOR']

    first_author_sql = """
        SELECT REPLACE(A.FULLNAME_STANDARD, ' ', '')
        FROM PUBLICATION P, AUTHOR A 
        WHERE P.FIRST_AUTHOR_ID=A.ID 
            and P.PUBMED_ID= :pmid
    """

    first_author = ""
    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)

        with contextlib.closing(connection.cursor()) as cursor:
            cursor.prepare(first_author_sql)
            cursor.execute(None, {'pmid': pmid})
            first_author_data = cursor.fetchone()
            first_author = first_author_data[0]

        connection.close()

    except cx_Oracle.DatabaseError, exception:
        print exception

    
    outfile = open(file_name, "w")
    csvout = csv.writer(outfile)
    csvout.writerow(attr_list)


    # Get data for curation review file
    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)


        with contextlib.closing(connection.cursor()) as cursor:
            cursor.prepare(curation_level1_data_sql)
            cursor.execute(None, {'pmid': pmid})
            curation_queue_data = cursor.fetchall()


            for data in curation_queue_data:

                data_results = {}

                data_results['STUDY_ID'] = data[0]

                # Account for studies that do not yet have an AccessionId
                if data[1] is None:
                    data_results['STUDY_ACCCESSION'] = 'Not yet assigned'
                else:
                    data_results['STUDY_ACCCESSION'] = data[1]

                data_results['PUBMED_ID'] = data[2]

                data_results['FIRST_AUTHOR'] = data[3]


                #############################
                # Get Study Dup Tag 
                #############################
                cursor.prepare(study_dup_tag_sql)
                r = cursor.execute(None, {'study_id': data[0]})
                dup_tag = cursor.fetchone()

                if not dup_tag:
                    data_results['DUP_TAG'] = 'None'                   
                else:
                    data_results['DUP_TAG'] = dup_tag[0]


                ##########################
                # Get Reported Trait 
                ##########################
                cursor.prepare(study_reported_trait_sql)
                r = cursor.execute(None, {'study_id': data[0]})
                reported_trait = cursor.fetchone()

                if not reported_trait:
                    data_results['REPORTED_TRAIT'] = 'None'                   
                else:
                    data_results['REPORTED_TRAIT'] = reported_trait[0]


                ##########################
                # Get Mapped/EFO Trait 
                ##########################
                cursor.prepare(study_mapped_trait_sql)
                r = cursor.execute(None, {'study_id': data[0]})
                mapped_trait = cursor.fetchone()

                if not mapped_trait:
                    data_results['MAPPED_TRAIT'] = 'None'
                    data_results['EFO_TRAIT'] = 'None'
                else:
                    data_results['MAPPED_TRAIT'] = mapped_trait[0]
                    data_results['EFO_TRAIT'] = mapped_trait[1]


                # Write out results
                data_keys = data_results.keys()
                results = [(data_results[key]) for key in attr_list if key in data_keys]
                csvout.writerow(results)


        connection.close()
        

    except cx_Oracle.DatabaseError, exception:
        print exception


def print_table(file_name):
    column_cmd = 'column -s"," -t {}'.format(file_name)
    process = subprocess.Popen(column_cmd.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(output)


def get_timestamp():
    '''
    Get timestamp of current date and time. 
    '''
    timestamp = '{:%Y-%m-%d-%H-%M}'.format(datetime.datetime.now())
    return timestamp


if __name__ == '__main__':
    '''
    Create Level2 curation check file.
    '''

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', default='SPOTPRO', choices=['SPOTPRO'], 
                        help='Run as (default: SPOTPRO).')
    parser.add_argument('--pmid', default='28256260', help='Add Pubmed Identifier, e.g. 28256260.')
    parser.add_argument('--username', default='gwas-curator', help='Run as (default: gwas-curator).')
    args = parser.parse_args()

    global DATABASE_NAME
    DATABASE_NAME = args.database

    pmid = args.pmid
    username = args.username

    TIMESTAMP = get_timestamp()
    file_name = pmid+"_"+username+"_"+TIMESTAMP+".csv"

    get_curation_review_data(pmid, username, file_name)
    if os.path.isfile(file_name):
        print_table(file_name)
