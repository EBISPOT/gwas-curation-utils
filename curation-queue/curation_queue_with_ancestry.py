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

import smtplib
from os.path import basename
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


def get_curation_queue_data():
    '''
    Get Curation Queue data
    '''

    # List of queries
    curation_queue_data_sql = """
        SELECT DISTINCT (S.ID) AS STUDY_ID, TO_CHAR(H.STUDY_ADDED_DATE, 'yyyy-mm-dd'), P.PUBMED_ID, 
        A.FULLNAME, TO_CHAR(P.PUBLICATION_DATE, 'yyyy-mm-dd') AS Publication_Date, P.PUBLICATION, 
        P.TITLE, S.USER_REQUESTED, S.FULL_PVALUE_SET, CS.STATUS, S.OPEN_TARGETS
        FROM STUDY S, HOUSEKEEPING H, PUBLICATION P, AUTHOR A, CURATION_STATUS CS
        WHERE S.HOUSEKEEPING_ID = H.ID AND H.IS_PUBLISHED = 0
          and S.PUBLICATION_ID=P.ID and P.FIRST_AUTHOR_ID=A.ID and H.CURATION_STATUS_ID=CS.ID
        ORDER BY S.ID
    """


    study_reported_trait_sql = """
        SELECT listagg(DT.TRAIT, ', ')  WITHIN GROUP (ORDER BY DT.TRAIT) 
        FROM STUDY S, STUDY_DISEASE_TRAIT SDT, DISEASE_TRAIT DT 
        WHERE S.ID=SDT.STUDY_ID and SDT.DISEASE_TRAIT_ID=DT.ID 
            and S.ID= :study_id
    """


    study_mapped_trait_sql = """
        SELECT listagg(ET.TRAIT, ', ')  WITHIN GROUP (ORDER BY ET.TRAIT)
        FROM STUDY S, STUDY_EFO_TRAIT SETR, EFO_TRAIT ET
        WHERE S.ID=SETR.STUDY_ID and SETR.EFO_TRAIT_ID=ET.ID
          and S.ID= :study_id
    """


    study_association_cnt_sql = """
        SELECT COUNT(A.ID)
        FROM STUDY S, ASSOCIATION A
        WHERE S.ID=A.STUDY_ID 
            and S.ID= :study_id
    """


    study_ancestry_initial_sql = """
    SELECT SUM(A.NUMBER_OF_INDIVIDUALS) 
    FROM STUDY S, ANCESTRY A
    WHERE A.STUDY_ID=S.ID 
        and S.ID= :study_id 
        and A.TYPE='initial'
    """


    study_ancestry_replication_sql = """
    SELECT SUM(A.NUMBER_OF_INDIVIDUALS) 
    FROM STUDY S, ANCESTRY A
    WHERE A.STUDY_ID=S.ID 
        and S.ID= :study_id 
        and A.TYPE='replication'
    """


    all_curation_queue_data = []

    curation_queue_attr_list = ['STUDY_ID', 'STUDY_CREATION_DATE', 'PUBMEDID', 'FIRST_AUTHOR', \
        'PUBLICATION_DATE', 'JOURNAL', 'TITLE', 'REPORTED_TRAIT', 'EFO_TRAIT', \
        'ASSOCIATION_COUNT', 'NUMBER_OF_INDIVIDUALS_INITIAL', 'NUMBER_OF_INDIVIDUALS_REPLICATION', \
        'USER_REQUESTED?', 'FULL P-VALUE SET?', 'CURATION_STATUS', 'IS_OPEN_TARGETS?']

    
    TIMESTAMP = get_timestamp()
    outfile = open("data_queue_"+TIMESTAMP+".csv", "w")
    csvout = csv.writer(outfile)

    csvout.writerow(curation_queue_attr_list)


    try:
        ip, port, sid, username, password = gwas_data_sources.get_db_properties(DATABASE_NAME)
        dsn_tns = cx_Oracle.makedsn(ip, port, sid)
        connection = cx_Oracle.connect(username, password, dsn_tns)

        with contextlib.closing(connection.cursor()) as cursor:

            cursor.execute(curation_queue_data_sql)

            curation_queue_data = cursor.fetchall()


            for data in tqdm(curation_queue_data, desc='Get Curation Queue data'):

                curation_data = []


                curation_data.insert(0, data[0])
                
                curation_data.insert(1, data[1])

                curation_data.insert(2, data[2])

                curation_data.insert(3, data[3])

                curation_data.insert(4, data[4])

                curation_data.insert(5, data[5])

                curation_data.insert(6, data[6])


                curation_data.insert(12, data[7])

                curation_data.insert(13, data[8])

                curation_data.insert(14, data[9])

                curation_data.insert(15, data[10])



                ##########################
                # Get Reported Trait 
                ##########################
                cursor.prepare(study_reported_trait_sql)
                r = cursor.execute(None, {'study_id': data[0]})
                reported_trait = cursor.fetchone()


                if reported_trait[0] is None:
                    curation_data.insert(7, 'No values')                    
                else:
                    curation_data.insert(7, reported_trait[0])


                ##########################
                # Get Mapped/EFO Trait 
                ##########################
                cursor.prepare(study_mapped_trait_sql)
                r = cursor.execute(None, {'study_id': data[0]})
                mapped_trait = cursor.fetchone()

                if mapped_trait[0] is None:
                    curation_data.insert(8,'No values')
                else:
                    curation_data.insert(8, mapped_trait[0])


                ##########################
                # Get Association count 
                ##########################
                cursor.prepare(study_association_cnt_sql)          
                r = cursor.execute(None, {'study_id': data[0]})
                association_cnt = cursor.fetchone()

                curation_data.insert(9, association_cnt[0])


                ###############################
                # Get Num Individuals Initial 
                ###############################
                cursor.prepare(study_ancestry_initial_sql)           
                r = cursor.execute(None, {'study_id': data[0]})
                ancestry_initial_cnt = cursor.fetchone()

                if ancestry_initial_cnt[0] is None:
                    curation_data.insert(10, 'No values')
                else:
                    curation_data.insert(10, ancestry_initial_cnt[0])
                    

                #########################################
                # Get Num Individuals Replication
                #########################################
                cursor.prepare(study_ancestry_replication_sql)            
                r = cursor.execute(None, {'study_id': data[0]})
                ancestry_replication_cnt = cursor.fetchone()

                if ancestry_replication_cnt[0] is None:
                    curation_data.insert(11, 'No values')                    
                else:
                    curation_data.insert(11, ancestry_replication_cnt[0])


                ###############################
                # Write out row data to file
                ##############################
                csvout.writerow(curation_data)

        

        connection.close()

        return all_curation_queue_data

    except cx_Oracle.DatabaseError, exception:
        print exception


def get_timestamp():
    """ 
    Get timestamp of current date and time. 
    """
    timestamp = '{:%Y-%m-%d}'.format(datetime.datetime.now())
    return timestamp


def send_email(*args):
    '''
    Email report file.
    '''
    
    # Today's date
    now = datetime.datetime.now()
    datestamp = str(now.day)+"_"+str(now.strftime("%b"))+"_"+str(now.year)

    file_name = args[0]

    with open(file_name, "rb") as fil:
        part = MIMEApplication(
            fil.read(),
            Name=basename(file_name)
        )
    
    # create a text/plain message
    msg = MIMEMultipart()

    # After the file is closed
    part['Content-Disposition'] = 'attachment; filename="%s"' % basename(file_name)
    msg.attach(part)


    # create headers
    me = 'spotbot@ebi.ac.uk'
    you = ['gwas-dev-logs@ebi.ac.uk', 'gwas-curator@ebi.ac.uk']
    # you = ['twhetzel@ebi.ac.uk']
    msg['Subject'] = 'GWAS Curation Queue '+datestamp
    msg['From'] = me
    msg['To'] = ", ".join(you)

    # send the message via our own SMTP server, but don't include the
    # envelope header
    s = smtplib.SMTP('localhost')
    s.sendmail(me, you, msg.as_string())
    s.quit()


if __name__ == '__main__':
    '''
    Create curation metrics.
    '''

    # Commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', default='SPOTPRO', choices=['DEV3', 'SPOTPRO'], 
                        help='Run as (default: SPOTPRO).')
    args = parser.parse_args()

    global DATABASE_NAME
    DATABASE_NAME = args.database

    curation_queue_data = get_curation_queue_data()
    
    # Email data to curators
    TIMESTAMP = get_timestamp()
    report_filename = "data_queue_"+TIMESTAMP+".csv"
    send_email(report_filename)

