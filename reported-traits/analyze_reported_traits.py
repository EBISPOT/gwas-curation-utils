import cx_Oracle
import contextlib
import sys
import os
import argparse
import subprocess
import json
import csv
import logging
from tqdm import tqdm
from datetime import datetime, date
from gwas_db_connect import DBConnection


class ReportedTraitData:

    ALL_REPORTED_TRAITS_SQL = '''
        SELECT  *
        FROM DISEASE_TRAIT
    '''


    def __init__(self, connection, database):
        self.database = database
        # self.logging_level = logging_level

        logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

        try:
            with contextlib.closing(connection.cursor()) as cursor:
                ######################
                # Get all study data
                ######################
                cursor.execute(self.ALL_REPORTED_TRAITS_SQL)
                data = cursor.fetchall()
                self.data = data
                # self.data = data[:20]
                logging.debug('Successfully extracted reported traits')

        except(cx_Oracle.DatabaseError, exception):
            print(exception)


    def save_file(self):
        ''' Write reported traits to file '''
        traits = [''.join(trait[1]) for trait in self.data]
        traits.sort()

        with open("reported_trait.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Reported Trait'])

            for trait in traits:
                writer.writerow([trait])

            logging.info('reported_trait.csv created')


    def _get_timestamp(self):
        ''' 
        Get timestamp of current date. 
        '''
        return date.today().strftime('%d-%m-%Y')


if __name__ == '__main__':

    # Parsing command line arguments:
    parser = argparse.ArgumentParser()
    # parser.add_argument('--action', type=str, help='Task to preform, e.g. dump, analyze, upload')
    parser.add_argument('--curation_db', type=str, help='Name of the database for extracting study data.')
    # parser.add_argument('--logging_level', type=str, default='logging.INFO', help='Name of the database for extracting study data.')
    args = parser.parse_args()

    database = args.curation_db
    # logging_level = args.logging_level

    # Open connection:
    db_object = DBConnection.gwasCatalogDbConnector(database)
    connection = db_object.connection

    # Get published studies from database
    all_reported_traits_obj = ReportedTraitData(connection, database)

    # Write to file
    all_reported_traits_obj.save_file()


 