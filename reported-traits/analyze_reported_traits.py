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

import Levenshtein

# import string
# from sklearn.metrics.pairwise import cosine_similarity
# from sklearn.feature_extraction.text import CountVectorization. #issue installing this
# from nltk.corpus import stopwords
# stopwords = stopwords.words('english')


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


    def read_reported_trait_file(self):
        ''' Read in text file '''
        print('\nWhat file do you want to analyze?')
        user_filename = input().strip()
        
        with open(user_filename, 'r') as input_file:
            traits = [trait.strip() for trait in input_file]
            return traits


    def find_similar_reported_traits(self, user_trait_data):
        ''' Find similar traits '''
        logging.info('Searching for similarities...')
        traits = [''.join(trait[1]) for trait in self.data]
        
        similarities = {}
        for user_trait in user_trait_data:
            print('\nLooking for similar terms for: ', user_trait)
            matches_above_threshold = {}
            for db_reported_trait in traits:
                similarity_score = Levenshtein.ratio(user_trait, db_reported_trait)
                if similarity_score >= 0.7:
                    matches_above_threshold[db_reported_trait] = similarity_score

            matches = sorted(matches_above_threshold.items(), key=lambda x: x[1], reverse=True)
            print(matches[:5])


if __name__ == '__main__':

    # Parsing command line arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', type=str, help='Task to preform, e.g. dump, analyze, upload')
    parser.add_argument('--curation_db', type=str, help='Name of the database for extracting study data.')
    # parser.add_argument('--logging_level', type=str, default='logging.INFO', help='Name of the database for extracting study data.')
    args = parser.parse_args()

    database = args.curation_db
    action = args.action
    # logging_level = args.logging_level

    # Open connection:
    db_object = DBConnection.gwasCatalogDbConnector(database)
    connection = db_object.connection

    
    # Create file of all Reported Traits
    if action == 'dump':
        # Get published studies from database
        all_reported_traits_obj = ReportedTraitData(connection, database)

        # Write to file
        all_reported_traits_obj.save_file()


    # Analyze list of reported traits
    if action == 'analyze':
        # Get published studies from database
        all_reported_traits_obj = ReportedTraitData(connection, database)

        # Read file of traits 
        traits_to_analyze = all_reported_traits_obj.read_reported_trait_file()
        # print(traits_to_analyze)

        all_reported_traits_obj.find_similar_reported_traits(traits_to_analyze)
        # TODO: Read in Excel and/or text file, find top 5 matches from an existing trait, write out similarity file



 