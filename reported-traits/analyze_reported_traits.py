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

                logging.debug('Successfully extracted reported traits')

        except(cx_Oracle.DatabaseError, exception):
            print(exception)


    def save_all_reported_traits_file(self):
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
            # TODO: Check that file has only one column of data 
            # decide whether to fail if more columns are found
            traits = [trait.strip() for trait in input_file]
            return traits


    def find_similar_reported_traits(self, user_trait_data):
        ''' Find similar traits 
        
        Args:
            user_trait_data: list of tuples (id, trait)
        '''
        logging.info('Searching for similarities...')
        traits = [''.join(trait[1]) for trait in self.data]
        
        similarities = {}
        for user_trait in tqdm(user_trait_data, desc="Traits"):
            matches_above_threshold = {}
            for db_reported_trait in traits:
                similarity_score = Levenshtein.ratio(user_trait.lower(), db_reported_trait.lower())
                if similarity_score >= 0.7:
                    matches_above_threshold[db_reported_trait] = "{:.2f}".format(similarity_score)

            # Sort list of tuples by score return list with tuple with highest score first in list
            matches = sorted(matches_above_threshold.items(), key=lambda x: x[1], reverse=True)

            similarities[user_trait] = matches

        return similarities


    def save_all_similarities_file(self, results):
        ''' Save results of similarity analysis 

        Args:
            results: dictionary of similarity results, key is user suppllied term, 
            the value is a list of similarity result tuples

            Example: {'test 1': [], 'heart attack': [('Heart rate', 0.7272727272727273)]}
        '''
        with open("similarity_analysis_results.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Reported Trait', 'Similarity results'])

            for trait, similarity_results in results.items():
                # similarity_results format --> [('CD11c+ HLA DR++ monocyte %monocyte', 1.0), ('HLA DR++ monocyte %monocyte', 0.8852459016393442)]
                result_matches = " || ".join("%s -- %s" % tup for tup in similarity_results)                
                writer.writerow([trait, result_matches])

            logging.info('similarity_analysis_results.csv created')

    
    def insert_traits(self, traits):
        ''' '''
        print(traits)


if __name__ == '__main__':

    # Parsing command line arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', type=str, help='Task to perform, e.g. dump, analyze, upload')
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
        # TODO: Change this class to separate out class data from database connection object
        # Get published studies from database
        all_reported_traits_obj = ReportedTraitData(connection, database)

        # Write to file
        all_reported_traits_obj.save_all_reported_traits_file()


    # Analyze list of reported traits
    if action == 'analyze':
        # TODO: Change this class to separate out class data from database connection object
        # Get published studies from database
        all_reported_traits_obj = ReportedTraitData(connection, database)

        # Read file of traits 
        traits_to_analyze = all_reported_traits_obj.read_reported_trait_file()
        # print(traits_to_analyze)

        similarity_results = all_reported_traits_obj.find_similar_reported_traits(traits_to_analyze)
        # print(similarity_results)
        
        all_reported_traits_obj.save_all_similarities_file(similarity_results)


    # Add Reported traits to the database
    if action == 'upload':
        # TODO: Change this class to separate out class data from database connection object
        # Get published studies from database
        all_reported_traits_obj = ReportedTraitData(connection, database)

        # Read file of traits 
        traits_to_analyze = all_reported_traits_obj.read_reported_trait_file()

        # Insert traits into the database
        all_reported_traits_obj.insert_traits(traits_to_analyze)


