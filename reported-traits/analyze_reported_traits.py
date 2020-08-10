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
from termcolor import colored
from datetime import datetime, date
from gwas_db_connect import DBConnection
import Levenshtein



class ReportedTraitData:

    ALL_REPORTED_TRAITS_SQL = '''
        SELECT  *
        FROM DISEASE_TRAIT
    '''

    def __init__(self, connection, database):
        self.database = database
        # self.logging_level = logging_level

        logging.basicConfig(
            level=logging.INFO, format='[%(levelname)s] %(message)s'
            )

    def get_all_reported_traits(self):
        try:
            with contextlib.closing(connection.cursor()) as cursor:
                cursor.execute(self.ALL_REPORTED_TRAITS_SQL)
                data = cursor.fetchall()
                #TODO: Decide whether to keep id and trait name
                self.data = data
                logging.debug('Successfully extracted reported traits')
        except cx_Oracle.DatabaseError as exception:
            logging.error(exception)

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
        ''' Get timestamp of current date. '''
        return date.today().strftime('%d-%m-%Y')

    def read_reported_trait_file(self, action):
        ''' Read in text file 
        
        Args: 
            action: the action the script will perform on the file data
        '''
        print('\nWhat file do you want to ' + action + '?')
        user_filename = input().strip()

        with open(user_filename, 'r') as input_file:
            # TODO: Check that file has only one column of data
            # decide whether to fail if more columns are found
            traits = [line.strip() for line in input_file if line.strip()]
            return traits

    def find_similar_reported_traits(self, user_trait_data):
        ''' Find similar traits

        Args:
            user_trait_data: list of tuples (id, trait)
        '''
        print('Enter the match threshold value (upper=1.0, lower=0). A match score of 1.0 is a perfect match.')
        match_threshold_value = float(input())

        if match_threshold_value > 1 or match_threshold_value  < 0:
            logging.warning('Exiting... Match threshold outside of accepted limit')
            sys.exit()

        logging.info('Searching for similarities...')
        traits = [''.join(trait[1]) for trait in self.data]

        similarities = {}
        for user_trait in tqdm(user_trait_data, desc="Traits"):
            matches_above_threshold = {}
            for db_reported_trait in traits:
                similarity_score = Levenshtein.ratio(user_trait.lower(), db_reported_trait.lower())

                # if similarity_score >= 0.7:
                if similarity_score >= match_threshold_value: 
                    matches_above_threshold[db_reported_trait] = "{:.2f}".format(similarity_score)

            # Sort list of tuples by score return list with tuple with highest score first in list
            matches = sorted(matches_above_threshold.items(), key=lambda x: x[1], reverse=True)

            similarities[user_trait] = matches

        return similarities

    def save_all_similarities_file(self, results):
        ''' Save results of similarity analysis

        Args:
            results: dictionary of similarity results, key is user
            supplied term, the value is a list of similarity result tuples

            Example: {'test 1': [], 'heart attack': [('Heart rate', 0.72)]}
        '''
        with open("similarity_analysis_results.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Reported Trait', 'Similarity results'])

            for trait, similarity_results in results.items():
                # similarity_results format example:
                # [('CD11c+ HLA DR++ monocyte %monocyte', 1.0), ('HLA DR++ monocyte %monocyte', 0.88)]
                result_matches = " || ".join("%s -- %s" % tup for tup in similarity_results)      
                writer.writerow([trait, result_matches])

            logging.info('similarity_analysis_results.csv created')

    def insert_traits(self, traits):
        ''' Add traits to the database '''
        logging.info('All traits to add: '+', '.join(traits))

        print(colored('Are you sure you want to add these traits to the Curation app production database? Options: yes, no', 'red', attrs=['bold']))
        confirm_action = input().strip().lower() or 'no' # leave a default for safety

        if confirm_action == 'no':
            logging.warning('Exiting... no confirmation to upload traits')
            sys.exit()
        elif confirm_action == 'testing':
            database_action = 'ROLLBACK'
        elif confirm_action == 'yes':
            database_action = 'COMMIT'
        else:
            logging.warning('Exiting... Confirmation did not match any of the expected values')
            sys.exit()

        existing_traits = [''.join(trait[1].strip()) for trait in self.data]
        traits_to_add = []

        # Check if any traits to be added currently exist in the database
        for trait in traits:
            if trait in existing_traits:
                print('\n')
                logging.info(trait+' already exists in database, skipping...')
            else:
                traits_to_add.append(trait)
                insert_trait_sql = 'INSERT INTO DISEASE_TRAIT VALUES (NULL, ' + "'"+trait+"'" + ')'
                # print('\n')
                # logging.info(insert_trait_sql)

                try:
                    with contextlib.closing(connection.cursor()) as cursor:
                        # Insert trait and return back the "id" primary key for the new row
                        new_id = cursor.var(cx_Oracle.NUMBER)
                        sql_event = insert_trait_sql + ' returning id into :new_id'

                        cursor.execute(sql_event, {'new_id': new_id})
                        print(new_id.getvalue())

                        disease_trait_id = new_id.getvalue()
                        logging.info('Successfully added trait: ' + "'"+ trait +"'" + ' with PK: ' + str(disease_trait_id))

                        cursor.execute(database_action)
                        if database_action == 'ROLLBACK':
                            logging.info('Queries executed in testing mode. No commit action was performed.')
                except cx_Oracle.DatabaseError as exception:
                    logging.error(exception)



if __name__ == '__main__':
    # Parsing command line arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', type=str, 
        help='Task to perform, e.g. dump, analyze, upload')
    parser.add_argument('--curation_db', type=str, 
        help='Name of the database for extracting study data.')
    # parser.add_argument('--logging_level', type=str, default='logging.INFO', help='Name of the database for extracting study data.')
    args = parser.parse_args()

    database = args.curation_db
    action = args.action
    # logging_level = args.logging_level

    # Open connection:
    db_object = DBConnection.gwasCatalogDbConnector(database)
    connection = db_object.connection

    ######################################
    # Create file of all Reported traits
    ######################################
    if action == 'dump':
        # Instantiate object
        all_reported_traits_obj = ReportedTraitData(connection, database)

        # Get all reported traits
        all_reported_traits_obj.get_all_reported_traits()

        # Write to file
        all_reported_traits_obj.save_all_reported_traits_file()

    ###################################
    # Analyze list of reported traits
    ###################################
    if action == 'analyze':
        # Instantiate object
        all_reported_traits_obj = ReportedTraitData(connection, database)

        # Get all reported traits
        all_reported_traits_obj.get_all_reported_traits()

        # Read file of traits
        traits_to_analyze = all_reported_traits_obj.read_reported_trait_file(action)

        # Analyze traits to find simiar reported trait 
        similarity_results = all_reported_traits_obj.find_similar_reported_traits(traits_to_analyze)
        
        all_reported_traits_obj.save_all_similarities_file(similarity_results)

    #######################################
    # Add Reported traits to the database
    #######################################
    if action == 'upload':
        # Instantiate object
        all_reported_traits_obj = ReportedTraitData(connection, database)
        
        # Get all reported traits
        all_reported_traits_obj.get_all_reported_traits()

        # Read file of traits to add
        traits_to_add_to_database = all_reported_traits_obj.read_reported_trait_file(action)

        # Insert traits into the database
        all_reported_traits_obj.insert_traits(traits_to_add_to_database)
