import csv
import os
from os import listdir
from os.path import isfile, join
import gzip
import shutil
import psycopg2


######################
# CONST VALUES
######################

DATABASE_NAME = 'dbimdb'                        # database name for psql connection
USER = 'postgres'                               # username for psql connection
PASSWORD = 'tak123'                             # password for psql connection
HOST = 'localhost'                              # host name for psql connection
GZIP_FILE_PATH = 'TSV_Files'                    # path to directory where all zipped files are
UNZIP_FILE_PATH = 'Unzipped_TSV_Files'          # path to directory where all files goes after program unzip them


######################
# FUNCTIONS
######################

# unzip_files unzips file to new directory
# arguments:
# from_where_unzip_file_path - path to directory where all zipped files are
# to_where_unzip_file_path - path to directory where
def unzip_files(from_where_unzip_file_path, to_where_unzip_file_path):

    # if checks if there is folder where we can store unzipped files if there any it creates one
    if not os.path.exists(to_where_unzip_file_path):
        os.makedirs(to_where_unzip_file_path)

    # list all files in directory where all zipped files are
    files_in_dir = [f for f in listdir(from_where_unzip_file_path) if isfile(join(from_where_unzip_file_path, f))]

    # extrude all files in that directory to another directory
    for file in files_in_dir:
        with gzip.open(f'{from_where_unzip_file_path}/{file}', 'rb') as f_in:
            with open(f'{to_where_unzip_file_path}/{file}.txt', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"| unzipped {file} successfully")


# create_table creates table based on parameters
# arguments:
# cursor - connection cursor
# table_name - name of table that will be crated (usually passed name of file that table is created on)
# tab - string witch coma separated values (first line in file)
def create_table(cursor, table_name, tab) -> None:

    split = tab.split(", ")
    id_value = split[0]  # id value is always first in passed tab string
    rest_values = split[1:]  # rest of the values passed in string tab
    rest_values_string = ''  # string that will be passed in psql command

    for i in rest_values:
        rest_values_string += f'{i} TEXT, '

    rest_values_string = rest_values_string[:-2]

    # psql code execution (this is line of code that will be executed in psql command line)
    cursor.execute(f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
                     {id_value}         TEXT          PRIMARY KEY,
                     {rest_values_string}
        );
    """)

    # DEBUG PURPOSE! - shows id of table and rest of values
    print("|Table name")
    print(f"|-- {table_name}")
    print("|ID value:")
    print(f'|-- {id_value}')
    print("|rest values:")
    print(f'|-- {rest_values_string}')
    print('| DATABASE CREATED SUCCESSFULLY !')


# table insert function insert values in table line by line from text file
# arguments:
# cursor - connection cursor
# line - line from text file
# tab - string witch coma separated values (first line in file)
def table_insert(cursor, line, table_name, tab):

    values = []
    split = tab.split(", ")

    for i in split:

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # i had i lot of problems with syntax for psql so i had to remove all "'" and replace them with space
        # have no idea how to deal with it
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        values.append(line[i].replace("'", " "))

    tuple_with_values = tuple(r'%s' % x for x in values)

    # line of code to execute in psql
    sql_insert = """INSERT INTO {table_name}  ({tab}) VALUES {tup}"""

    # cursor will execute that line of code:
    cursor.execute(sql_insert.format(table_name=table_name, tab=tab, tup=tuple_with_values))

    # DEBUG PURPOSE! - shows id of inserted row
    print(f'|Row inserted successfully:')
    print(f'|-- {tuple_with_values[0]}')


# tsv_to_psql function search for all files in directory then open each of them, scan each line and then based on those
# lines create table and then insert values into created table
# cursor - connection cursor
# conn - connection to database
# table_name - name of the table that will be created
# from_where_unzip_file_path - path to directory witch unzipped files
# database_table_exist = if true table will be not created
def tsv_to_psql(cursor, from_where_unzip_file_path, database_table_exist=False):

    # list all files in directory where all zipped files are
    files_in_dir = [f for f in listdir(from_where_unzip_file_path) if isfile(join(from_where_unzip_file_path, f))]

    # for each file in directory:
    for file in files_in_dir:
        # open that file in utf-8 format :
        with open(f'{from_where_unzip_file_path}/{file}', 'r', encoding="utf8") as infile:

            # name of the table is based on file name before firs dot (".")
            tab_name = file.split('.')[0]

            # take first line from file
            tab1 = infile.readline()

            # replace all TABS for ', ' and deletes new line
            tab = tab1.replace('\t', ', ').replace('\n', '')

            # if table exist is true function will not recreate table from zero
            if not database_table_exist:
                create_table(cursor, tab_name, tab)

            # reset file line
            infile.seek(0)

            # csv generator to the file, will be read line by line
            content = csv.DictReader(infile, quoting=csv.QUOTE_NONE, delimiter='\t')

            # for each line in content (content is list of words of one line from file)
            for line in content:
                # table_insert function
                table_insert(cursor, line, tab_name, tab)


#####################################
# main function of the program:
#####################################

def main_func(unzip=True):

    # unzipping (if you have already unzipped files you can check False)
    # if unzip is true unzip files in folder
    if unzip:
        unzip_files(GZIP_FILE_PATH, UNZIP_FILE_PATH)

    # establish connection to database
    # connection
    conn = psycopg2.connect(
        host=HOST,
        database=DATABASE_NAME,
        user=USER,
        password=PASSWORD,
    )

    conn.autocommit = True

    # cursor
    cursor = conn.cursor()

    # function will convert tsv file into psql
    tsv_to_psql(cursor, UNZIP_FILE_PATH)


# RUN PROGRAM
main_func(unzip=False)
