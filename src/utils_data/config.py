import os

import numpy as np
import pandas as pd
pd.set_option('display.max_columns', None)
import logging


cols_to_normalize = [('section_code', 'section_description')
                        , ('fund_code', 'fund_description')
                        , ('title_code', 'title_description')
                        , ('department_code', 'department_description')
                         ]


# set global var for data folder
data_folder = os.sep.join([os.environ['PWD'], 'data'])
"""
data source
https://data.cityofchicago.org/Administration-Finance/Budget-2021-Budget-Ordinance-Positions-and-Salarie/gcwx-xm5a
"""

def parse_column_names(df):
    """
    :params: df -> a dataframe of budget data from chicago
    reduces column names to have no spaces which is important for database
    """
    logging.info('parse_cols() Parse column names to lower case and replce spaces with underscores.')
    df.columns = map(str.lower, df.columns)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('-', '_')
    df.columns = df.columns.str.replace('/', '_')
    return df

def parse_position_control_column(df):
    """
    :params: df -> a dataframe of budget data from chicago
    splits total_budgeted_unit into two columns based on business rule on unit types.
    per chicago data portal
    “Position Control” determines whether Total Budgeted Units column will count employees and vacancies or hours/months.

    If a Position Control is 1, then employees and vacancies are displayed;
    If a Position Control is 0, then the total number of hours/months recorded is displayed.

    """
    logging.info('parse_position_control_column() Parse budget data for position_control and total_budgeted_unit.')

    df['total_budgeted_unit_people'] = np.where(df['position_control'] == 1, df['total_budgeted_unit'], 0)
    df['total_budgeted_unit_time'] = np.where(df['position_control'] == 0, df['total_budgeted_unit'], 0)

    df = df.drop(columns=['total_budgeted_unit'])

    return df

def make_normalized(df):
    """
    transform one big table into a series of normalized tables
    purpose is to demonstrate how to bring this all together with SQLalchemy
    write to disk - four csv files of code/description pairs and a main table of budget data
    """

    # pull the code and description apart where it seems like they are universally unique pairs
    logging.info('make_normalized() Normalizing the source table.')

    data_tables = []

    for code, description in cols_to_normalize:
        # format file names to help synch databse upload in utils_alchemy
        t1, t2 = description.split('_')
        t2 = t2.title()
        temp_name = ''.join([t1, t2])
        filename = f'{temp_name}.csv'
        full_path = os.sep.join([data_folder, filename])
        codes = df[[code, description]].copy().drop_duplicates().reset_index(drop=True)
        # later, sql alchemy will want to have a unique id for each table - use the index as a UID for this table
        # separately, the schema will use the section code to link to the section name
        codes.index.name = 'id'
        codes.to_csv(full_path)
        df.drop(columns=[description], inplace=True)
        logging.info(f'- normalized {code} and {description}.')
        data_tables.append(full_path)

    filename = 'budget2021.csv'
    full_path = os.sep.join([data_folder, filename])
    df.index.name = 'id'
    df.to_csv(full_path)
    logging.info(f'- Wrote {filename} to disk.')
    data_tables.append(full_path)

    return df, data_tables