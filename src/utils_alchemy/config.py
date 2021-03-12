from utils_data.config import cols_to_normalize, data_folder, logging

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.types import Integer, Text, String, Float, Boolean, Time, Date
from sqlalchemy.ext.declarative import declarative_base

import os
import ntpath

db_folder = os.sep.join([os.environ['PWD'], 'db'])
db_class_name = 'BudgeDB.base'
db_table_name = 'budget2021'

def get_vars_main(filename):
    logging.info('get_vars_main() Producing code to make main data table in alchemy.')
    print('# Main Table - Copy and Paste this class into db_schema.')

    class_name = f'class {db_table_name.title()}({db_class_name}):'
    print(class_name)
    full_path = os.sep.join([data_folder, filename])
    data = pd.read_csv(full_path)
    cols = []

    for col in data.columns:
        col_type = data[col].dtype
        cols.append((col, col_type))

    def c_type(x):
        return "Integer" if 'int' in str(x) else "String" if "object" in str(x) else "String"

    def p_type(x):
        return ", primary_key=True" if 'id' in str(x) else ""

    def k_type(x):
        code_dict = dict((code, description) for code, description in cols_to_normalize)

        if x in code_dict.keys():
            z = code_dict[x]
            z = z.replace("_", ' ')
            z1, z2 = z.split(' ')
            z2 = z2.title()
            z = ''.join([z1,z2])
            z = '.'.join([z, x])
        else:
            z = ''

        return f', ForeignKey("{z}")' if any(x == col for col, descr in cols_to_normalize) else ''

    column_variables = [(f"{col_name} = Column({c_type(col_type)}{p_type(col_name)}{k_type(col_name)})") for col_name, col_type in cols]

    print(f'\t__tablename__ = "{db_table_name}"')
    print('\t' + '\n\t'.join(column_variables))
    print()
    print('\t# These are for linking tables via relationships')

    def r_type(x):
        x = x.replace("_", " ")
        x = x.title()
        x = x.replace(" ", "")
        return x

    relationship_variables = [f'{description}=relationship("{r_type(description)}", back_populates="main", uselist=True)' for code, description in cols_to_normalize]
    # print('[%s]' % '\n'.join(map(str, relationship_variables)))
    print('\t' + '\n\t'.join(relationship_variables))
    print('\t# This is the end of the class code')

    """
    class SectionDescription(BudgetDB.base):
    __tablename__ = "sectionDescription"
    section_code = Column(String, primary_key=True)
    section_description = Column(String)
    main = relationship("Budget2021", back_populates="section_description", uselist=True)
    """

def df_2_db(data_paths, schema_table_names, engine, if_exists='replace', index=False, chunksize=500):
    logging.info('df_2_db() Converting pandas dataframes to SQL db.')
    # https://stackoverflow.com/questions/8384737/extract-file-name-from-path-no-matter-what-the-os-path-format
    data_tables = []
    type_map = {'int64': Integer
              , 'float64': Float
              , 'object': String
                }
    def d(x):
        x = str(x)
        if x in type_map.keys():
            x = [val for key, val in type_map.items() if x in key][0]
        else:
            logging.warning('df_2_db()->d()Data Type Match Not Found')
        return x

    for i in data_paths:
        df = None
        df = pd.read_csv(i)
        curr_table = ntpath.basename(i).replace('.csv','')
        # return a tuple of col name and data type
        dtype = dict([(i, d(df[i].dtype.name)) for i in df.columns])
        if curr_table in schema_table_names and df is not None:
            df.to_sql(name=curr_table
                    , con=engine
                    , if_exists=if_exists
                    , index=index
                    , chunksize=chunksize
                    , dtype=dtype
                      )
        else:
            logging.warning('df_2_db() Disconnect in adding a table, check schema names and table names.')

    logging.info('df_2_db() completed pandas to db conversion.')