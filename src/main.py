from utils_data.data_pipeline import run_dataprep_pipeline
from utils_alchemy.db_schema import *

if __name__ == '__main__':
    # read a source file
    filename = "Budget_-_2021_Budget_Ordinance_-_Positions_and_Salaries.csv"
    # process the source file
    # normalize it a bit and prepare for database ingest
    df, data_tables = run_dataprep_pipeline(filename=filename)
    # create schema and load data into the db
    make_db(data_tables)



    





