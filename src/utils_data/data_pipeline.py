from src.utils_data.config import *

def run_dataprep_pipeline(filename):
    full_path = os.sep.join([data_folder, filename])
    df = pd.read_csv(full_path)

    df, data_tables = (df.pipe(parse_column_names)
                         .pipe(parse_position_control_column)
                         .pipe(make_normalized)
                       )

    return df, data_tables