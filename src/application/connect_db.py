from application.config import *
from utils_alchemy.db_schema import BudgetDB
from utils_alchemy.config import sessionmaker, create_engine, db_name


@st.cache(allow_output_mutation=True)
def connect_db():
    try:
        dirPath = os.path.dirname(os.path.realpath(__file__))
    except:
        dirPath = os.getcwd()

    # the key attrs
    path = "sqlite:///{}/{}".format(dirPath, db_name)

    # this leads to an error
    # con = sqlite3.connect(path)
    # instead create engine
    eng = create_engine(path, connect_args={'check_same_thread': False})
    session = sessionmaker(bind=eng)
    Base = BudgetDB.base

    # TODO: test schema connection, graph schema, and query schema

    return BudgetDB.engine_name

