from src.application.config import *
from src.utils_alchemy.db_schema import BudgetDB
from src.utils_alchemy.config import sessionmaker, create_engine, db_name

import contextlib

def connectivity(engine):
    # https://docs.sqlalchemy.org/en/13/core/connections.html
    connection = None

    @contextlib.contextmanager
    def connect():
        nonlocal connection

        if connection is None:
            connection = engine.connect()
            with connection:
                with connection.begin():
                    yield connection
        else:
            yield connection

    return connect

def connect_db_session():
    try:
        dirPath = os.path.dirname(os.path.realpath(__file__))
    except:
        dirPath = os.getcwd()

    # the key attrs
    path = "sqlite:///{}/{}".format(dirPath, db_name)

    # this leads to an error
    # con = sqlite3.connect(path)
    # instead create engine
    engine = create_engine(path, connect_args={'check_same_thread': False})
    # Session = sessionmaker(bind=engine)
    # Base = BudgetDB.base

    # TODO: test schema connection, graph schema, and query schema

    return engine

def check_connectivity(x):
    x.query()


    return x

