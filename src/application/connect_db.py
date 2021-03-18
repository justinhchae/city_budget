from src.application.config import *
from src.utils_alchemy.db_schema import BudgetDB, Budget2021
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

def data_frame(query, columns):
    """
    Takes a sqlalchemy query and a list of columns, returns a dataframe.
    Ref: http://danielweitzenfeld.github.io/passtheroc/blog/2014/10/12/datasci-sqlalchemy/
    """
    def make_row(x):
        return dict([(c, getattr(x, c)) for c in columns])
    return pd.DataFrame([make_row(x) for x in query])

def connect_db_session(db_path):
    # the key attrs
    path = "sqlite:///{}/{}".format(db_path, db_name)

    # instead create sql engine
    engine = create_engine(path, connect_args={'check_same_thread': False})
    # next line not currently in use, but might need it later
    Session = sessionmaker(bind=engine)
    session = Session()

    # test = pd.read_sql('SELECT * FROM budget2021', engine)
    test = pd.read_sql_table(table_name='budget2021', con=session.connection(), index_col='id')
    # st.write(test)

    res = session.query(Budget2021).filter(Budget2021.total_budgeted_amount > 210000).limit(10).all()

    df = data_frame(res, [c.name for c in Budget2021.__table__.columns])

    st.write(df)

    #TODO: back populate ORM relationships

    return engine

def check_connectivity(x):
    x.query()


    return x

