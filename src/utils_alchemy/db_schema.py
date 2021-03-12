from utils_alchemy.config import *

def make_db(data_paths):
    logging.info('make_db() Creating and writing SQL Alchemy Database')
    # https://stackoverflow.com/questions/6473925/sqlalchemy-getting-a-list-of-tables
    BudgetDB.base.metadata.create_all(BudgetDB.engine)
    schema_table_names = list(BudgetDB.base.metadata.tables.keys())
    df_2_db(data_paths=data_paths, schema_table_names=schema_table_names, engine=BudgetDB.engine)

class BudgetDB():
    base = declarative_base()
    db_name = 'budget.db'
    path = db_folder
    full_path = os.sep.join([db_folder, db_name])
    engine_name = f'sqlite:///{full_path}'
    engine = create_engine(engine_name, echo=True, connect_args={'check_same_thread': False})

class SectionDescription(BudgetDB.base):
    __tablename__ = "sectionDescription"
    section_code = Column(String, primary_key=True)
    section_description = Column(String)
    main = relationship("Budget2021", back_populates="section_description", uselist=True)

class FundDescription(BudgetDB.base):
    __tablename__ = "fundDescription"
    fund_code = Column(String, primary_key=True)
    fund_description = Column(String)
    main = relationship("Budget2021", back_populates="fund_description", uselist=True)

class TitleDescription(BudgetDB.base):
    __tablename__ = "titleDescription"
    title_code = Column(String, primary_key=True)
    title_description = Column(String)
    main = relationship("Budget2021", back_populates="title_description", uselist=True)

class DepartmentDescription(BudgetDB.base):
    __tablename__ = "departmentDescription"
    department_code = Column(Integer, primary_key=True)
    department_description = Column(String)
    main = relationship("Budget2021", back_populates="department_description", uselist=True)

class Budget2021(BudgetDB.base):
    __tablename__ = "budget2021"
    # uncomment the following line to print out this class as text
    # df = get_vars_main('budget_main.csv')
    id = Column(Integer, primary_key=True)
    fund_type = Column(String)
    department_code = Column(Integer, ForeignKey("departmentDescription.department_code"))
    fund_code = Column(String, ForeignKey("fundDescription.fund_code"))
    organization_code = Column(Integer)
    organization_description = Column(String)
    division_code = Column(String)
    division_description = Column(String)
    section_code = Column(String, ForeignKey("sectionDescription.section_code"))
    sub_section_code = Column(Integer)
    sub_section_description = Column(String)
    schedule_grade = Column(String)
    bargaining_unit = Column(Integer)
    title_code = Column(String, ForeignKey("titleDescription.title_code"))
    budgeted_unit = Column(String)
    position_control = Column(Integer)
    budgeted_pay_rate = Column(Integer)
    total_budgeted_amount = Column(Integer)
    total_budgeted_unit_people = Column(String)
    total_budgeted_unit_time = Column(String)

    section_description = relationship("SectionDescription", back_populates="main", uselist=True)
    fund_description = relationship("FundDescription", back_populates="main", uselist=True)
    title_description = relationship("TitleDescription", back_populates="main", uselist=True)
    department_description = relationship("DepartmentDescription", back_populates="main", uselist=True)
