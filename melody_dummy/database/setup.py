from sqlalchemy import Column, Integer, String, Date, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError

from melody_dummy.database.db_utils import DBEngineContextManager

# Define the base class
Base = declarative_base()

# Define the Patients table
class Patients(Base):
    __tablename__ = 'Patients'
    NEWNHSNO = Column(Integer, primary_key=True, unique=True)
    ABDATE = Column(Date, nullable=False)
    ABDATE_6MONTHS = Column(Date, nullable=False)
    COHORT = Column(String, nullable=False)
    AB_STATUS = Column(Boolean, nullable=False)

# Define the Demographics table
class Demographics(Base):
    __tablename__ = 'Demographics'
    NEWNHSNO = Column(Integer, ForeignKey('Patients.NEWNHSNO'), primary_key=True, unique=True)
    DOB = Column(Date, nullable=False)
    SEX = Column(String, nullable=False)

# Define the Infections table
class Infections(Base):
    __tablename__ = 'Infections'
    NEWNHSNO = Column(Integer, ForeignKey('Patients.NEWNHSNO'), primary_key=True)
    SPECIMEN_DATE = Column(Date, nullable=False)
    EPISODE_NUM = Column(Integer, nullable=False)
    INFECTION_NUM = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint('NEWNHSNO', 'EPISODE_NUM', 'INFECTION_NUM'),)

# Define the Therapeutics table
class Therapeutics(Base):
    __tablename__ = 'Therapeutics'
    NEWNHSNO = Column(Integer, ForeignKey('Patients.NEWNHSNO'), primary_key=True)
    RECEIVED = Column(Date, nullable=False)
    INTERVENTION = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint('NEWNHSNO', 'RECEIVED', 'INTERVENTION'),)

# Define the Hospitalisations table
class Hospitalisations(Base):
    __tablename__ = 'Hospitalisations'
    NEWNHSNO = Column(Integer, ForeignKey('Patients.NEWNHSNO'), primary_key=True)
    ADMIDATE_DV = Column(Date, nullable=False)
    EPISODE_COUNT = Column(Integer, nullable=False)
    ADMI_LEN = Column(Integer)
    __table_args__ = (UniqueConstraint('NEWNHSNO', 'ADMIDATE_DV'),)

# Define the Deaths table
class Deaths(Base):
    __tablename__ = 'Deaths'
    NEWNHSNO = Column(Integer, ForeignKey('Patients.NEWNHSNO'), primary_key=True, unique=True)
    DOD = Column(Date, nullable=False)
    ICDU_GROUP = Column(String)
    ICD10 = Column(String)
    COVID_MENTIONED = Column(Boolean, nullable=False)
    COVID_UNDERLYING = Column(Boolean, nullable=False)


# Function to create the database and tables
def create_database(conn_string, overwrite=False):
    print(f"Creating database at {conn_string}")
    with DBEngineContextManager(conn_string, db_should_exist=False) as engine:
        if overwrite:
            print("Dropping existing tables")
            try:
                Base.metadata.drop_all(engine)
            except OperationalError:
                print("Unable to drop tables; please check your connection string and database permissions.")
        Base.metadata.create_all(engine)