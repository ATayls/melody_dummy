from sqlalchemy import Column, Integer, String, Date, Boolean, ForeignKey, UniqueConstraint, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import relationship

from database.db_utils import DBEngineContextManager


# Define the base class
Base = declarative_base()


# Define the Patients table
class Patients(Base):
    __tablename__ = 'patients'
    NEWNHSNO = Column(Integer, primary_key=True)
    ABDATE = Column(Date, nullable=False)
    ABDATE_6M = Column(Date, nullable=False)
    COHORT = Column(String(2), nullable=False)
    AB_STATUS = Column(Boolean, nullable=False)

    # Relationships
    demographics = relationship("Demographics", back_populates="patient", uselist=False)
    infections = relationship("Infections", back_populates="patient")
    therapeutics = relationship("Therapeutics", back_populates="patient")
    hospitalisations = relationship("Hospitalisations", back_populates="patient")
    death = relationship("Deaths", back_populates="patient", uselist=False)


# Define the Demographics table
class Demographics(Base):
    __tablename__ = 'demographics'
    NEWNHSNO = Column(Integer, ForeignKey('patients.NEWNHSNO'), primary_key=True)
    DOB = Column(Date, nullable=False)
    SEX = Column(String(10), nullable=False)

    # Relationship
    patient = relationship("Patients", back_populates="demographics")


# Define the Infections table
class Infections(Base):
    __tablename__ = 'infections'
    NEWNHSNO = Column(Integer, ForeignKey('patients.NEWNHSNO'), primary_key=True)
    SPECIMEN_DATE = Column(Date, primary_key=True)  # Make SPECIMEN_DATE part of the primary key
    EPISODE_NUM = Column(Integer, nullable=False)
    INFECTION_NUM = Column(Integer, nullable=False)
    DAYS_SINCE_EPISODE_START = Column(Integer, nullable=False)

    # Relationship
    patient = relationship("Patients", back_populates="infections")


# Define the Therapeutics table
class Therapeutics(Base):
    __tablename__ = 'therapeutics'
    NEWNHSNO = Column(Integer, ForeignKey('patients.NEWNHSNO'), primary_key=True)
    THERAPEUTIC_NUM = Column(Integer, nullable=False, primary_key=True) # Make SPECIMEN_NUM part of the primary key
    RECEIVED = Column(Date, nullable=False)
    INTERVENTION = Column(String(20), nullable=False)
    __table_args__ = (UniqueConstraint('NEWNHSNO', 'RECEIVED', 'INTERVENTION'),)

    # Relationship
    patient = relationship("Patients", back_populates="therapeutics")


# Define the Hospitalisations table
class Hospitalisations(Base):
    __tablename__ = 'hospitalisations'
    NEWNHSNO = Column(Integer, ForeignKey('patients.NEWNHSNO'), primary_key=True)
    ADMIDATE_DV = Column(Date, nullable=False, primary_key=True)  # Make ADMIDATE_DV part of the primary key
    DISDATE_DV = Column(Date, nullable=False)
    EPISODE_COUNT = Column(Integer, nullable=False)
    ADMI_LEN = Column(Integer, nullable=True)  # Specify nullable explicitly if nulls are acceptable
    ADMI_LEN_BINNED = Column(String(20))
    xDIAGCONCAT = Column(String(512))
    xOPERCONCAT = Column(String(512))
    DIAG_CODE_MATCH = Column(Boolean)
    CC_ADMI = Column(Boolean)
    CCLevel2 = Column(Integer)
    CCLevel3 = Column(Integer)
    CCBasicResp = Column(Integer)
    CCAdvancedResp = Column(Integer)

    # Relationship
    patient = relationship("Patients", back_populates="hospitalisations")


# Define the Deaths table
class Deaths(Base):
    __tablename__ = 'deaths'
    NEWNHSNO = Column(Integer, ForeignKey('patients.NEWNHSNO'), primary_key=True, unique=True)
    DOD = Column(Date, nullable=False)
    DOB = Column(Date, nullable=False)
    AGEC = Column(Integer, nullable=False)
    ICDU_GROUP = Column(String(50), nullable=True)
    ICD10 = Column(String(5), nullable=True)
    COVID_MENTIONED = Column(Boolean, nullable=False)
    COVID_UNDERLYING = Column(Boolean, nullable=False)

    __table_args__ = (
        CheckConstraint('NOT COVID_UNDERLYING OR COVID_MENTIONED',
                        name='check_covid_mentioned_if_underlying'),
    )

    # Relationship
    patient = relationship("Patients", back_populates="death")


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