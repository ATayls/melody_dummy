import os

from sqlalchemy import Column, Integer, String, Date, Boolean, ForeignKey, UniqueConstraint, CheckConstraint, Enum
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
    surveydata = relationship("SurveyData", back_populates="patient", uselist=False)
    infections = relationship("Infections", back_populates="patient")
    therapeutics = relationship("Therapeutics", back_populates="patient")
    hospitalisations = relationship("Hospitalisations", back_populates="patient")
    death = relationship("Deaths", back_populates="patient", uselist=False)


# Define the Demographics table
class Demographics(Base):
    __tablename__ = 'demographics'
    NEWNHSNO = Column(Integer, ForeignKey('patients.NEWNHSNO'), primary_key=True)
    AGE = Column(Integer, nullable=True)
    GEND = Column(Enum('Male', 'Female', 'Other', name='gend_enum'), nullable=True)
    ETHNICITY = Column(Enum('White', 'Asian', 'Black', 'Other', name='ethnic_enum'), nullable=True)
    HEIGHT_CM = Column(Integer)
    WEIGHT_KG = Column(Integer)

    # Relationship
    patient = relationship("Patients", back_populates="demographics")


# Define a table with more survey data
class SurveyData(Base):
    __tablename__ = 'surveydata'
    NEWNHSNO = Column(Integer, ForeignKey('patients.NEWNHSNO'), primary_key=True)

    # Vaccine
    VACCDOSE_AT_TEST = Column(Integer, nullable=False)
    VACCGROUP_AT_TEST = Column(Enum('MRNA_AZ_ONLY', 'MRNA_ONLY', 'AZ_ONLY', 'OTHER', name='vaccgroup_enum'), nullable=True)

    # General
    NADULTS = Column(Integer)
    NCHILD = Column(Integer)
    EMPLOYMENT = Column(Enum('Employed/Education', 'Retired or not in Employment/Education', name='employment_enum'), nullable=True)
    WORK_SPACE_NUMBERS = Column(Enum('Alone/Home', '1-2', '3-6', '7-10', '10+', name='workspace_enum'), nullable=True)
    WORK_TRAVEL_GROUP = Column(Enum('Private', 'Shared', 'Mix', name='worktravel_enum'), nullable=True)

    # Behaviours
    SHIELD = Column(Enum('Yes but attend work', 'Yes strict but attend work', 'Yes strict', 'No', name='shield_enum'), nullable=True)
    FACEMASK = Column(Enum('No', 'Yes at work/school only', 'Yes other situations only', 'Yes work/school and other situations', 'Yes for other reasons', name='shield_enum'), nullable=True)

    # Mental Health
    GAD7 = Column(Integer)
    PHQ8 = Column(Integer)

    # Covid Opinions
    COVID_INFECT = Column(Enum('Positive Test', 'Doctor Suspicions', 'Own Suspicions', 'No', name='covid_infect_enum'), nullable=True)
    COVID_WORRIED = Column(Enum('Extremely', 'Very', 'Somewhat', 'Not Very', 'Not At All', name='covid_worried_enum'), nullable=True)
    COVID_PERSONAL_RISK = Column(Enum('Major', 'Moderate', 'Minor', 'No', name='covid_personal_risk_enum'), nullable=True)
    COVID_UK_RISK = Column(Enum('Major', 'Moderate', 'Minor', 'No', name='covid_uk_risk_enum'), nullable=True)
    ST1_IMMUNITY = Column(Boolean, nullable=True)
    ST2_AB_STATUS_IMPORTANCE = Column(Enum('Very', 'Fairly', 'Not Very', 'Not At All', name='st2_enum'), nullable=True)
    ST3_TEST_CONCERN = Column(Enum('Very', 'Fairly', 'Not Very', 'Not At All', name='st3_enum'), nullable=True)
    ST4_RESULT_CONCERN = Column(Enum('Very', 'Fairly', 'Not Very', 'Not At All', name='st4_enum'), nullable=True)

    # Table Relationship
    patient = relationship("Patients", back_populates="surveydata")


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
    THERAPEUTIC_NUM = Column(Integer, nullable=False, primary_key=True) # Make THERAPEUTIC_NUM part of the primary key
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
    ICDU_GROUP = Column(String(50), nullable=True)
    ICDU = Column(String(5), nullable=True)
    CODE_MENTIONED = Column(Boolean, nullable=False)
    CODE_UNDERLYING = Column(Boolean, nullable=False)
    CODE_POSITION = Column(Integer, nullable=True)

    __table_args__ = (
        CheckConstraint('NOT CODE_UNDERLYING OR CODE_MENTIONED',
                        name='check_code_mentioned_if_underlying'),
        CheckConstraint('CODE_MENTIONED = (CODE_POSITION IS NOT NULL)',
                        name='check_code_position_existance_based_on_code_mentioned')
    )

    # Relationship
    patient = relationship("Patients", back_populates="death")


def get_yes_or_no_input(prompt="Please enter 'y' for yes or 'n' for no: "):
    while True:
        user_input = input(prompt).lower()  # Convert to lowercase to make the check case-insensitive
        if user_input == 'y':
            return True
        elif user_input == 'n':
            return False
        else:
            print("Invalid input. Please enter 'y' for yes or 'n' for no.")


# Function to create the database and tables
def create_database(conn_string, overwrite=False):
    manager = DBEngineContextManager(conn_string, db_should_exist=False)
    if not overwrite and manager.exists:
        raise FileExistsError(f"Database already exists at {conn_string}. Set overwrite=True to replace it.")

    print(f"Creating database at {conn_string}")
    with manager as engine:
        if overwrite and get_yes_or_no_input("WARNING Overwriting existing DB, proceed? (y/n):"):
            print("Dropping existing tables")
            try:
                Base.metadata.drop_all(engine)
            except OperationalError:
                print("Unable to drop tables; please check your connection string and database permissions.")
        Base.metadata.create_all(engine)
