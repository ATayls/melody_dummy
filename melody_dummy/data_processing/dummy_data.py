from copy import copy

import pandas as pd
import numpy as np
from datetime import timedelta

from utils import load_config


def create_patients_and_demographics(n, start_date='2020-01-01', end_date='2023-01-01'):
    """Create Tables 1 (Patients) and 2 (Demographics) with n patients."""
    patients = pd.DataFrame({
        'NEWNHSNO': range(1, n+1),
        'ABDATE': pd.to_datetime(np.random.choice(pd.date_range(start=start_date,
                                                                end=end_date, freq='D'), n)),
        'COHORT': np.random.choice(['RD', 'BC'], n),
        'AB_STATUS': np.random.choice([True, False], n)
    })
    patients['ABDATE_6M'] = patients['ABDATE'] + timedelta(days=180)

    demographics = pd.DataFrame({
        'NEWNHSNO': range(1, n+1),
        'DOB': pd.to_datetime(np.random.choice(pd.date_range(start='1950-01-01', end='2003-01-01', freq='D'), n)),
        'SEX': np.random.choice(['M', 'F'], n)
    })
    return patients, demographics


def populate_infections(patients, chance=0.5):
    """
    Populate Table 3 (Infections) based on patients data.
    """
    infections = []
    for idx, row in patients.iterrows():

        episode_start = None
        episode_num = 1
        infection_num = 1
        current_date = row['ABDATE']
        max_date = row['ABDATE'] + timedelta(days=179)

        while np.random.rand() < chance:

            # Set infection date
            if episode_start is None:
                infection_date = current_date + timedelta(days=np.random.randint(1, 180))
                episode_start = copy(infection_date)
            else:
                infection_date = current_date + timedelta(days=np.random.randint(91, 180))

            days_since_episode_start = (infection_date - episode_start).days

            # New episode?
            if days_since_episode_start > 91:
                episode_start = copy(infection_date)
                episode_num += 1
                infection_num = 1

            tests_count = np.random.poisson(1, 1)[0] + 1
            for test_num in range(tests_count):
                if test_num == 0:
                    specimen_date = copy(infection_date)
                else:
                    specimen_date = current_date + timedelta(days=np.random.randint(
                        1, np.random.poisson(2, 1)[0] + 2
                    ))

                current_date = copy(specimen_date)
                days_since_episode_start = (specimen_date - episode_start).days

                if specimen_date > max_date:
                     break

                infections.append({
                    'NEWNHSNO': row['NEWNHSNO'],
                    'SPECIMEN_DATE': specimen_date,
                    'EPISODE_NUM': episode_num,
                    'INFECTION_NUM': infection_num,
                    'DAYS_SINCE_EPISODE_START': days_since_episode_start
                })
                infection_num += 1

    return pd.DataFrame(infections)


def populate_therapeutics(infections, chance=0.2):
    """Populate Table 4 (Therapeutics) based on infections data."""
    therapeutics = []
    for idx, row in infections.iterrows():
        if np.random.rand() < chance:
            therapeutics.append({
                'NEWNHSNO': row['NEWNHSNO'],
                'RECEIVED': row['SPECIMEN_DATE'] + timedelta(days=np.random.randint(
                    1, 5)),
                'INTERVENTION': np.random.choice(['A', 'B', 'C', 'D', 'E', 'F'])
            })
    return pd.DataFrame(therapeutics)


def populate_hospitalisations(infections, chance=0.5):
    """Populate Table 5 (Hospitalisations) based on infections data."""
    hospitalisations = []
    for idx, row in infections.iterrows():
        if np.random.rand() < chance:
            hospitalisations.append({
                'NEWNHSNO': row['NEWNHSNO'],
                'ADMIDATE_DV': row['SPECIMEN_DATE'] + timedelta(
                    days=np.random.randint(1, 14)),
                'EPISODE_COUNT': np.random.randint(1, 5),
                'ADMI_LEN': np.random.randint(1, 30)
            })
    return pd.DataFrame(hospitalisations)


def populate_deaths(hospitalisations, chance=0.3):
    """Populate Table 6 (Deaths) based on hospitalisations data."""
    deaths = []
    for idx, row in hospitalisations.iterrows():
        if np.random.rand() < chance:
            covid_mentioned = np.random.choice([True, False])
            deaths.append({
                'NEWNHSNO': row['NEWNHSNO'],
                'DOD': row['ADMIDATE_DV'] + timedelta(days=np.random.randint(1, 30)),
                'ICDU_GROUP': np.random.choice(['Group1', 'Group2', 'Group3']),
                'ICD10': 'ICD' + str(np.random.randint(10, 99)),
                'COVID_MENTIONED': covid_mentioned,
                'COVID_UNDERLYING': (np.random.choice([True,
                                     False]) if covid_mentioned else False)
            })
    return pd.DataFrame(deaths)


def drop_rows_outside_study_period(df, patient_df, date_col, study_end_col='ABDATE_6M'):
    """Drop rows from df that occur outside the study period."""

    # Merging patients with other tables to filter based on ABDATE_6M
    df = df.merge(patient_df[['NEWNHSNO', study_end_col]], on='NEWNHSNO')

    # Filtering rows where events occur after ABDATE_6M
    mask = df[date_col] <= df[study_end_col]
    df = df[mask]

    # Dropping the ABDATE_6M column as it's no longer needed
    df.drop(study_end_col, axis=1, inplace=True)
    return df


def datetime_cols_to_date(df):
    # Convert all datetime columns to date-only format
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.date
    return df


def create_dummy_dataframes(
        n_patients=10000, start_date='2021-01-01', end_date='2022-01-01'
) -> dict:
    """Create dummy data for the project."""

    # Load config file
    config = load_config()

    # Load configurations
    n_patients = config['dummy_data']['n_patients']
    start_date = config['dummy_data']['start_date']
    end_date = config['dummy_data']['end_date']
    infection_chance = config['dummy_data']['infection_chance']
    therapeutic_chance = config['dummy_data']['therapeutic_chance']
    hospitalisation_chance = config['dummy_data']['hospitalisation_chance']
    death_chance = config['dummy_data']['death_chance']
    print(f"Creating dummy data with the following config {config['dummy_data']}")

    # Creating Tables 1 and 2
    patients, demographics = create_patients_and_demographics(
        n_patients,
        start_date,
        end_date
    )

    # Populating other tables based on patients
    infections = populate_infections(patients, chance=infection_chance)
    therapeutics = populate_therapeutics(infections, chance=therapeutic_chance)
    hospitalisations = populate_hospitalisations(infections, chance=hospitalisation_chance)
    deaths = populate_deaths(hospitalisations, chance=death_chance)

    # Convert all datetime columns to date-only format
    patients = datetime_cols_to_date(patients)
    demographics = datetime_cols_to_date(demographics)
    infections = datetime_cols_to_date(infections)
    therapeutics = datetime_cols_to_date(therapeutics)
    hospitalisations = datetime_cols_to_date(hospitalisations)
    deaths = datetime_cols_to_date(deaths)

    print("Dropping rows outside study period")
    infections = drop_rows_outside_study_period(infections, patients, 'SPECIMEN_DATE')
    therapeutics = drop_rows_outside_study_period(therapeutics, patients, 'RECEIVED')
    hospitalisations = drop_rows_outside_study_period(hospitalisations, patients, 'ADMIDATE_DV')
    deaths = drop_rows_outside_study_period(deaths, patients, 'DOD')

    df_dict = {
        'patients': patients,
        'demographics': demographics,
        'infections': infections,
        'therapeutics': therapeutics,
        'hospitalisations': hospitalisations,
        'deaths': deaths
    }
    return df_dict

if __name__ == '__main__':
    dataframes = create_dummy_dataframes()