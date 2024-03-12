from copy import copy
import json

import pandas as pd
import numpy as np
from datetime import timedelta

from utils import load_config

def adjust_probability(probability, factor):
    """
    Adjusts the probability based on a multiplication factor for the odds.

    Parameters:
    - probability: A decimal representing the initial chance (e.g., 0.10 for 10%).
    - factor: The multiplication factor for the odds (e.g., 2 for double).

    Returns:
    - A new decimal chance after adjusting the odds.
    """
    # Convert probability to odds
    odds = probability / (1 - probability)

    # Multiply the odds by the given factor
    adjusted_odds = odds * factor

    # Convert the adjusted odds back to probability
    adjusted_probability = adjusted_odds / (1 + adjusted_odds)

    return adjusted_probability


def create_patients_and_demographics(n, start_date='2020-01-01', end_date='2023-01-01'):
    """Create Tables 1 (Patients) and 2 (Demographics) with n patients."""
    patients_list = []
    demographics_list = []

    for i in range(1, n+1):
        abdate = pd.to_datetime(np.random.choice(pd.date_range(start=start_date, end=end_date, freq='D')))
        cohort = np.random.choice(['RD', 'BC'])
        abdate_6m = abdate + timedelta(days=180)
        age = np.random.randint(18, 100)
        gend = np.random.choice(['M', 'F'])
        ethnicity = np.random.choice(['White', 'Asian', 'Black', 'Other'])
        height_cm = np.random.randint(140, 200)
        weight_kg = np.random.randint(40, 150)

        #Todo move to config
        ab_chance = 0.5
        # Adjust chance so women are more likely to be AB positive
        if gend == 'F':
            ab_chance = adjust_probability(ab_chance, factor=10)

        # Adjust chance so for every year over 60 the chance of being AB positive
        # decreases by 5%
        ab_chance = adjust_probability(
            ab_chance,
            factor=max(0.1, min(1 - (age - 60) * 0.05, 1))
        )

        ab_status = np.random.choice([True, False], p=[ab_chance, 1-ab_chance])

        patients_list.append([i, abdate, cohort, ab_status, abdate_6m])
        demographics_list.append([i, age, gend, ethnicity, height_cm, weight_kg])

    patients_df = pd.DataFrame(
        patients_list,
        columns=['NEWNHSNO', 'ABDATE', 'COHORT', 'AB_STATUS', 'ABDATE_6M']
    )
    demographics_df = pd.DataFrame(
        demographics_list,
        columns=['NEWNHSNO', 'AGE', 'GEND', 'ETHNICITY', 'HEIGHT_CM', 'WEIGHT_KG']
    )

    return patients_df, demographics_df


def populate_surveydata(patients):
    """
    Populate the SurveyData table based on patient data.
    """
    surveydata = pd.DataFrame({
        'NEWNHSNO': patients['NEWNHSNO'],
        'VACCDOSE_AT_TEST': np.random.randint(0, 4, size=len(patients)),  # Assuming 0-3 doses
        'VACCGROUP_AT_TEST': np.random.choice(['MRNA_AZ_ONLY', 'MRNA_ONLY', 'AZ_ONLY', 'OTHER'], size=len(patients)),
        'NADULTS': np.random.randint(1, 6, size=len(patients)),
        'NCHILD': np.random.randint(0, 5, size=len(patients)),
        'EMPLOYMENT': np.random.choice(['Employed/Education', 'Retired or not in Employment/Education'], size=len(patients)),
        'WORK_SPACE_NUMBERS': np.random.choice(['Alone/Home', '1-2', '3-6', '7-10', '10+'], size=len(patients)),
        'WORK_TRAVEL_GROUP': np.random.choice(['Private', 'Shared', 'Mix'], size=len(patients)),
        'SHIELD': np.random.choice(['Yes but attend work', 'Yes strict but attend work', 'Yes strict', 'No'], size=len(patients)),
        'FACEMASK': np.random.choice(['No', 'Yes at work/school only', 'Yes other situations only', 'Yes work/school and other situations', 'Yes for other reasons'], size=len(patients)),
        'GAD7': np.random.randint(0, 22, size=len(patients)),  # GAD7 scores range from 0 to 21
        'PHQ8': np.random.randint(0, 24, size=len(patients)),  # PHQ8 scores range from 0 to 24
        'COVID_INFECT': np.random.choice(['Positive Test', 'Doctor Suspicions', 'Own Suspicions', 'No'], size=len(patients)),
        'COVID_WORRIED': np.random.choice(['Extremely', 'Very', 'Somewhat', 'Not Very', 'Not At All'], size=len(patients)),
        'COVID_PERSONAL_RISK': np.random.choice(['Major', 'Moderate', 'Minor', 'No'], size=len(patients)),
        'COVID_UK_RISK': np.random.choice(['Major', 'Moderate', 'Minor', 'No'], size=len(patients)),
        'ST1_IMMUNITY': np.random.choice([True, False], size=len(patients)),
        'ST2_AB_STATUS_IMPORTANCE': np.random.choice(['Very', 'Fairly', 'Not Very', 'Not At All'], size=len(patients)),
        'ST3_TEST_CONCERN': np.random.choice(['Very', 'Fairly', 'Not Very', 'Not At All'], size=len(patients)),
        'ST4_RESULT_CONCERN': np.random.choice(['Very', 'Fairly', 'Not Very', 'Not At All'], size=len(patients)),
    })

    return surveydata


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

        # If patient is not AB positive, increase chance of infection
        if row['AB_STATUS'] == False:
            patient_chance = adjust_probability(chance, factor=10)
        else:
            patient_chance = chance

        # Add a confound of COHORT
        if row['COHORT'] == 'RD':
            patient_chance = adjust_probability(patient_chance, factor=2)

        # Random chance of at least one infection event
        gets_infection = np.random.rand() < patient_chance

        while current_date < max_date and gets_infection:

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

            # Update gets_infection for second occurance
            gets_infection = np.random.rand() < patient_chance

    return pd.DataFrame(infections)


def populate_therapeutics(infections, chance=0.2):
    """
    Populate Table 4 (Therapeutics) based on infections data.
    Assumption is that only people who have registered positive test get an infection.
    """
    therapeutics = []

    received_counts = {}

    # Iterate through first infection episodes only.
    for idx, row in infections[infections['INFECTION_NUM'] == 1].iterrows():
        if np.random.rand() < chance:

            if row['NEWNHSNO'] in received_counts:
                received_counts[row['NEWNHSNO']] += 1
            else:
                received_counts[row['NEWNHSNO']] = 1

            therapeutics.append({
                'NEWNHSNO': row['NEWNHSNO'],
                'THERAPEUTIC_NUM': received_counts[row['NEWNHSNO']],
                'RECEIVED': row['SPECIMEN_DATE'] + timedelta(days=np.random.randint(
                    1, 5)),
                'INTERVENTION': np.random.choice(['A', 'B', 'C', 'D', 'E', 'F'])
            })
    return pd.DataFrame(therapeutics)


def generate_icd10_list(length, sample_codes, sample_chance):
    """
    Generates a list of ICD-10 codes.

    Parameters:
    - length: int, the number of ICD-10 codes to generate.
    - sample_codes: list, a list of sample ICD-10 codes to potentially include.
    - sample_chance: float, the probability (0 to 1) of including a code from the sample_codes in the list.

    Returns:
    - list of generated ICD-10 codes.
    """
    # Example list of ICD-10 codes to fill the list with random codes, if not picked from sample_codes
    all_icd10_codes = ['A00', 'B00', 'C00', 'D00', 'E00', 'F00', 'G00', 'H00', 'I00', 'J00', 'K00', 'L00', 'M00', 'N00',
                       'O00', 'P00', 'Q00', 'R00', 'S00', 'T00', 'U00', 'V00', 'W00', 'X00', 'Y00', 'Z00']
    available_sample_codes = copy(sample_codes)
    generated_list = []
    for _ in range(length):
        if available_sample_codes and np.random.rand() < sample_chance:
            # Pick from the sample_codes
            code = np.random.choice(available_sample_codes)
            available_sample_codes.remove(code)
        else:
            # Pick from the larger set of ICD-10 codes
            root_code = np.random.choice(all_icd10_codes)
            code = root_code + str(np.random.randint(1, 9))
        generated_list.append(code)

    return generated_list


def populate_hospitalisations(patients, code_list, chance=0.2):
    """
    Populate Table 5 (Hospitalisations).
    """
    hospitalisations = []
    for idx, row in patients.iterrows():

        active_date = copy(row['ABDATE'])

        # If patient is not AB positive, increase chance of hospitalisation
        if row['AB_STATUS'] == False:
            patient_chance = adjust_probability(chance, factor=10)
        else:
            patient_chance = chance

        # Random chance of at least one hospitalisation event
        has_hospitalisation = np.random.rand() < patient_chance

        while active_date < row['ABDATE_6M'] and has_hospitalisation:
            admission_date = active_date + timedelta(days=np.random.randint(1, 179))

            admission_len_days = int(np.random.poisson(5, 1)[0])
            number_of_episodes = int(np.random.poisson(1, 1)[0] + 1)
            discharge_date = admission_date + timedelta(days=admission_len_days)

            if admission_len_days == 0:
                admission_len_binned = '<24hrs'
            elif admission_len_days <= 7:
                admission_len_binned = '1-7days'
            else:
                admission_len_binned = '>1week'

            # Create diag codes
            diag_codes = []
            diag_code_match = False
            for _ in range(number_of_episodes):
                episode_code_list = generate_icd10_list(
                    length=np.random.randint(1, 5),
                    sample_codes=code_list,
                    sample_chance=0.1
                )
                if not diag_code_match:
                    diag_code_match = any(item in episode_code_list for item in code_list)
                diag_codes.append(episode_code_list)

            cc_admission = np.random.choice([0, 1], p=[0.95, 0.05])

            hospitalisations.append({
                'NEWNHSNO': row['NEWNHSNO'],
                'ADMIDATE_DV': admission_date,
                'DISDATE_DV': discharge_date,
                'EPISODE_COUNT': number_of_episodes,
                'ADMI_LEN': admission_len_days,
                'ADMI_LEN_BINNED': admission_len_binned,
                'xDIAGCONCAT': json.dumps(diag_codes),
                'xOPERCONCAT': json.dumps(diag_codes), # Reusing dummy diag codes
                'DIAG_CODE_MATCH': diag_code_match,
                'CC_ADMI': cc_admission,
                'CCLevel2': int(np.floor(admission_len_days*0.5)) if cc_admission else 0,
                'CCLevel3': int(np.floor(admission_len_days*0.3)) if cc_admission else 0,
                'CCBasicResp': int(np.floor(admission_len_days*0.5)) if cc_admission else 0,
                'CCAdvancedResp': int(np.floor(admission_len_days*0.2)) if cc_admission else 0,
            })
            active_date = copy(admission_date)

            # Update has_hospitalisation for second occurrence
            has_hospitalisation = np.random.rand() < patient_chance

    hospitalisations_df = pd.DataFrame(hospitalisations)

    # Remove same day duplicates
    hospitalisations_df = hospitalisations_df.drop_duplicates(subset=['NEWNHSNO', 'ADMIDATE_DV'])

    return hospitalisations_df


def populate_deaths(hospitalisations, code_list, chance=0.3):
    """
    Populate Table 6 (Deaths) based on hospitalisations data.
    Assumption in dummy data that only those that have been hospitalised can die.
    """
    last_hospitalisations = hospitalisations.copy(
        deep=True
    ).sort_values(
        by=['NEWNHSNO', 'ADMIDATE_DV'], ascending=False
    ).drop_duplicates(
        subset=['NEWNHSNO']
    )

    deaths = []
    for idx, row in last_hospitalisations.iterrows():

        if np.random.rand() < chance:
            # Generate underlying cause of death
            icd10_list = generate_icd10_list(
                length=3,
                sample_codes=code_list,
                sample_chance=0.2
            )
            icdu = icd10_list[0]
            code_mentioned = any([code in icd10_list for code in code_list])
            code_underlying = icdu in code_list
            deaths.append({
                'NEWNHSNO': row['NEWNHSNO'],
                'DOD': row['ADMIDATE_DV'] + timedelta(days=np.random.randint(1, 30)),
                'ICDU_GROUP': np.random.choice(['Group1', 'Group2', 'Group3']),
                'ICDU': icdu,
                'CODE_MENTIONED': code_mentioned,
                'CODE_UNDERLYING': code_underlying,
                'CODE_POSITION': next((icd10_list.index(item) + 1 for item in icd10_list if item in code_list), None)
            })
    return pd.DataFrame(deaths)


def drop_events_after_deaths(event_df_in, deaths_df_in, event_date_col, death_date_col='DOD'):
    """
    Drop event dates that occurred after a death.

    Parameters:
    - event_df_in: DataFrame containing events.
    - deaths_df_in: DataFrame containing death records.
    - event_date_col: The name of the column in event_df that contains the event dates.
    - death_date_col: The name of the column in deaths_df that contains the death dates. Defaults to 'DOD'.

    Returns:
    - A DataFrame containing only the events that occurred on or before the death dates.
    """
    # Create copies to avoid inplace edits
    event_df = event_df_in.copy(deep=True)
    deaths_df = deaths_df_in.copy(deep=True)

    # Merge and filter
    merged_df = pd.merge(event_df, deaths_df[['NEWNHSNO', death_date_col]], on='NEWNHSNO', how='left')
    filtered_df = merged_df[
        (merged_df[event_date_col] <= merged_df[death_date_col]) | (pd.isna(merged_df[death_date_col]))
    ]

    # Drop the death date column from the filtered dataframe to return to original event_df structure
    return filtered_df[event_df_in.columns]


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
    code_list = config['dummy_data']['code_list']
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
    surveydata = populate_surveydata(patients)
    infections = populate_infections(patients, chance=infection_chance)
    therapeutics = populate_therapeutics(infections, chance=therapeutic_chance)
    hospitalisations = populate_hospitalisations(patients, code_list, chance=hospitalisation_chance)
    deaths = populate_deaths(hospitalisations, code_list, chance=death_chance)

    # Convert all datetime columns to date-only format
    patients = datetime_cols_to_date(patients)
    surveydata = datetime_cols_to_date(surveydata)
    demographics = datetime_cols_to_date(demographics)
    infections = datetime_cols_to_date(infections)
    therapeutics = datetime_cols_to_date(therapeutics)
    hospitalisations = datetime_cols_to_date(hospitalisations)
    deaths = datetime_cols_to_date(deaths)

    # Drop events after deaths
    infections = drop_events_after_deaths(infections, deaths, 'SPECIMEN_DATE')
    therapeutics = drop_events_after_deaths(therapeutics, deaths, 'RECEIVED')
    hospitalisations = drop_events_after_deaths(hospitalisations, deaths, 'ADMIDATE_DV')

    print("Dropping rows outside study period")
    infections = drop_rows_outside_study_period(infections, patients, 'SPECIMEN_DATE')
    therapeutics = drop_rows_outside_study_period(therapeutics, patients, 'RECEIVED')
    hospitalisations = drop_rows_outside_study_period(hospitalisations, patients, 'ADMIDATE_DV')
    deaths = drop_rows_outside_study_period(deaths, patients, 'DOD')

    df_dict = {
        'patients': patients,
        'surveydata': surveydata,
        'demographics': demographics,
        'infections': infections,
        'therapeutics': therapeutics,
        'hospitalisations': hospitalisations,
        'deaths': deaths
    }
    return df_dict

if __name__ == '__main__':
    dataframes = create_dummy_dataframes()