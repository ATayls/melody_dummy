from database.db_utils import sql_read_to_pandas


def get_full_table_as_dataframe(conn_string, table_name):
    """
    Returns all patients from the database as a DataFrame.
    """
    sql_query = f"SELECT * FROM {table_name}"
    return sql_read_to_pandas(sql_query, conn_string)


def follow_up_time(conn_string):
    sql_query = """
    SELECT
        cohort,
        ROUND(SUM(julianday(ABDATE_6M) - julianday(ABDATE)) / 365, 2) AS total_follow_up_years,
        ROUND(AVG(julianday(ABDATE_6M) - julianday(ABDATE)) / 365, 2) AS mean_follow_up_years
    FROM
        Patients
    GROUP by
        cohort;
    """
    return sql_read_to_pandas(sql_query, conn_string)


def get_therapeutics_and_closest_prior_infections(conn_string):
    sql_query = """
    SELECT
        t.*,
        (
            SELECT
                SPECIMEN_DATE
            FROM
                infections AS i
            WHERE
                i.NEWNHSNO = t.NEWNHSNO
                AND i.INFECTION_NUM = 1
                AND i.SPECIMEN_DATE <= t.RECEIVED
            ORDER BY
                i.SPECIMEN_DATE DESC
            LIMIT 1
        ) AS CLOSEST_PRIOR_EPISODE_START_DATE,
        (
            SELECT
                SPECIMEN_DATE
            FROM
                infections AS i
            WHERE
                i.NEWNHSNO = t.NEWNHSNO
                AND i.SPECIMEN_DATE <= t.RECEIVED
            ORDER BY
                i.SPECIMEN_DATE DESC
            LIMIT 1
        ) AS CLOSEST_PRIOR_INFECTION_DATE
    FROM
        therapeutics t;
    """

    return sql_read_to_pandas(sql_query, conn_string)


def get_hospitalisations_post_infection_within_days(conn_string, days):
    """
    Retrieves hospitalisations that occur within a specified number of days after an infection.

    :param conn_string: Database connection string
    :param days: Number of days to consider after the infection date
    :return: DataFrame containing the query results
    """
    all_hospitalisations = get_hospitalisations_with_closest_prior_infection(conn_string)
    filtered_hospitalisations = all_hospitalisations[
        all_hospitalisations['INFECTION_EPISODE_START_TO_ADMI_DAYS'] <= days
    ]
    return filtered_hospitalisations


def get_valid_hospitalisations(conn_string, days):
    """
    Retrieves hospitalisations either have an infection episode start within X days or
    have a diag code match
    """
    all_hospitalisations = get_hospitalisations_with_closest_prior_infection(conn_string)
    within_days = all_hospitalisations['INFECTION_EPISODE_START_TO_ADMI_DAYS'] <= days
    all_hospitalisations[f'INFECTION_WITHIN_{days}_DAYS_PRIOR'] = within_days
    filtered_hospitalisations = all_hospitalisations[
        within_days | all_hospitalisations['DIAG_CODE_MATCH']
    ]
    return filtered_hospitalisations


def get_hospitalisations_with_closest_prior_infection(conn_string):
    """
    Retrieves all hospitalisations and a column describing the closest prior infection
    Inclusive of specimens taken on the same day as the hospitalisation.
    First Infection of Infection Episode only.
    :param conn_string: Database connection string
    :return: DataFrame containing the query results
    """
    sql_query = """
    WITH HospitalisationClosestPriorInfection AS ( 
        SELECT 
            h.*, 
            (
                SELECT 
                    SPECIMEN_DATE
                FROM 
                    infections i 
                WHERE 
                    i.NEWNHSNO = h.NEWNHSNO 
                    AND i.INFECTION_NUM = 1
                    AND i.SPECIMEN_DATE <= h.ADMIDATE_DV
                ORDER BY 
                    i.SPECIMEN_DATE DESC
                LIMIT 1
            ) AS CLOSEST_PRIOR_EPISODE_START_DATE
        FROM 
            hospitalisations h
    )
    SELECT 
        hcpi.*,
        (
            julianday(hcpi.ADMIDATE_DV) - julianday(hcpi.CLOSEST_PRIOR_EPISODE_START_DATE)
        ) AS INFECTION_EPISODE_START_TO_ADMI_DAYS
    FROM 
        HospitalisationClosestPriorInfection AS hcpi
    ;
    """

    return sql_read_to_pandas(sql_query, conn_string)


def get_patients_with_first_dates(conn_string):
    """
    Retrieves all patients with first date for:
        infection (episode start only)
        therapy
        hospitalisation (all cause)
        hospitalisation (valid covid)
    :param conn_string: Database connection string
    :return: DataFrame containing the query results
    """
    sql_query = """
    SELECT
        patients.*,
        (
            SELECT RECEIVED
            FROM therapeutics
            WHERE therapeutics.NEWNHSNO = patients.NEWNHSNO
            ORDER BY RECEIVED 
            LIMIT 1
        ) AS first_therapy,
        (
            SELECT SPECIMEN_DATE
            FROM infections
            WHERE infections.NEWNHSNO = patients.NEWNHSNO
            AND infections.INFECTION_NUM = 1
            ORDER BY SPECIMEN_DATE 
            LIMIT 1
        ) AS first_infection,
        (
            SELECT ADMIDATE_DV
            FROM hospitalisations
            WHERE hospitalisations.NEWNHSNO = patients.NEWNHSNO
            ORDER BY ADMIDATE_DV 
            LIMIT 1
        ) AS first_hospitalisation,
        (
            SELECT ADMIDATE_DV
            FROM covid_admissions
            WHERE covid_admissions.NEWNHSNO = patients.NEWNHSNO
            ORDER BY ADMIDATE_DV 
            LIMIT 1
        ) AS first_covid_hospitalisation,
        d.DOD AS all_cause_death,
        cd.DOD AS covid_death
    FROM
        patients
    LEFT JOIN deaths d ON patients.NEWNHSNO = d.NEWNHSNO
    LEFT JOIN covid_deaths cd ON patients.NEWNHSNO = cd.NEWNHSNO
    ;
    """

    return sql_read_to_pandas(sql_query, conn_string)


def get_deaths_with_closest_prior_infection(conn_string):
    """
    Retrieves all deaths and a column describing the closest prior infection
    Inclusive of specimens taken on the same day as the DOD.
    First Infection of Infection Episode only.
    :param conn_string: Database connection string
    :return: DataFrame containing the query results
    """
    sql_query= """
    WITH DeathsClosestPriorInfection AS (
        SELECT
            d.*,
            (
                SELECT
                    SPECIMEN_DATE
                FROM
                    infections i
                WHERE
                    i.NEWNHSNO = d.NEWNHSNO
                    AND i.INFECTION_NUM = 1
                    AND i.SPECIMEN_DATE <= d.DOD
                ORDER BY
                    i.SPECIMEN_DATE DESC
                LIMIT 1
            ) AS CLOSEST_PRIOR_EPISODE_START_DATE
        FROM
            deaths d
    )
    SELECT
        dcpi.*,
        (
            julianday(dcpi.DOD) - julianday(dcpi.CLOSEST_PRIOR_EPISODE_START_DATE)
        ) AS INFECTION_EPISODE_START_TO_DOD_DAYS
    FROM
        DeathsClosestPriorInfection AS dcpi
    ;
    """

    return sql_read_to_pandas(sql_query, conn_string)


def get_valid_deaths(conn_string, days):
    """
    Retrieves deaths either have an infection episode start within X days or
    have a diag code match in underlying
    """
    all_deaths = get_deaths_with_closest_prior_infection(conn_string)
    within_days = all_deaths['INFECTION_EPISODE_START_TO_DOD_DAYS'] <= days
    all_deaths[f'INFECTION_WITHIN_{days}_DAYS_PRIOR'] = within_days
    filtered_hospitalisations = all_deaths[
        within_days | all_deaths['CODE_UNDERLYING']
    ]
    return filtered_hospitalisations


def get_patient_event_dates(conn_string, newnhsno):
    """
    Retrieves event dates for a specific nhsno
    events from hospitalisation, infections and deaths tables.
    Additional column 'InCovidView' To indicate whether the hosp or infection events are in there respective
        covid_deaths or covid_hospitalisation views
    Treats hospitalisation admissions and discharges seperately for a timeline view.
    Orders by date and has specific ordering (admission - infect - therapy - death -discharge)
        for events within the same day.
    """
    sql_query = """
        WITH PatientEvents AS (
        SELECT 
            h.NEWNHSNO AS PatientID,
            h.ADMIDATE_DV AS EventDate,
            'HES Admission' AS EventTable,
            CASE WHEN ca.NEWNHSNO IS NOT NULL THEN TRUE ELSE FALSE END AS InCovidView
        FROM 
            hospitalisations h
        LEFT JOIN
            covid_admissions ca ON h.NEWNHSNO = ca.NEWNHSNO AND h.ADMIDATE_DV = ca.ADMIDATE_DV
        WHERE 
            h.NEWNHSNO = :newnhsno

        UNION ALL

        SELECT 
            h.NEWNHSNO AS PatientID,
            h.DISDATE_DV AS EventDate,
            'HES Discharge' AS EventTable,
            CASE WHEN ca.NEWNHSNO IS NOT NULL THEN TRUE ELSE FALSE END AS InCovidView
        FROM 
            hospitalisations h
        LEFT JOIN
            covid_admissions ca ON h.NEWNHSNO = ca.NEWNHSNO AND h.DISDATE_DV = ca.DISDATE_DV
        WHERE 
            h.NEWNHSNO = :newnhsno

        UNION ALL

        SELECT 
            NEWNHSNO,
            SPECIMEN_DATE,
            'Infection',
            NULL AS InCovidView
        FROM 
            infections
        WHERE 
            NEWNHSNO = :newnhsno

        UNION ALL

        SELECT 
            NEWNHSNO,
            RECEIVED,
            'Therapeutic',
            NULL AS InCovidView
        FROM 
            therapeutics
        WHERE 
            NEWNHSNO = :newnhsno

        UNION ALL

        SELECT 
            d.NEWNHSNO,
            d.DOD,
            'Death',
            CASE WHEN cd.NEWNHSNO IS NOT NULL THEN TRUE ELSE FALSE END AS InCovidView
        FROM 
            deaths d
        LEFT JOIN
            covid_deaths cd ON d.NEWNHSNO = cd.NEWNHSNO AND d.DOD = cd.DOD
        WHERE 
            d.NEWNHSNO = :newnhsno
    )

    SELECT
        DATE(EventDate) AS EventDate,
        EventTable,
        InCovidView
    FROM 
        PatientEvents
    ORDER BY 
        EventDate,
        CASE EventTable
            WHEN 'Death' THEN 1
            WHEN 'HES Admission' THEN 2
            WHEN 'Infection' THEN 3
            WHEN 'HES Discharge' THEN 4
            WHEN 'Therapeutic' THEN 5
            ELSE 6
        END
    ;
    """
    params = {'newnhsno': newnhsno}
    return sql_read_to_pandas(sql_query, conn_string, params)
