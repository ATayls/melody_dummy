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