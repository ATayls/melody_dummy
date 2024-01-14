from db_utils import sql_read_to_pandas


def get_all_patients(conn_string):
    """
    Returns all patients from the database as a DataFrame.
    """
    sql_query = "SELECT * FROM Patients"
    return sql_read_to_pandas(sql_query, conn_string)


def get_days_between_hospitalisation_and_earliest_infection(conn_string):
    sql_query = """
    SELECT 
        t.NEWNHSNO,
        MIN(t.RECEIVED) AS FirstTherapeutic,
        MIN(i.SPECIMEN_DATE) AS EarliestInfection,
        CASE 
            WHEN MIN(i.SPECIMEN_DATE) IS NULL THEN NULL
            ELSE julianday(MIN(t.RECEIVED)) - julianday(MIN(i.SPECIMEN_DATE)) 
        END AS DaysBetween
    FROM 
        Therapeutics AS t
    LEFT JOIN 
        Infections AS i ON t.NEWNHSNO = i.NEWNHSNO
    GROUP BY 
        t.NEWNHSNO
    HAVING 
        FirstTherapeutic IS NOT NULL;
    """

    return sql_read_to_pandas(sql_query, conn_string)

def get_hospitalisations_post_infection_within_days(conn_string, days):
    """
    Retrieves hospitalisations that occur within a specified number of days after an infection.

    :param conn_string: Database connection string
    :param days: Number of days to consider after the infection date
    :return: DataFrame containing the query results
    """
    sql_query = """
    SELECT 
        i.NEWNHSNO, 
        i.SPECIMEN_DATE, 
        i.EPISODE_NUM, 
        i.INFECTION_NUM, 
        h.ADMIDATE_DV, 
        h.EPISODE_COUNT, 
        h.ADMI_LEN
    FROM 
        Infections i
    JOIN 
        Hospitalisations h ON i.NEWNHSNO = h.NEWNHSNO
    WHERE 
        h.ADMIDATE_DV BETWEEN i.SPECIMEN_DATE AND DATE(i.SPECIMEN_DATE, ?)
    """
    params = (f'+{days} days',)

    return sql_read_to_pandas(sql_query, conn_string, params=params)