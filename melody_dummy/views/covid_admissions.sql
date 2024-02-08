CREATE VIEW covid_admissions AS
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
    ) AS INFECTION_EPISODE_START_TO_ADMI_DAYS,
    (
        julianday(hcpi.ADMIDATE_DV) - julianday(hcpi.CLOSEST_PRIOR_EPISODE_START_DATE)
    ) <= 14 AS INFECTION_WITHIN_14_DAYS_PRIOR
FROM
    HospitalisationClosestPriorInfection AS hcpi
WHERE
    hcpi.DIAG_CODE_MATCH = 1
    OR INFECTION_EPISODE_START_TO_ADMI_DAYS <= 14
;