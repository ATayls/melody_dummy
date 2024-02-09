CREATE VIEW covid_deaths AS
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
    ) AS INFECTION_EPISODE_START_TO_DOD_DAYS,
    (
        julianday(dcpi.DOD) - julianday(dcpi.CLOSEST_PRIOR_EPISODE_START_DATE)
    ) <= 28 AS INFECTION_WITHIN_28_DAYS_PRIOR
FROM
    DeathsClosestPriorInfection AS dcpi
WHERE
    dcpi.CODE_UNDERLYING = 1
    OR INFECTION_EPISODE_START_TO_DOD_DAYS <= 28
;