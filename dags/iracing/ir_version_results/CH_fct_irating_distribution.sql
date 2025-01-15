CREATE OR REPLACE TABLE iracing_stg.stg_fct_irating_distribution
    ENGINE = Memory
    AS
SELECT
    cust_id,
    license_category,
    season_year,
    season_quarter,
    newi_rating,
    floor(newi_rating / 50) * 50 as irating_group,
    count(*) OVER () as total_count,
    row_number() OVER (
        PARTITION BY license_category, season_year, season_quarter
        ORDER BY newi_rating DESC, cust_id
    ) as row_num
FROM iracing.v_results r
INNER JOIN (
    SELECT cust_id,
           license_category,
           season_year,
           season_quarter,
           max(start_time) as mst
    FROM iracing.v_results
    GROUP BY all
) x ON r.cust_id = x.cust_id AND r.start_time = x.mst
WHERE
    newi_rating <> -1
AND simsession_type = 6
AND simsession_number = 0
;
EXCHANGE TABLES iracing_stg.stg_fct_irating_distribution TO iracing_API.fct_irating_distribution;
DROP TABLE IF EXISTS iracing_stg.stg_fct_irating_distribution;