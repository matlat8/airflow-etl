CREATE OR REPLACE TABLE iracing_stg.stg_fct_irating_distribution
    ENGINE = Memory
    AS
WITH current_year AS (
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
)
SELECT
    c.*,
    py.newi_rating as prior_year_rating,
    py.irating_group as prior_year_irating_group,
    py.row_num as prior_year_row_num
FROM current_year c
LEFT JOIN (
    SELECT
        cust_id,
        license_category,
        season_year + 1 as next_year,
        season_quarter,
        newi_rating,
        floor(newi_rating / 50) * 50 as irating_group,
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
) py ON c.cust_id = py.cust_id
    AND c.license_category = py.license_category
    AND c.season_year = py.next_year
    AND c.season_quarter = py.season_quarter;
EXCHANGE TABLES iracing_stg.stg_fct_irating_distribution AND iracing_api.fct_irating_distribution;
DROP TABLE IF EXISTS iracing_stg.stg_fct_irating_distribution;