CREATE OR REPLACE TABLE iracing_stg.stg_results
    ENGINE = MergeTree()
    ORDER BY (subsession_id)
    AS
SELECT *
FROM iracing.v_results;
EXCHANGE TABLES iracing_stg.stg_results AND iracing_api.results;
DROP TABLE IF EXISTS iracing_stg.stg_results;