INSERT INTO iracing.stg_session_results
SELECT *
FROM s3(
        '{endpoint}/{bucket}/STG/iRacing/results/session_results/*.parquet',
        '{access_key}',
        '{secret_key}',
        'PARQUET'
    )