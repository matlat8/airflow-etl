INSERT INTO iracing_stg.{tablename}
select * from s3('{key}', '{access_key}', '{secret_key}')
