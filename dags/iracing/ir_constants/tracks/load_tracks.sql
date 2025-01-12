insert into iracing_stg.stg_tracks
select * from s3('{filepath}', '{access_key}', '{secret_key}')