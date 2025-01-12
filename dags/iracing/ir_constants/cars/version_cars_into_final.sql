INSERT INTO iracing.cars
SELECT 
    *, current_timestamp() as loaddate
FROM (
    SELECT 
        *
    FROM 
        iracing_stg.stg_cars
    
    EXCEPT
    
    SELECT 
        *
    EXCEPT ( loaddate )
    FROM 
        iracing.cars
)