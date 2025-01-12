insert into iracing.tracks
select *, current_timestamp() as loaddate
from (
    select *
    from iracing_stg.stg_tracks
    
    except 
    
    select *
    except ( loaddate )
    from iracing.tracks
     )