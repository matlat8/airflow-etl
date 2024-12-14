insert into iracing.session_results
select
    *,
    current_timestamp() as loaddate
from (
    select *
    from iracing.stg_session_results

    EXCEPT

    select *
    except ( loaddate )
    from iracing.session_results
     )
