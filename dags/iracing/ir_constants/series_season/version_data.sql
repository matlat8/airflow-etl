INSERT INTO iracing.{target_table}
SELECT *, current_timestamp() as loaddate
from (
    select *
    from iracing_stg.{stage_table}

    except

    select *
    except ( loaddate )
    from iracing.{target_table}
     )