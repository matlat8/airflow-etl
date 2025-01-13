CREATE OR REPLACE TABLE iracing_stg.stg_series_seasons_schedules (
key_series_season_schedule Nullable(Int64),
season_id Nullable(Int64),
race_week_num Nullable(Int64),
car_restrictions Array(Nullable(String)),
category Nullable(String),
category_id Nullable(Int64),
enable_pitlane_collisions Nullable(Bool),
full_course_cautions Nullable(Bool),
practice_length Nullable(Int64),
qual_attached Nullable(Bool),
qualify_laps Nullable(Int64),
qualify_length Nullable(Int64),
race_lap_limit Nullable(Int64),
race_time_descriptors Array(Tuple(
    repeating Nullable(Bool),
    session_minutes Nullable(Int64),
    session_times Array(Nullable(String)),
    super_session Nullable(Bool)
)),
race_time_limit Nullable(String),
race_week_cars Array(Tuple(
    car_id Nullable(Int64),
    car_name Nullable(String),
    car_name_abbreviated Nullable(String)
)),
restart_type Nullable(String),
schedule_name Nullable(String),
season_name Nullable(String),
series_id Nullable(Int64),
series_name Nullable(String),
short_parade_lap Nullable(Bool),
special_event_type Nullable(String),
start_date Nullable(Date),
start_type Nullable(String),
start_zone Nullable(Bool),
track Tuple(
    category Nullable(String),
    category_id Nullable(Int64),
    track_id Nullable(Int64),
    track_name Nullable(String)),
    track_state Tuple(
    leave_marbles Nullable(Bool)
),
warmup_length Nullable(Int64)
)
ENGINE = Memory