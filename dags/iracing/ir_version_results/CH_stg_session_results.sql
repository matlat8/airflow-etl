CREATE OR REPLACE TABLE iracing.stg_session_results
(
    -- Session Identification
    subsession_id Int64,
    simsession_number Nullable(Int64),
    simsession_name Nullable(String),
    simsession_type Nullable(Int64),
    simsession_type_name Nullable(String),

    -- Driver Information
    cust_id Int64,
    display_name Nullable(String),
    club_id Nullable(Int64),
    club_name Nullable(String),
    club_shortname Nullable(String),
    country_code Nullable(String),
    friend Nullable(Bool),
    ai Nullable(Bool),

    -- Car and Class Details
    car_id Nullable(Int64),
    car_name Nullable(String),
    car_class_id Nullable(Int64),
    car_class_name Nullable(String),
    car_class_short_name Nullable(String),

    -- Race Performance
    position Nullable(Int64),
    finish_position Nullable(Int64),
    finish_position_in_class Nullable(Int64),
    starting_position Nullable(Int64),
    starting_position_in_class Nullable(Int64),
    laps_complete Nullable(Int64),
    laps_lead Nullable(Int64),
    opt_laps_complete Nullable(Int64),
    incidents Nullable(Int64),
    interval Nullable(Int64),
    class_interval Nullable(Int64),
    reason_out Nullable(String),
    reason_out_id Nullable(Int64),
    weight_penalty_kg Nullable(Int64),

    -- Lap Times
    average_lap Nullable(Int64),
    best_lap_num Nullable(Int64),
    best_lap_time Nullable(Int64),
    best_nlaps_num Nullable(Int64),
    best_nlaps_time Nullable(Int64),
    qual_lap_time Nullable(Int64),
    best_qual_lap_num Nullable(Int64),
    best_qual_lap_time Nullable(Int64),
    best_qual_lap_at Nullable(DateTime64(6)),

    -- Points and Ratings
    champ_points Nullable(Int64),
    aggregate_champ_points Nullable(Int64),
    club_points Nullable(Int64),
    league_points Nullable(Int64),
    league_agg_points Nullable(Int64),
    drop_race Nullable(Bool),
    multiplier Nullable(Int64),

    -- License and Rating Changes
    oldi_rating Nullable(Int64),
    newi_rating Nullable(Int64),
    old_ttrating Nullable(Int64),
    new_ttrating Nullable(Int64),
    old_license_level Nullable(Int64),
    new_license_level Nullable(Int64),
    old_sub_level Nullable(Int64),
    new_sub_level Nullable(Int64),
    old_cpi Nullable(Float64),
    new_cpi Nullable(Float64),
    license_change_oval Nullable(Int64),
    license_change_road Nullable(Int64),

    -- Additional Flags
    watched Nullable(Bool),
    max_pct_fuel_fill Nullable(Int64)
)
ENGINE = MergeTree()
ORDER BY (subsession_id, cust_id)
;