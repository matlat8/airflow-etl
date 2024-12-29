truncate table iracing.mv_session_results;

INSERT INTO iracing.mv_session_results
WITH latest_records AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY subsession_id, simsession_number, simsession_type, cust_id, loaddate
               ORDER BY loaddate
           ) AS load_rank,
           row_number() OVER (
               PARTITION BY subsession_id, simsession_number, simsession_type, cust_id
               ORDER BY loaddate DESC
           ) AS overall_rank
    FROM iracing.session_results
)
SELECT
    subsession_id,
    simsession_number,
    simsession_name,
    simsession_type,
    simsession_type_name,
    cust_id,
    display_name,
    club_id,
    club_name,
    club_shortname,
    country_code,
    friend,
    ai,
    car_id,
    car_name,
    car_class_id,
    car_class_name,
    car_class_short_name,
    position,
    finish_position,
    finish_position_in_class,
    starting_position,
    starting_position_in_class,
    laps_complete,
    laps_lead,
    opt_laps_complete,
    incidents,
    `interval`,
    class_interval,
    reason_out,
    reason_out_id,
    weight_penalty_kg,
    average_lap,
    best_lap_num,
    best_lap_time,
    best_nlaps_num,
    best_nlaps_time,
    qual_lap_time,
    best_qual_lap_num,
    best_qual_lap_time,
    best_qual_lap_at,
    champ_points,
    aggregate_champ_points,
    club_points,
    league_points,
    league_agg_points,
    drop_race,
    multiplier,
    oldi_rating,
    newi_rating,
    old_ttrating,
    new_ttrating,
    old_license_level,
    new_license_level,
    old_sub_level,
    new_sub_level,
    old_cpi,
    new_cpi,
    license_change_oval,
    license_change_road,
    watched,
    max_pct_fuel_fill,
    loaddate
FROM latest_records
WHERE load_rank = 1
  AND overall_rank = 1;