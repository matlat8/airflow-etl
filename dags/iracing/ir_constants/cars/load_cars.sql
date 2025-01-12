INSERT INTO iracing_stg.stg_cars
SELECT
      car_id
    , car_name
    , car_name_abbreviated
    , ai_enabled
    , allow_number_colors
    , allow_number_font
    , allow_sponsor1
    , allow_sponsor2
    , allow_wheel_color
    , award_exempt
    , car_dirpath
    , car_types
    , car_weight
    , categories
    , created
    , first_sale
    , forum_url
    , free_with_subscription
    , has_headlights
    , has_multiple_dry_tire_types
    , has_rain_capable_tire_types
    , hp
    , is_ps_purchasable
    , max_power_adjust_pct
    , max_weight_penalty_kg
    , min_power_adjust_pct
    , package_id
    , patterns
    , price
    , price_display
    , rain_enabled
    , retired
    , search_filters
    , sku
    , car_rules
    , detail_copy
    , detail_screen_shot_images
    , detail_techspecs_copy
    , folder
    , gallery_images
    , gallery_prefix
    , group_image
    , group_name
    , large_image
    , logo
    , small_image
    , sponsor_logo
    , template_path
    , car_make
    , car_model
    , site_url
FROM
    s3('{filepath}', '{access_key}', '{secret_key}')
;