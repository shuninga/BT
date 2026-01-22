--higherlimit

DROP TABLE IF EXISTS shuninga.causal_analysis_bp_raw_data_step_4;

CREATE TABLE IF NOT EXISTS shuninga.causal_analysis_bp_raw_data_step_4 AS (

 --higherlimit
WITH fact_ride_pmm_groupings_agg AS (
 SELECT ride_id,
        SUM(bookings) AS bookings,
        SUM(net_revenue) AS net_revenue,
        SUM(post_marketing_margin) AS post_marketing_margin
   FROM iceberg.rifi.ride_financial_metrics
  WHERE CAST(ds AS DATE) >= CAST('2025-01-01' AS DATE)
  GROUP BY 1
)

,bp_users_v1 AS (
SELECT bp_user_id,
       user_id,
       org_id,
       account_id,
       bp_type_clean AS bp_type,
       bp_sub_type,
       program_type,
       total_rides
  FROM salesops.bp_users
)

/*,bp_users_v2 AS (
SELECT bp_user_id,
       user_id,
       org_id,
       account_id,
       bp_type_clean,
       bp_sub_type,
       program_type,
       total_rides,
       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY total_rides DESC) rnk 
  FROM bp_users_v1
)

,bp_users_v3 AS (
SELECT bp_user_id,
       user_id,
       org_id,
       account_id,
       bp_type_clean,
       bp_sub_type,
       program_type,
       total_rides
  FROM bp_users_v2 
 WHERE rnk = 1 -- most used bp 
)
*/

,dim_bp_rides AS (
SELECT ride_id,
       MIN(business_program_type) AS business_program_type,
       MIN(business_program_user_id) AS business_program_user_id
  FROM enterprise.dim_bp_rides  
 WHERE CAST(ds AS DATE) >= CAST('2025-01-01' AS DATE)
 GROUP BY 1
)

,bp_riders AS (
SELECT DISTINCT 
       sub1_v1.rider_lyft_id
  FROM coco.fact_rides sub1_v1
  JOIN enterprise.fact_enterprise_rides sub1_v2
    ON sub1_v1.ride_id = sub1_v2.ride_id
  LEFT JOIN dim_bp_rides sub1_v3 
    ON sub1_v1.ride_id = sub1_v3.ride_id
  LEFT JOIN fact_ride_pmm_groupings_agg sub2
    ON sub1_v1.ride_id = sub2.ride_id
  LEFT JOIN bp_users_v1 sub6
    ON sub1_v3.business_program_user_id = sub6.bp_user_id 
  LEFT JOIN hive.salesops.segments_2023 sub7
    ON sub1_v2.org_id = sub7.org_id
 WHERE CAST(sub1_v1.ds AS DATE) >= CAST('2025-01-01' AS DATE)
   AND CAST(sub1_v2.ds AS DATE) >= CAST('2025-01-01' AS DATE)
   AND sub1_v1.ride_status IN ('finished')
   AND NOT sub1_v1.is_concierge_ride
   AND sub1_v1.rider_payment_usd IS NOT NULL 
   AND sub1_v1.is_business_ride = TRUE 
)

--- user who have bp rides since 2025-01-01
,causal_analysis_bp_raw_data_step_1 AS (
SELECT sub1_v1.rider_lyft_id,
       sub1_v1.ride_id,
       sub1_v1.requested_at,
       sub1_v1.ride_request_region,
       sub1_v1.is_business_ride,
       sub1_v3.business_program_user_id,
       sub1_v3.business_program_type
  FROM coco.fact_rides sub1_v1
  JOIN enterprise.fact_enterprise_rides sub1_v2
    ON sub1_v1.ride_id = sub1_v2.ride_id
  LEFT JOIN dim_bp_rides sub1_v3 
    ON sub1_v1.ride_id = sub1_v3.ride_id
  LEFT JOIN fact_ride_pmm_groupings_agg sub2
    ON sub1_v1.ride_id = sub2.ride_id
  LEFT JOIN bp_users_v1 sub6
    ON sub1_v3.business_program_user_id = sub6.bp_user_id 
  LEFT JOIN hive.salesops.segments_2023 sub7
    ON sub1_v2.org_id = sub7.org_id
 WHERE CAST(sub1_v1.ds AS DATE) >= CAST('2025-01-01' AS DATE)
   AND CAST(sub1_v2.ds AS DATE) >= CAST('2025-01-01' AS DATE)
   AND sub1_v1.ride_status IN ('finished')
   AND NOT sub1_v1.is_concierge_ride
   AND sub1_v1.rider_payment_usd IS NOT NULL 
   AND sub1_v1.is_business_ride = TRUE 

UNION ALL 

SELECT sub1_v1.rider_lyft_id,
       sub1_v1.ride_id,
       sub1_v1.requested_at,
       sub1_v1.ride_request_region,
       sub1_v1.is_business_ride,
       sub1_v3.business_program_user_id,
       sub1_v3.business_program_type
  FROM coco.fact_rides sub1_v1
  LEFT JOIN dim_bp_rides sub1_v3 
    ON sub1_v1.ride_id = sub1_v3.ride_id
  LEFT JOIN fact_ride_pmm_groupings_agg sub2
    ON sub1_v1.ride_id = sub2.ride_id
  LEFT JOIN bp_users_v1 sub6
    ON sub1_v3.business_program_user_id = sub6.bp_user_id 
  JOIN bp_riders sub4 
    ON sub1_v1.rider_lyft_id = sub4.rider_lyft_id 
 WHERE CAST(sub1_v1.ds AS DATE) >= CAST('2025-01-01' AS DATE)
   AND sub1_v1.ride_status IN ('finished')
   AND NOT sub1_v1.is_concierge_ride
   AND sub1_v1.rider_payment_usd IS NOT NULL 
   AND sub1_v1.is_business_ride <> TRUE 
)

----------------------------------- step 2

, causal_analysis_raw_bp_data AS (
SELECT sub1.rider_lyft_id,
       sub1.ride_id,
       sub1.requested_at,
       sub1.ride_request_region,
       sub1.is_business_ride,
       sub1.business_program_user_id,
       sub1.business_program_type
  FROM causal_analysis_bp_raw_data_step_1 sub1
 WHERE is_business_ride = TRUE
)

,causal_analysis_raw_bp_data_agg_v1 AS (
SELECT rider_lyft_id,
       business_program_user_id,
       business_program_type,
       COUNT(DISTINCT ride_id) count_rides 
  FROM causal_analysis_raw_bp_data 
 GROUP BY 1,2,3
)

,causal_analysis_raw_bp_data_agg_v2 AS (
SELECT rider_lyft_id,
       business_program_user_id,
       business_program_type,
       count_rides,
       ROW_NUMBER() OVER(PARTITION BY rider_lyft_id ORDER BY count_rides DESC) rnk
  FROM causal_analysis_raw_bp_data_agg_v1
) 

,causal_analysis_raw_bp_data_agg_v3 AS (
SELECT rider_lyft_id,
       business_program_user_id,
       business_program_type,
       count_rides
  FROM causal_analysis_raw_bp_data_agg_v2
 WHERE rnk = 1
) 

,causal_analysis_raw_data_v1 AS (
SELECT sub1.rider_lyft_id,
       sub1.ride_id,
       sub1.requested_at,
       sub1.ride_request_region,
       sub1.is_business_ride,
       sub1.business_program_user_id,
       sub1.business_program_type,
       sub2.business_program_user_id AS most_used_business_program_user_id,
       sub2.business_program_type AS most_used_business_program_type
  FROM causal_analysis_bp_raw_data_step_1 sub1
  LEFT JOIN causal_analysis_raw_bp_data_agg_v3 sub2 
    ON sub1.rider_lyft_id = sub2.rider_lyft_id
)

,causal_analysis_bp_raw_data_step_2 AS (
SELECT sub1.rider_lyft_id,
       sub1.ride_id,
       sub1.requested_at,
       sub1.ride_request_region,
       sub1.is_business_ride,
       sub1.business_program_user_id,
       sub1.business_program_type,
       sub1.most_used_business_program_user_id,
       sub1.most_used_business_program_type
  FROM causal_analysis_raw_data_v1 sub1
)


----------------------- step 3 

,dim_users AS (
SELECT user_lyft_id, 
       signup_at
  FROM coco.dim_user 
)

,event_businessprograms_program_user_created_or_updated AS (
SELECT program_user_id,
       MIN(occurred_at) AS min_occurred_at,
       MAX(occurred_at) AS max_occurred_at
  FROM events.event_businessprograms_program_user_created_or_updated
 WHERE ds >= '2017-01-01'
 GROUP BY 1
)

,causal_analysis_bp_raw_data_step_3 AS (
SELECT sub1.rider_lyft_id,
       sub1.ride_id,
       sub1.requested_at,
       sub1.ride_request_region,
       sub1.is_business_ride,
       sub1.business_program_user_id,
       sub1.business_program_type,
       CASE WHEN sub1.business_program_type IS NULL AND sub3_v1.first_activated_at < DATE('2021-08-12') THEN 'pre-btp' 
            WHEN sub1.business_program_type IS NULL AND sub3_v1.first_activated_at >= DATE('2021-08-12') THEN 'organic' 
            WHEN sub1.business_program_type IN ('autopay','manual expense') and sub4_v1.bp_rewards_eligible = 'Eligible' THEN 'managed'  
            WHEN sub1.business_program_type IN ('autopay','manual expense') and (sub4_v1.bp_rewards_eligible <>  'Eligible' OR sub4_v1.bp_rewards_eligible is null) THEN 'organic' 
            WHEN sub1.business_program_type IN ('organic') THEN 'organic'  
            ELSE business_program_type
            END AS business_program_type_cleaned,
       sub1.most_used_business_program_user_id,
       sub1.most_used_business_program_type,
       CASE WHEN sub1.most_used_business_program_type IS NULL AND sub3_v2.first_activated_at < DATE('2021-08-12') THEN 'pre-btp' 
            WHEN sub1.most_used_business_program_type IS NULL AND sub3_v2.first_activated_at >= DATE('2021-08-12') THEN 'organic' 
            WHEN sub1.most_used_business_program_type IN ('autopay','manual expense') and sub4_v2.bp_rewards_eligible = 'Eligible' THEN 'managed'  
            WHEN sub1.most_used_business_program_type IN ('autopay','manual expense') and (sub4_v2.bp_rewards_eligible <>  'Eligible' OR sub4_v2.bp_rewards_eligible is null) THEN 'organic' 
            WHEN sub1.most_used_business_program_type IN ('organic') THEN 'organic'  
            ELSE most_used_business_program_type
            END AS most_used_business_program_type_cleaned,
       sub2.signup_at AS lyft_signed_up_at, 
       sub5_v1.min_occurred_at AS bp_created_at, 
       sub3_v1.first_activated_at AS bp_activated_at,
       sub5_v2.min_occurred_at AS most_used_bp_created_at, 
       sub3_v2.first_activated_at AS most_used_bp_activated_at,
       CASE WHEN sub2.signup_at >= CAST('2025-08-04' AS DATE) THEN TRUE ELSE FALSE END has_lyft_signed_up_after_bt_rewards_2_0_launch,
       CASE WHEN sub1.requested_at >= CAST('2025-08-04' AS DATE) THEN TRUE ELSE FALSE END has_requested_after_bt_rewards_2_0_launch       
  FROM causal_analysis_bp_raw_data_step_2 sub1
  JOIN dim_users sub2 
    ON sub1.rider_lyft_id = sub2.user_lyft_id
  LEFT JOIN salesops.bp_users sub3_v1
    ON sub1.business_program_user_id = sub3_v1.bp_user_id
  LEFT JOIN salesops.bp_orgs sub4_v1
    ON sub3_v1.org_id = sub4_v1.org_id 
  LEFT JOIN event_businessprograms_program_user_created_or_updated sub5_v1
    ON sub1.most_used_business_program_user_id = sub5_v1.program_user_id
  LEFT JOIN salesops.bp_users sub3_v2
    ON sub1.most_used_business_program_user_id = sub3_v2.bp_user_id
  LEFT JOIN salesops.bp_orgs sub4_v2
    ON sub3_v2.org_id = sub4_v2.org_id 
  LEFT JOIN event_businessprograms_program_user_created_or_updated sub5_v2 
    ON sub1.most_used_business_program_user_id = sub5_v2.program_user_id
)
------------------------------ step 4 

--higherlimit


, bp_users AS (
SELECT user_id,
       MIN(first_activated_at) AS first_activated_at
  FROM salesops.bp_users
 GROUP BY 1
)

,causal_analysis_bp_raw_data AS (
SELECT sub1.rider_lyft_id,
       sub1.ride_id,
       CAST(sub1.requested_at AS DATE) ds,
       sub1.requested_at,
       sub1.ride_request_region,
       sub1.is_business_ride,
       sub1.business_program_user_id,
       sub1.business_program_type,
       sub1.business_program_type_cleaned,
       sub1.most_used_business_program_user_id,
       sub1.most_used_business_program_type,
       sub1.most_used_business_program_type_cleaned,
       sub1.lyft_signed_up_at, 
       sub1.bp_created_at, 
       sub1.bp_activated_at,
       sub1.most_used_bp_created_at, 
       sub1.most_used_bp_activated_at,
       sub1.has_lyft_signed_up_after_bt_rewards_2_0_launch,
       sub1.has_requested_after_bt_rewards_2_0_launch,
       CASE WHEN sub2.first_activated_at >= CAST('2025-08-04' AS DATE) AND sub1.has_lyft_signed_up_after_bt_rewards_2_0_launch = FALSE THEN 'new-user-PP' 
            WHEN sub2.first_activated_at >= CAST('2025-08-04' AS DATE) AND sub1.has_lyft_signed_up_after_bt_rewards_2_0_launch = TRUE THEN 'new-user-NP'
            ELSE 'pre-existing-user' END AS user_type,
       CASE WHEN sub2.first_activated_at >= CAST('2025-08-04' AS DATE) AND sub1.most_used_business_program_type_cleaned = 'managed' AND sub1.has_lyft_signed_up_after_bt_rewards_2_0_launch = FALSE THEN 'new-user-PP-managed' 
            WHEN sub2.first_activated_at >= CAST('2025-08-04' AS DATE) AND sub1.most_used_business_program_type_cleaned = 'managed' AND sub1.has_lyft_signed_up_after_bt_rewards_2_0_launch = TRUE THEN 'new-user-NP-managed'
            WHEN sub2.first_activated_at >= CAST('2025-08-04' AS DATE) AND sub1.most_used_business_program_type_cleaned = 'organic' AND sub1.has_lyft_signed_up_after_bt_rewards_2_0_launch = FALSE THEN 'new-user-PP-organic' 
            WHEN sub2.first_activated_at >= CAST('2025-08-04' AS DATE) AND sub1.most_used_business_program_type_cleaned = 'organic' AND sub1.has_lyft_signed_up_after_bt_rewards_2_0_launch = TRUE THEN 'new-user-NP-organic'
            WHEN sub1.most_used_business_program_type_cleaned = 'organic' THEN 'pre-existing-user-organic' 
            WHEN sub1.most_used_business_program_type_cleaned = 'managed' THEN 'pre-existing-user-managed' 
            END AS sub_user_type,
      sub2.first_activated_at
  FROM causal_analysis_bp_raw_data_step_3 sub1
  LEFT JOIN bp_users sub2 
    ON sub1.rider_lyft_id = sub2.user_id
 WHERE TRUE 
)

SELECT * FROM causal_analysis_bp_raw_data 
);

--higherlimit
select min(ds), max(ds) from shuninga.causal_analysis_bp_raw_data_step_4

