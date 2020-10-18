with active_users as (
    SELECT SUM(tx + rx) / (1048576 * 1024)                         AS user_traffic_gb
         , user_id                                                 AS user_id
         , IF(ANY_VALUE(u.auth_method) = 'anonymous', False, True) AS anonymous
    FROM partner_prod.vpn_session_{{params.carrier}} AS s
             LEFT JOIN partner_prod.user_{{params.carrier}} AS u ON s.user_id = u.id
    WHERE DATE(end_time) = DATE('{{ds}}')
    GROUP BY user_id
),
     new_users as (
         SELECT id                                                    AS user_id
              , IF(ANY_VALUE(auth_method) = 'anonymous', False, True) AS anonymous
         FROM partner_prod.user_{{params.carrier}}
             WHERE DATE(transaction_date) = DATE('{{ds}}')
           AND transaction_type = 'create'
         GROUP BY id
     ),
     dau as (
         SELECT 1                     as users
              , if(limited, 1, 0)     as limited_users
              , if(limited, 0, 1)     as unlimited_users
              , if(anonymous, 1, 0)   as anonym_users
              , if(anonymous, 0, 1)   as registered_users
              , user_traffic_gb       as user_traffic_gb
         FROM active_users as dau
                  LEFT JOIN airflow.daily_limits_{{params.carrier}} as dl on dl.user_id = dau.user_id and dl.ptn_date = DATE('{{ds}}')
     ),
     dau_agg as (
         SELECT sum(users)                                                 as active_users
              , sum(limited_users)                                         as active_limited_users
              , sum(unlimited_users)                                       as active_unlimited_users
              , sum(anonym_users)                                          as active_anonym_users
              , sum(registered_users)                                      as active_registered_users
              , ROUND((sum(user_traffic_gb)), 3)                     as traffic_gb
              , ROUND((sum(user_traffic_gb * limited_users)), 3)     as limited_traffic_gb
              , ROUND((sum(user_traffic_gb * unlimited_users)), 3)   as unlimited_traffic_gb
              , ROUND((sum(user_traffic_gb * anonym_users)), 3)      as anonym_traffic_gb
              , ROUND((sum(user_traffic_gb * registered_users)), 3)  as registered_traffic_gb
         FROM dau
     ),
     dnu as (
         SELECT 1                   as users
              , if(limited, 1, 0)   as limited_users
              , if(limited, 0, 1)   as unlimited_users
              , if(anonymous, 1, 0) as anonym_users
              , if(anonymous, 0, 1) as registered_users
         FROM new_users as dau
                  LEFT JOIN airflow.daily_limits_{{params.carrier}} as dl on dl.user_id = dau.user_id and dl.ptn_date = DATE('{{ds}}')
     ),
     dnu_agg as (
         SELECT sum(users)            as new_users
              , sum(limited_users)    as new_limited_users
              , sum(unlimited_users)  as new_unlimited_users
              , sum(anonym_users)     as new_anonym_users
              , sum(registered_users) as new_registered_users
         FROM dnu
     ),
     limits_current_day as (
        SELECT user_id
             , limited
        FROM airflow.daily_limits_{{params.carrier}}
        WHERE ptn_date = DATE('{{ds}}')
     )
,
     limits_previous_day as (
        SELECT user_id
             , limited
        FROM airflow.daily_limits_{{params.carrier}}
        WHERE ptn_date = DATE_SUB(DATE('{{ds}}'), INTERVAL 1 DAY)
     )
,
     limits_delta as (
        SELECT if(lpd.limited = lcd.limited, 0, 1) as not_equal
             , if(lpd.limited, 1, 0)               as limited
             , if(lpd.limited, 0, 1)               as unlimited

        FROM limits_previous_day as lpd
        INNER JOIN limits_current_day as lcd on lpd.user_id = lcd.user_id
     )
,
    limits_delta_agg as (
        SELECT sum(not_equal * limited)   as limited_to_unlimited_users
             , sum(not_equal * unlimited) as unlimited_to_limited_users
        FROM limits_delta
    )

select DATE('{{ds}}') as ptn_date
     , active_users
     , active_limited_users
     , active_unlimited_users
     , active_anonym_users
     , active_registered_users
     , traffic_gb
     , limited_traffic_gb
     , unlimited_traffic_gb
     , anonym_traffic_gb
     , registered_traffic_gb
     , new_users
     , new_limited_users
     , new_unlimited_users
     , new_anonym_users
     , new_registered_users
     , limited_to_unlimited_users
     , unlimited_to_limited_users
from dau_agg,
     dnu_agg,
     limits_delta_agg
 