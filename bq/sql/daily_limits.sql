SELECT DISTINCT(user_id)                                                                                       AS user_id
             , IF(FIRST_VALUE(user_limit) OVER (PARTITION BY user_id ORDER BY time DESC) IS NULL, False, True) AS limited
             , DATE('{{ds}}')                                                                                  AS ptn_date
FROM partner_prod.limits_{{ params.carrier }}
WHERE action = 'set' and  DATE(time) <= DATE('{{ds}}')