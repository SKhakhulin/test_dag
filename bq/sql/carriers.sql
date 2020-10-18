with pr as (
    SELECT replace(table_id, 'device_', '') as table_id
    FROM partner_prod.__TABLES__
    WHERE starts_with(table_id, 'device_')

    union all

    SELECT replace(table_id, 'limits_', '') as table_id
    FROM partner_prod.__TABLES__
    WHERE starts_with(table_id, 'limits_')
    union all

    SELECT replace(table_id, 'user_', '') as table_id
    FROM partner_prod.__TABLES__
    WHERE starts_with(table_id, 'user_')
    union all

    SELECT replace(table_id, 'vpn_session_', '') as table_id
    FROM partner_prod.__TABLES__
    WHERE starts_with(table_id, 'vpn_session_')
)

select table_id
from pr
group by table_id