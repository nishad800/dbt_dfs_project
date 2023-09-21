select

    customer_id,

    first_name,

    last_name

from {{ source('jaffle_shop', 'EXTERNAL_CUSTOMER_WITH_PARTITION') }}