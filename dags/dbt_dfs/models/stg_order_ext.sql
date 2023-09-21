select

    order_id,

    customer_id,

    order_date,

    status,
    ETL_LOADED_AT

from {{ source('jaffle_shop', 'EXTERNAL_ORDER_PARTION') }}
