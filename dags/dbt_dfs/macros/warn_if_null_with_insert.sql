{% test warn_if_null_with_insert(model,column_name) %}

 

    {{ config(severity = 'warn') }}

 

{% set null_check_query %}

    select

        '{{ target.database }}' ,

        '{{ target.schema }}',

        '{{ model }}',

        '90',

        'IS_NULL_TEST',

        'Null in key column -> {{ column_name }} in {{ model }}',

        current_timestamp(),

        'BSNS_ENTTY_CODE_ABC',

         METADATA$FILENAME ,

        value,

        current_timestamp()

        from {{model}}

        where ({{column_name}}  is NULL)
{% endset %}

{{ null_check_query }}

{% if execute %}

    --{{ insert_reject_records(null_check_query) }}
    {% set result_val = insert_reject_records(null_check_query) %}      

{% endif %}

{% endtest %}