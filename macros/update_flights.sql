{% macro update_flights() %}

update {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }} SET ITEM_NAME = {{ parse_quotes('ITEM_NAME') }},
PRODUCT_GROUP = {{ parse_quotes('PRODUCT_GROUP') }},
CLEAN_TIMESTAMP = CURRENT_TIMESTAMP()
WHERE ITEM_NAME like '%\"%'  or PRODUCT_GROUP like '%\"%'

{% endmacro %}