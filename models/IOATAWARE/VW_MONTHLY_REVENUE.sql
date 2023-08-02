{{
    config(
        materialized = 'view',
    )
}}

select a."Flight MST Date",a.NET_CHRG as "Daily Base Revenue",b.NET_CHRG as "Daily Ancillary Revenue",(a.NET_CHRG+b.NET_CHRG) "Total" from 

( select TO_VARCHAR(TO_DATE("Flight MST Date"),'MM/DD/YYYY') AS "Flight MST Date" ,"Category",sum("Net Charge") NET_CHRG
from {{ ref('VW_REVENUE_RAW_DATA') }}
where "Category"='Base'
group by 1,2
) as a join 

(select TO_VARCHAR(TO_DATE("Flight MST Date"),'MM/DD/YYYY') AS "Flight MST Date" ,"Category",sum("Net Charge") NET_CHRG
from {{ ref('VW_REVENUE_RAW_DATA') }}
where "Category"='Ancillary' 
group by 1,2
) as b on a."Flight MST Date"=b."Flight MST Date"
--where b."Flight Date" = '05/01/2023'