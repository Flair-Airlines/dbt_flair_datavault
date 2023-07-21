{{
    config(
        materialized = 'view',
    )
}}

select a."Flight Date",a.NET_CHRG as "Daily Base Revenue",b.NET_CHRG as "Daily Ancillary Revenue",(a.NET_CHRG+b.NET_CHRG) "Total" from 
(
select 
"Flight Date","Category",sum("Net Charge") NET_CHRG
from {{ ref('VW_REVENUE') }}
where "Category"='Base'
group by 1,2
) as a join 
(
select "Flight Date","Category",sum("Net Charge") NET_CHRG
from {{ ref('VW_REVENUE') }}
where "Category"='Ancillary' 
group by 1,2
) as b on a."Flight Date"=b."Flight Date"
--where b."Flight Date" = '05/01/2023'