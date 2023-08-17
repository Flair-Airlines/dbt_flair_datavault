Select a."Charge Date","TAXES GST","TAXES HST","TAXES QST","TAXES MEX VAT","TAXES DR",("TAXES GST"+"TAXES HST"+"TAXES QST"+"TAXES MEX VAT"+"TAXES DR") as "Total"
from (
select
"Charge Date",sum("Taxes GST") "TAXES GST"
from {{ ref('VW_TAX_BREAKDOWN_DETAILED') }}
group by 1
) a join (
select
"Charge Date",sum("Taxes HST") "TAXES HST"
from {{ ref('VW_TAX_BREAKDOWN_DETAILED') }}
group by 1
) b on a."Charge Date"=b."Charge Date"
join (
select
"Charge Date",sum("Taxes QST") "TAXES QST"
from {{ ref('VW_TAX_BREAKDOWN_DETAILED') }}
group by 1
) c on a."Charge Date"=c."Charge Date"
join (
select
"Charge Date",sum("Taxes Mex VAT") "TAXES MEX VAT"
from {{ ref('VW_TAX_BREAKDOWN_DETAILED') }}
group by 1
) d on a."Charge Date"=d."Charge Date"
join (
select
"Charge Date",sum("Taxes DR") "TAXES DR"
from {{ ref('VW_TAX_BREAKDOWN_DETAILED') }}
group by 1
) e on a."Charge Date"=e."Charge Date"
