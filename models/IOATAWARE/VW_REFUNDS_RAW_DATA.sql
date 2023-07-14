{{
    config(
        materialized = 'view'
    )
}}

select 
TO_VARCHAR(TO_DATE(t1.DTM_REFUND_DATE),'MM/DD/YYYY') AS "Refund Date",
TO_VARCHAR(TO_TIME(t1.DTM_REFUND_DATE),'HH12:MI AM') as "Refund Time",
t1.LNG_RESERVATION_NMBR as "Reservation Nmbr",
t4.STR_REF1 as "PNR",
ROUND(t1.MNY_REFUND_AMOUNT*-1,2)  as "Refund Amount (CAN)",
t5.STR_CURRENCY_IDENT as "Base Currency",
round(t1.MNY_GL_CURRENCY_PAYMENTS_AMOUNT*-1,2) as "Base Currency Amount",
t1.MNY_EXCHANGE_RATE as "Exchange Rate",
t3.STR_GL_PAYMENT_METHOD_DESC as "str GL Payment Method Desc",
t2.STR_GL_PAYMENTS_DESC as "str GL Payments Desc",
t2.STR_GL_PAYMENTS_PAYER as "str GL Payments Payer",
t2.STR_GL_PAYMENTS_NOTES as "str GL Payments Notes",
t2.LNG_GL_PAYMENTS_RECEIPT_NMBR as "lng GL Payments Receipt Nmbr"
from FIVETRAN_DATABASE.PSS_AMELIARES_DBO.TBL_GL_PAYMENTS_REFUND t1
left join {{ source('PSS_AMELIARES_DBO', 'TBL_GL_PAYMENTS') }} t2 on t1.LNG_GL_PAYMENTS_ID_NMBR =t2.LNG_GL_PAYMENTS_ID_NMBR
left join {{ source('PSS_AMELIARES_DBO', 'TBL_GL_PAYMENT_METHOD') }} t3 on t2.LNG_GL_PAYMENT_METHOD_ID_NMBR = t3.LNG_GL_PAYMENT_METHOD_ID_NMBR
left join {{ source('PSS_AMELIARES_DBO', 'TBL_RES_HEADER') }} t4 on t4.LNG_RESERVATION_NMBR = t1.LNG_RESERVATION_NMBR
left join {{ source('PSS_AMELIARES_DBO', 'TBL_CURRENCY') }} t5 on t5.LNG_CURRENCY_ID_NMBR = t1.LNG_CURRENCY_ID_NMBR
