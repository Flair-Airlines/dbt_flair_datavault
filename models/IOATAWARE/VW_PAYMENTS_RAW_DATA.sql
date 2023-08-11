{{
    config(
        materialized = 'view',
    )
}}
select
--change the timezone conversion here as handles daylight saving 
TO_VARCHAR(convert_timezone('UTC', 'America/Denver', t1.DTM_GL_PAYMENTS_DATE),'MM/DD/YYYY') as "Payment Date",
--to_varchar(timeadd(hour,-6,t1.DTM_GL_PAYMENTS_DATE),'MM/DD/YYYY HH12:MI:SS AM') as "Payment Date",
--t1.DTM_GL_PAYMENTS_DATE as "Payment Date",
t1.LNG_RESERVATION_NMBR as "Reservation Nmbr",
t4.STR_REF1 as PNR,
t1.MNY_GL_PAYMENTS_AMOUNT AS "Payment Amount(CAN)",
t2.STR_GL_PAYMENT_METHOD_DESC as "Payment Method",
t1.STR_GL_PAYMENTS_DESC as "Payment Description",
t1.STR_GL_PAYMENTS_PAYER as "Payer",
t1.STR_GL_PAYMENTS_NOTES as "Payment Notes",
t1.LNG_GL_PAYMENTS_RECEIPT_NMBR as "Receipt Number",
t3.STR_CURRENCY_IDENT as "Base Currency",
t1.MNY_GL_CURRENCY_PAYMENTS_AMOUNT as "Base Currency Amount",
t1.MNY_EXCHANGE_RATE as "Exchange Rate"
FROM {{ source('PSS_AMELIARES_DBO' ,'TBL_GL_PAYMENTS') }} t1
--from FIVETRAN_DATABASE.PSS_AMELIARES_DBO.TBL_GL_PAYMENTS t1
left join {{ source('PSS_AMELIARES_DBO' ,'TBL_GL_PAYMENT_METHOD') }} t2 on t2.LNG_GL_PAYMENT_METHOD_ID_NMBR = t1.LNG_GL_PAYMENT_METHOD_ID_NMBR
left join {{ source('PSS_AMELIARES_DBO' ,'TBL_CURRENCY') }} t3 on t3.LNG_CURRENCY_ID_NMBR = t1.LNG_CURRENCY_ID_NMBR
left join {{ source('PSS_AMELIARES_DBO' ,'TBL_RES_HEADER') }} t4 on t4.LNG_RESERVATION_NMBR = t1.LNG_RESERVATION_NMBR