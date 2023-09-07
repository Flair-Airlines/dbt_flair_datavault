{{
    config(
        materialized = 'view',
    )
}}
select
'Amelia' AS Source,
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
where t4.STR_CAX_REASON IS NULL OR t4.STR_CAX_REASON  = ''
UNION
(
SELECT
	'MIS Choice' AS Source,
	CONVERT_TIMEZONE('UTC',
	'America/Regina',
	PaxAncillaryPayments.PaymentTimestampUTC) AS "Payment Date",
	PAX.lng_reservation_nmbr AS "Reservation Nmbr",
	PAX.PNR AS PNR,
	PaxAncillaryPayments.ChargeCurrencyAmount AS "Payment Amount(CAN)",
	CASE
		WHEN PaxAncillaryPayments.CreditCardType = '0' THEN 'Interac Debit'
		WHEN PaxAncillaryPayments.CreditCardType = '1' THEN 'Master Card'
		WHEN PaxAncillaryPayments.CreditCardType = '2' THEN 'Visa'
		WHEN PaxAncillaryPayments.CreditCardType = '3' THEN 'Discover'
		WHEN PaxAncillaryPayments.CreditCardType = '4' THEN 'AMEX'
		ELSE ''
	END AS "Payment Method",
	PaxAncillaryPayments.Message AS "Payment Description",
	PaxAncillaryPayments.PAYMENTBY AS "Payer",
	'' AS "Payment Notes",
	0 AS "Receipt Number",
	PaxAncillaryPayments.ChargeCurrencyCode AS "Base Currency",
	PaxAncillaryPayments.DEFAULTCURRENCYAMOUNT AS "Base Currency Amount",
	1 AS "Exchange Rate"--,*
FROM
(
	SELECT
		*
	FROM
		{{ source ('MIS_DBO','STG_FLIGHT_SEGMENTS') }}
	WHERE
		active_flag = 'Y') FlightSegments
INNER JOIN
     (
	SELECT
		PAX.*,
		ra.lng_reservation_nmbr,
		-- trip_arr_airport,
		-- trip_dep_airport,
		booking_location,
		channel,
		str_agency_name,
		lng_agency_id_nmbr
	FROM
		{{ source ('MIS_DBO','STG_PAX') }} PAX
	LEFT JOIN (
		SELECT
			DISTINCT str_ref1,
			lng_reservation_nmbr,
			-- trip_arr_airport,
			-- trip_dep_airport,
			booking_location,
			channel,
			str_agency_name,
			lng_agency_id_nmbr
		FROM
			{{ source('PSS_AMELIARES_DBO','VW_RAW_REVENUE_AMELIA')}}
        ) AS ra 
        ON
		PAX.PNR = ra.str_ref1
	WHERE
		ACTIVE_FLAG = 'Y'
) PAX ON
	PAX.FlightID = FlightSegments.ID
	--AND PAX.PNR IN ('BJ3HXA')
INNER JOIN
      (
	SELECT
		*
	FROM
		{{ source('MIS_DBO','STG_PAX_ANCILLARY_CHARGES') }}
	WHERE
		active_flag = 'Y') PaxAncillaryCharges ON
	PaxAncillaryCharges.PaxID = PAX.ID
INNER JOIN
      (
	SELECT
		*
	FROM
		{{ source('MIS_DBO','STG_PAX_ANCILLARY_TRANSACTIONS') }}
	WHERE
		active_flag = 'Y') PaxAncillaryTransactions ON
	PaxAncillaryTransactions.ID = PaxAncillaryCharges.PaxAncillaryTransactionID
INNER JOIN
      (
	SELECT
		*
	FROM
		{{ source('MIS_DBO','STG_PAX_ANCILLARY_PAYMENTS') }}
	WHERE
		active_flag = 'Y') PaxAncillaryPayments ON
	PaxAncillaryPayments.PaxAncillaryTransactionID = PaxAncillaryTransactions.ID	
)