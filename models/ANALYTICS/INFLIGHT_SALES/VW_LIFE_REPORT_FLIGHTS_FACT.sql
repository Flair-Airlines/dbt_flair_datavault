{{
    config(
        materialized = 'view',
    )
}}

SELECT 
    LIFE_REPORT_FLIGHTS_FACT_IDENTIFIER as "Life Report Flights Fact Identifier",
	AIRCRAFT_IDENTIFIER as "Aircraft Identifier",
	FLIGHT_IDENTIFIER as "Flight Identifier",
	CURRENCY_CODE as "Currency Code",
	PRODUCT_IDENTIFIER as "Product Identifier",
	ORDER_IDENTIFIER as "Order Identifier",
	TAX_INFO_IDENTIFIER as "Tax Info Identifier",
	DISCOUNT_REASON_IDENTIFIER as "Discount Reason Identifier",
	"_LINE" as "Line",
	DEPARTURE_AIRPORT_CODE as "Departure Airport Code",
	ARRIVAL_AIRPORT_CODE as "Arrival Airport Code",
	QUANTITY as "Quantity",
	PRICE_AMOUNT as "Price Amount",
	BASE_AMOUNT as "Base Amount",
	DISCOUNT_AMOUNT as "Discount Amount",
	TAX_AMOUNT as "Tax Amount",
	TOTAL_GROSS_AMOUNT as "Total Gross Amount",
	INSERTDATETIME as "Insert Datetime",
	"_FILE" as "File"
FROM {{ source('INFLIGHTSALES_FACT_STG', 'LIFE_REPORT_FLIGHTS_FACT') }}