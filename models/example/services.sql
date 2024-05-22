
{{config(materialized='table')}}

WITH clickhouse_irembo_services AS (
    SELECT 
        JSONExtractString(_airbyte_data, 'id') AS id,
        JSONExtractString(_airbyte_data, 'state') AS state,
        JSONExtractString(_airbyte_data, 'version') AS version,
        JSONExtractString(_airbyte_data, 'application_code') AS application_code,
        JSONExtractString(_airbyte_data, 'code') AS code,
        JSONExtractString(_airbyte_data, 'description') AS description,
        JSONExtractString(_airbyte_data, 'name') AS name,
        JSONExtractString(_airbyte_data, 'base_price') AS base_price,
        JSONExtractString(_airbyte_data, 'payment_expiration_time') AS payment_expiration_time,
        JSONExtractString(_airbyte_data, 'price_type') AS price_type,
        JSONExtractString(_airbyte_data, 'processing_days') AS processing_days,
        JSONExtractString(_airbyte_data, 'has_digital_certificate') AS has_digital_certificate,
        JSONExtractString(_airbyte_data, 'currency_id') AS currency_id,
        JSONExtractString(_airbyte_data, 'service_owner') AS service_owner,
        JSONExtractString(_airbyte_data, 'grouped_service_id') AS grouped_service_id,
        JSONExtractString(_airbyte_data, 'payment_account_id') AS payment_account_id,
        JSONExtractString(_airbyte_data, 'multi_currency') AS multi_currency,
        JSONExtractString(_airbyte_data, 'suspended') AS suspended,
        JSONExtractString(_airbyte_data, 'suspension_message') AS suspension_message,
        JSONExtractString(_airbyte_data, 'has_backend_tool') AS has_backend_tool,
        JSONExtractString(_airbyte_data, 'officer_suspended') AS officer_suspended,
        JSONExtractString(_airbyte_data, 'information_message') AS information_message,
        JSONExtractString(_airbyte_data, 'discontinued') AS discontinued,
        JSONExtractString(_airbyte_data, 'payment_integration_type') AS payment_integration_type,
        JSONExtractString(_airbyte_data, 'payment_merchant') AS payment_merchant,
        JSONExtractString(_airbyte_data, 'service_form_type') AS service_form_type,
        JSONExtractString(_airbyte_data, 'form_information_message') AS form_information_message,
        JSONExtractString(_airbyte_data, 'change_request_id') AS change_request_id

    FROM 
        default_raw__stream_irembo_service 
)


SELECT * FROM clickhouse_irembo_services