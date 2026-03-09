CREATE OR REPLACE TABLE VALIDATED_POLICY AS
SELECT
    policy_number,
    customer_id,
    TRIM(LOWER(email)) AS email,
    REGEXP_REPLACE(phone, '[^0-9]', '') AS phone,
    INITCAP(state) AS state,
    INITCAP(city) AS city,
    TRY_TO_DATE(effective_date) AS effective_date,
    TRY_TO_DATE(expiration_date) AS expiration_date,
    TRY_TO_NUMBER(annual_premium) AS annual_premium,
    agent_id,
    agency_region
FROM RAWSCHEMA.POLICY_RAW
WHERE policy_number IS NOT NULL;