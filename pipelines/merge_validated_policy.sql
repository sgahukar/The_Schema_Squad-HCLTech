USE SCHEMA VALIDATEDSCHEMA;

MERGE INTO VALIDATED_POLICY tgt
USING (
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
    FROM RAWSCHEMA.POLICY_RAW_STREAM
    WHERE METADATA$ACTION IN ('INSERT','UPDATE')
) src
ON tgt.policy_number = src.policy_number
WHEN MATCHED THEN UPDATE SET
    customer_id = src.customer_id,
    email = src.email,
    phone = src.phone,
    state = src.state,
    city = src.city,
    effective_date = src.effective_date,
    expiration_date = src.expiration_date,
    annual_premium = src.annual_premium,
    agent_id = src.agent_id,
    agency_region = src.agency_region
WHEN NOT MATCHED THEN INSERT VALUES (
    src.policy_number,
    src.customer_id,
    src.email,
    src.phone,
    src.state,
    src.city,
    src.effective_date,
    src.expiration_date,
    src.annual_premium,
    src.agent_id,
    src.agency_region
);