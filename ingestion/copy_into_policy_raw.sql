COPY INTO POLICY_RAW (
    policy_number,
    customer_id,
    first_name,
    last_name,
    ssn,
    email,
    phone,
    address,
    city,
    state,
    zip,
    policy_type,
    effective_date,
    expiration_date,
    annual_premium,
    payment_frequency,
    renewal_flag,
    policy_status,
    marital_status,
    agent_id,
    agency_name,
    agency_region
)
FROM @POLICY_CLAIM_STAGE
FILE_FORMAT = (FORMAT_NAME = CSV_POLICY_FORMAT)
ON_ERROR = 'CONTINUE';

SELECT * FROM POLICY_RAW;