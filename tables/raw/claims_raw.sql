CREATE OR REPLACE TABLE CLAIMS_RAW (
    claim_id          STRING,
    policy_number     STRING,
    customer_id       STRING,
    claim_payload     VARIANT,
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file_name  STRING
);