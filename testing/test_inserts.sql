INSERT INTO RAWSCHEMA.POLICY_RAW
(policy_number, customer_id, email, phone, state, city,
 effective_date, expiration_date, annual_premium, agent_id, agency_region)
VALUES
('TEST2001','CUST2001','test@mail.com','9999999999','Delhi','Delhi',
 '2025-01-01','2026-01-01','5000','A1','North');

INSERT INTO RAWSCHEMA.CLAIMS_RAW
(claim_id, policy_number, customer_id, claim_payload, source_file_name)
SELECT
    'CLM_TEST_1',
    'TEST2001',
    'CUST2001',
    PARSE_JSON('{
      "address": {"state": "Delhi", "city": "Delhi", "zip": "110001"},
      "claim_type": "AUTO",
      "incident_date": "2025-01-10",
      "fnol_datetime": "2025-01-11 10:00:00",
      "status": "OPEN",
      "report_channel": "ONLINE",
      "total_incurred": "10000",
      "total_paid": "2000"
    }'),
    'test_file.json';