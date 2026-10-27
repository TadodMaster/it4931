-- Rule implementation #3 (oracle, DuckDB SQL). rule: 1 = v1, 2 = v2.
CREATE OR REPLACE MACRO signed_amount(t, a) AS
  CASE WHEN t IN ('TOPUP','REFUND','TRANSFER_IN','CASHBACK') THEN a
       WHEN t IN ('PAYMENT','TRANSFER_OUT') THEN -a END;

CREATE OR REPLACE MACRO spend_delta(rule, t, a, is_self) AS
  CASE WHEN rule = 1 THEN
         CASE WHEN t IN ('PAYMENT','TRANSFER_OUT') THEN a END
       WHEN rule = 2 THEN
         CASE WHEN t = 'PAYMENT' THEN a
              WHEN t = 'REFUND' THEN -a
              WHEN t = 'TRANSFER_OUT' AND NOT is_self THEN a END
  END;

CREATE OR REPLACE MACRO spend_category(t, merchant_category) AS
  CASE WHEN t IN ('PAYMENT','REFUND') THEN merchant_category
       WHEN t = 'TRANSFER_OUT' THEN 'TRANSFER' END;
