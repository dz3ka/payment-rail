-- Reverse dependency order; the settlements index drops with its table. The
-- seed clearing account is deleted after the referencing table is gone.
DROP TABLE IF EXISTS settlements;
DELETE FROM accounts WHERE name = 'onchain_settlement' AND asset = 'USDC';
