-- Drop deliveries first: it holds the FK into subscriptions.
DROP TABLE IF EXISTS webhook_deliveries;
DROP TABLE IF EXISTS webhook_subscriptions;
