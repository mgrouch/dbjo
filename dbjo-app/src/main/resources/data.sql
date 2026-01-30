INSERT INTO client (email, name, created_at) VALUES
  ('alice@example.com', 'Alice Doe', CURRENT_TIMESTAMP),
  ('bob@example.com', 'Bob Smith', CURRENT_TIMESTAMP),
  ('cara@example.com', 'Cara Jones', CURRENT_TIMESTAMP);

INSERT INTO product (sku, title, price_cents) VALUES
  ('SKU-RED-001', 'Red Widget', 1299),
  ('SKU-BLU-002', 'Blue Widget', 1499),
  ('SKU-GRN-003', 'Green Widget', 999);

INSERT INTO purchase (client_id, product_id, qty, ordered_at) VALUES
  (0, 0, 1, CURRENT_TIMESTAMP);
--  (0, 1, 0, CURRENT_TIMESTAMP),
--  (0, 2, 1, CURRENT_TIMESTAMP),
--  (2, 0, 0, CURRENT_TIMESTAMP);
