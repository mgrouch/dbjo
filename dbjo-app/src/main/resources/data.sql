INSERT INTO client (id, email, name, created_at) VALUES
  (1, 'alice@example.com', 'Alice Doe', CURRENT_TIMESTAMP),
  (2, 'bob@example.com', 'Bob Smith', CURRENT_TIMESTAMP),
  (3, 'cara@example.com', 'Cara Jones', CURRENT_TIMESTAMP);

INSERT INTO product (id, sku, title, price_cents) VALUES
  (1, 'SKU-RED-001', 'Red Widget', 1299),
  (2, 'SKU-BLU-002', 'Blue Widget', 1499),
  (3, 'SKU-GRN-003', 'Green Widget', 999);

INSERT INTO purchase (id, client_id, product_id, qty, ordered_at) VALUES
  (1, 1, 1, 1, CURRENT_TIMESTAMP);
--  (0, 1, 0, CURRENT_TIMESTAMP),
--  (0, 2, 1, CURRENT_TIMESTAMP),
--  (2, 0, 0, CURRENT_TIMESTAMP);
