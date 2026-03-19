-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_sage;

-- Create test tables for analysis
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    total_amount NUMERIC(10,2),
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    category TEXT,
    stock INTEGER DEFAULT 0
);

-- Foreign keys
ALTER TABLE orders ADD CONSTRAINT fk_orders_customer
    FOREIGN KEY (customer_id) REFERENCES customers(id);
ALTER TABLE orders ADD CONSTRAINT fk_orders_product
    FOREIGN KEY (product_id) REFERENCES products(id);

-- Insert test data
INSERT INTO customers (name, email)
SELECT 'Customer ' || i, 'customer' || i || '@example.com'
FROM generate_series(1, 10000) i;

INSERT INTO products (name, price, category, stock)
SELECT 'Product ' || i, (random() * 100)::numeric(10,2),
       CASE (i % 5) WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Books'
       WHEN 2 THEN 'Clothing' WHEN 3 THEN 'Food' ELSE 'Other' END,
       (random() * 1000)::int
FROM generate_series(1, 1000) i;

INSERT INTO orders (customer_id, product_id, quantity, total_amount, status, created_at)
SELECT (random() * 9999 + 1)::int,
       (random() * 999 + 1)::int,
       (random() * 10 + 1)::int,
       (random() * 500)::numeric(10,2),
       CASE (i % 4) WHEN 0 THEN 'pending' WHEN 1 THEN 'shipped'
       WHEN 2 THEN 'delivered' ELSE 'cancelled' END,
       now() - (random() * 90 || ' days')::interval
FROM generate_series(1, 100000) i;

-- Create some intentionally bad indexes for detection
CREATE INDEX idx_orders_duplicate1 ON orders (customer_id);
CREATE INDEX idx_orders_duplicate2 ON orders (customer_id);  -- duplicate!
CREATE INDEX idx_orders_unused ON orders (status, created_at, updated_at, quantity);

-- Create a sequence that's nearly exhausted (for sequence exhaustion test)
CREATE SEQUENCE test_exhausted_seq AS INTEGER START 2000000000 MAXVALUE 2147483647;
SELECT nextval('test_exhausted_seq');

-- Run ANALYZE to populate stats
ANALYZE;
