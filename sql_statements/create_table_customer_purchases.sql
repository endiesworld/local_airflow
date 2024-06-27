CREATE TABLE IF NOT EXISTS customer_purchases (
    id INTEGER PRIMARY KEY,
    product VARCHAR(255) NOT NULL,
    customer_id INTEGER NOT NULL REFERENCES customers (id)
);