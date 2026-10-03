-- The source database of the demo. Debezium snapshots these rows on first start
-- and then streams every insert, update and delete from the write-ahead log.
CREATE TABLE customers (
    id   integer PRIMARY KEY,
    name text    NOT NULL,
    tier text    NOT NULL
);

ALTER TABLE customers REPLICA IDENTITY FULL;

INSERT INTO customers (id, name, tier) VALUES
    (1, 'Acme',    'enterprise'),
    (2, 'Globex',  'free'),
    (3, 'Initech', 'enterprise');
