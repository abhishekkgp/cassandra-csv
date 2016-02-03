CREATE KEYSPACE my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE my_keyspace.my_table (
    id int,
    transaction_type text,
    value decimal,
    PRIMARY KEY (id)
);
