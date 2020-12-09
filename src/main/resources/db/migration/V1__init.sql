CREATE TABLE payment (
    id UUID PRIMARY KEY,
    method VARCHAR(255),
    amount DECIMAL,
    time_created TIMESTAMP
);
