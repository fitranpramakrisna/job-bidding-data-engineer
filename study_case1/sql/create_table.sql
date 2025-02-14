CREATE TABLE avg_time_per_ship (
    id SERIAL PRIMARY KEY,
    ship_id VARCHAR(100) NOT NULL,
    operation_time INT
);

CREATE TABLE crane_operation (
    id SERIAL PRIMARY KEY,
    crane_id VARCHAR(100) NOT NULL,
    operation_time INT
);