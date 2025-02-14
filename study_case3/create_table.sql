

CREATE TABLE avg_time_per_ship (
    port_id SERIAL PRIMARY KEY,
    port_name VARCHAR(100) NOT NULL,
    location VARCHAR(100) NOT NULL
);

CREATE TABLE port_container_capacity (
    id SERIAL PRIMARY KEY,
    port_id INT REFERENCES port_details(port_id),
    year INT NOT NULL,
    month INT CHECK (month BETWEEN 1 AND 12),
    capacity INT CHECK (capacity >= 0)
);