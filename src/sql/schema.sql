CREATE TABLE records (
    bus_id INTEGER,
    timestamp TEXT,
    latitude FLOAT,
    longitude FLOAT,
    doors_open BOOLEAN,
    PRIMARY KEY (bus_id, timestamp)
);

CREATE TABLE status (
    bus_id INTEGER,
    timestamp TEXT,
    latitude FLOAT,
    longitude FLOAT,
    doors_open BOOLEAN,
    PRIMARY KEY (bus_id)
);