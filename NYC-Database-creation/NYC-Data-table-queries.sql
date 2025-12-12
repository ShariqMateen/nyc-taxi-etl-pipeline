CREATE TABLE trips_clean (
    VendorID               INTEGER,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        DOUBLE PRECISION,
    trip_distance          DOUBLE PRECISION,
    RatecodeID             INTEGER,
    store_and_fwd_flag     BOOLEAN,
    PULocationID           BIGINT,
    DOLocationID           BIGINT,
    payment_type           INTEGER,
    fare_amount            DOUBLE PRECISION,
    extra                  DOUBLE PRECISION,
    mta_tax                DOUBLE PRECISION,
    tip_amount             DOUBLE PRECISION,
    tolls_amount           DOUBLE PRECISION,
    improvement_surcharge  DOUBLE PRECISION,
    total_amount           DOUBLE PRECISION,
    congestion_surcharge   DOUBLE PRECISION,
    airport_fee            DOUBLE PRECISION,
    trip_duration_minutes  DOUBLE PRECISION,
    average_speed_mph      DOUBLE PRECISION,
    pickup_date            DATE,
    pickup_hour            INTEGER,
    pickup_weekday         TEXT
);




CREATE TABLE trips_raw (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count SMALLINT,
    trip_distance DECIMAL(6,3),          
    RatecodeID SMALLINT,
    store_and_fwd_flag CHAR(1),
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DECIMAL(7,2),           
    extra DECIMAL(6,2),
    mta_tax DECIMAL(5,2),
    tip_amount DECIMAL(7,2),
    tolls_amount DECIMAL(7,2),
    improvement_surcharge DECIMAL(5,2),
    total_amount DECIMAL(8,2),
    congestion_surcharge DECIMAL(6,2),
    airport_fee DECIMAL(6,2)
);

ALTER TABLE trips_raw
    ALTER COLUMN trip_distance TYPE DOUBLE PRECISION USING trip_distance::float;

ALTER TABLE trips_raw
ALTER COLUMN passenger_count TYPE DOUBLE PRECISION
USING passenger_count::DOUBLE PRECISION;

ALTER TABLE trips_raw
ALTER COLUMN RatecodeID TYPE DOUBLE PRECISION USING RatecodeID::DOUBLE PRECISION;

ALTER TABLE trips_raw
ALTER COLUMN fare_amount TYPE DOUBLE PRECISION
USING fare_amount::DOUBLE PRECISION;

DROP TABLE trips_raw;

SELECT COUNT(*) FROM trips_clean;

SELECT * FROM trips_raw limit 10;
