-- total records
SELECT COUNT(*) "Count" FROM raw_yellow_tripdata;


-- observe NULL values
SELECT vendorid, COUNT(*) "Count"
FROM  raw_yellow_tripdata
GROUP BY vendorid
ORDER BY 1;

-- observe other categories
SELECT pulocationid, COUNT(*) "Count"
FROM   raw_yellow_tripdata
GROUP BY pulocationid
ORDER BY 1;


-- observe NULL values
SELECT payment_type, COUNT(*) "Count"
FROM   raw_yellow_tripdata
GROUP BY payment_type
ORDER BY 1;

-- observe other columns with NULL values
SELECT * 
FROM   raw_yellow_tripdata
WHERE  vendorid IS NULL
LIMIT 100;



-- tpep_pickup_datetime is defined as STRING
-- observe record counts that falls outside of the time period 
SELECT SUBSTR(tpep_pickup_datetime, 1, 7) "Period", COUNT(*) "Total Records"
FROM   raw_yellow_tripdata
GROUP BY SUBSTR(tpep_pickup_datetime, 1, 7) 
ORDER BY 1;

-- Count records that falls outside of year 2020.
SELECT COUNT(*) "Count"
FROM   raw_yellow_tripdata 
WHERE  SUBSTR(tpep_pickup_datetime, 1, 7) NOT LIKE '2020%';

-- Records with NULL categories like Vendor ID
SELECT COUNT(*) "Count"
FROM   raw_yellow_tripdata
WHERE  vendorid IS NULL
AND    SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020%';


-- Total records in BER months, excluding columns with missing Vendor ID
SELECT COUNT(*) "Count"
FROM   raw_yellow_tripdata
WHERE  vendorid IS NOT NULL
AND    SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-1%';

-- explore data with lookup information
-- observe column names from lookup tables
SELECT td.*, pu.*, do.*
FROM   raw_yellow_tripdata td, 
       raw_taxi_zone_lookup pu, 
       raw_taxi_zone_lookup do 
WHERE  td.pulocationid = pu.locationid AND
       td.pulocationid = do.locationid AND
       vendorid IS NOT NULL AND
       SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-1%'
LIMIT 100;


-- Count total joined records for the last quarter of 2020.
SELECT COUNT(*) "Count"
FROM   raw_yellow_tripdata td, 
       raw_taxi_zone_lookup pu, 
       raw_taxi_zone_lookup do 
WHERE  td.pulocationid = pu.locationid AND
       td.pulocationid = do.locationid AND
       vendorid IS NOT NULL AND
       SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-1%';
       
       
-- total records
SELECT COUNT(*) "Count"
FROM   yellow_tripdata;

-- records per month
SELECT DATE_TRUNC('month', pickup_datetime) "Period", 
       COUNT(*) "Total Records"
FROM   yellow_tripdata
GROUP BY DATE_TRUNC('month', pickup_datetime)
ORDER BY 1;


SELECT * FROM "nyctaxi_db"."v_yellow_tripdata" limit 10;

SELECT vendor_name "Vendor",
       rate_type "Rate Type", 
       payment_type "Payment Type",
       ROUND(AVG(fare_amount), 2) "Fare",
       ROUND(AVG(extra), 2) "Extra",
       ROUND(AVG(mta_tax), 2) "MTA",
       ROUND(AVG(tip_amount), 2) "Tip",
       ROUND(AVG(tolls_amount), 2) "Toll",
       ROUND(AVG(improvement_surcharge), 2) "Improvement",
       ROUND(AVG(congestion_surcharge), 2) "Congestion",
       ROUND(AVG(total_amount), 2) "Total"
FROM   v_yellow_tripdata
GROUP BY vendor_name,
         rate_type,
         payment_type
ORDER BY 1, 2, 3;




SELECT 
  date_format(pickup_datetime, '%Y-%m') AS month,
  SUM(total_amount) AS total_revenue
FROM yellow_tripdata
GROUP BY 1
ORDER BY 1;



SELECT 
pu_borough, SUM(total_amount) AS total_revenue
FROM yellow_tripdata
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10


CREATE OR REPLACE VIEW v_yellow_tripdata
AS
SELECT CASE vendor_id
            WHEN 1 THEN 'Creative Mobile'
            WHEN 2 THEN 'VeriFone'
            ELSE 'No Data'
       END "vendor_name",
       pickup_datetime,
       dropoff_datetime,
       passenger_count,
       trip_distance,
       CASE ratecodeid
            WHEN 1 THEN 'Standard Rate'
            WHEN 2 THEN 'JFK'
            WHEN 3 THEN 'Newark'
            WHEN 4 THEN 'Nassau/Westchester'
            WHEN 5 THEN 'Negotiated Fare'
            WHEN 6 THEN 'Group Ride'
            WHEN 99 THEN 'Special Rate'
            ELSE 'No Data'
       END "rate_type",
       store_and_fwd_flag,
       pu_borough,
       pu_zone,
       pu_service_zone,
       do_borough,
       do_zone,
       do_service_zone,
       CASE payment_type
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided Trip'
            ELSE 'No Data'
       END "payment_type",
       fare_amount,
       extra,
       mta_tax,
       tip_amount,
       tolls_amount,
       improvement_surcharge,
       congestion_surcharge,
       total_amount
FROM   yellow_tripdata;


