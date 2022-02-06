-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `praxis-cab-338710.trips_data_all.fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_praxis-cab-338710/raw/fhv_tripdata_2019-*.parquet']
);

CREATE OR REPLACE TABLE `praxis-cab-338710.trips_data_all.fhv_tripdata_partitoned_clustered`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `praxis-cab-338710.trips_data_all.fhv_tripdata`

-- Q1
SELECT count(*) FROM `praxis-cab-338710.trips_data_all.fhv_tripdata_partitoned_clustered`

-- Q2
SELECT count(DISTINCT dispatching_base_num) FROM `praxis-cab-338710.trips_data_all.fhv_tripdata_partitoned_clustered`

-- Q4
SELECT count(*) FROM `praxis-cab-338710.trips_data_all.fhv_tripdata_partitoned_clustered` WHERE 
DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31' AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');
