%sql
create table orders (
  order_id int, 
  order_date date, 
  customer_id int, 
  order_status string
) using delta;

%sql
describe detail orders;

-- format delta
-- id 5df6ccbe-6237-4275-9b38-ebbdc6d30022
-- name workspace.default.orders
-- description  null
-- location 
-- createdAt  2025-12-24T11:40:49.488Z
-- lastModified 2025-12-24T11:40:51.291Z
-- partitionColumns []
-- clusteringColumns  []
-- numFiles 0
-- sizeInBytes  0
-- properties {"delta.parquet.compression.codec":"zstd","delta.enableDeletionVectors":"true","delta.writePartitionColumnsToParquet":"true","delta.enableRowTracking":"true","delta.rowTracking.materializedRowCommitVersionColumnName":"_row-commit-version-col-1cabb8ed-84a5-4dc8-81c2-5406310f5e24","delta.rowTracking.materializedRowIdColumnName":"_row-id-col-86a843e6-d057-4c3a-ab29-9f4a4772281a"}
-- minReaderVersion 3
-- minWriterVersion 7
-- tableFeatures  ["appendOnly","deletionVectors","domainMetadata","invariants","rowTracking"]
-- statistics {"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
-- clusterByAuto  FALSE

%sql
COPY INTO orders
FROM (
  SELECT 
    CAST(order_id AS INT) AS order_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(customer_id AS INT) AS customer_id,
    order_status
  FROM '/Volumes/workspace/default/my_volume/input/file1.json'
)
FILEFORMAT = JSON;

-- num_affected_rows  4
-- num_inserted_rows  4
-- num_skipped_corrupt_files  0

%sql
COPY INTO orders
FROM (
  SELECT 
    CAST(order_id AS INT) AS order_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(customer_id AS INT) AS customer_id,
    order_status
  FROM '/Volumes/workspace/default/my_volume/input/file1.json'
)
FILEFORMAT = JSON;
-- if we run this commands again, then it'll not insert the same records again
-- that's the beauty of copy into commands. it keeps track of the already uploaded files.

-- num_affected_rows  0
-- num_inserted_rows  0
-- num_skipped_corrupt_files  0

%sql
COPY INTO orders
FROM (
  SELECT 
    CAST(order_id AS INT) AS order_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(customer_id AS INT) AS customer_id,
    order_status
  FROM '/Volumes/workspace/default/my_volume/input/file2.json'
)
FILEFORMAT = JSON;

-- num_affected_rows  4
-- num_inserted_rows  4
-- num_skipped_corrupt_files  0

%sql
-- if we run the commands with 5 columns files3.json, ealrier it was 4 columns in file2.json, and file1.json. it'll skip the last columns.
COPY INTO orders
FROM (
  SELECT 
    CAST(order_id AS INT) AS order_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(customer_id AS INT) AS customer_id,
    order_status,
    order_amount
  FROM '/Volumes/workspace/default/my_volume/input/file3.json'
)
FILEFORMAT = JSON;

-- num_affected_rows  4
-- num_inserted_rows  4
-- num_skipped_corrupt_files  0


%sql
select * from orders;
-- order_id order_date  customer_id order_status
-- 1  2013-07-25  11599 CLOSED
-- 2  2013-07-25  256 PENDING_PAYMENT
-- 3  2013-07-25  12111 COMPLETE
-- 4  2013-07-25  8827  CLOSED
-- 5  2013-07-25  11599 CLOSED
-- 6  2013-07-25  256 PENDING_PAYMENT
-- 7  2013-07-25  12111 COMPLETE
-- 8  2013-07-25  8827  CLOSED
-- 5  2013-07-25  11599 CLOSED
-- 6  2013-07-25  256 PENDING_PAYMENT
-- 7  2013-07-25  12111 COMPLETE
-- 8  2013-07-25  8827  CLOSED


%sql
--copy into command doesn't support schema evolution.
--it works well, when the schema is stattic across all the files.
--place the newfiles, file4.json with datatype mismatch
-- in this file, the one row is:
-- {"order_id": 9, "order_date": "2013-07-25 00:00:00.0", "customer_id": COMPLETE, -- "order_status": "COMPLETE", "order_amount": 10}
COPY INTO orders
FROM (
  SELECT 
    CAST(order_id AS INT) AS order_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(customer_id AS INT) AS customer_id,
    order_status
  FROM '/Volumes/workspace/default/my_volume/input/file4.json'
)
FILEFORMAT = JSON;

-- num_affected_rows  4
-- num_inserted_rows  4
-- num_skipped_corrupt_files  0

%sql
select * from orders;
-- order_id order_date  customer_id order_status
-- 1  2013-07-25  11599 CLOSED
-- 2  2013-07-25  256 PENDING_PAYMENT
-- 3  2013-07-25  12111 COMPLETE
-- 4  2013-07-25  8827  CLOSED
-- 5  2013-07-25  11599 CLOSED
-- 6  2013-07-25  256 PENDING_PAYMENT
-- 7  2013-07-25  12111 COMPLETE
-- 8  2013-07-25  8827  CLOSED
-- 5  2013-07-25  11599 CLOSED
-- 6  2013-07-25  256 PENDING_PAYMENT
-- 7  2013-07-25  12111 COMPLETE
-- 8  2013-07-25  8827  CLOSED
-- null null  null  null
-- 10 2013-07-25  256 PENDING_PAYMENT
-- 11 2013-07-25  12111 COMPLETE
-- 12 2013-07-25  8827  CLOSED
