# autoloader will handle the schema evolutions, when new columns added. 
# job will stop & we need to restart it again, then it will evolve the schema.

# serverless cluster doesn't support continuos streaming triger, such as 
# processingTime = 10 seconds/ default infinite streamings / long-running streams process.

# we are doing this in serverless cluster.
# .trigger(availableNow=True) - required for servesless cluster

landing_zone = '/Volumes/workspace/default/my_volume/retail_data/'
orders_data = landing_zone + 'orders_data'
checkpoint_path = landing_zone + 'orders_checkpoint'

orders_df = spark.readStream.format('cloudFiles') \
    .option('cloudFiles.format', 'json') \
        .option('cloudFiles.inferColumnTypes', 'true') \
            .option('cloudFiles.schemaLocation', checkpoint_path ) \
                .load(orders_data)

## never use display in streaming method. it will block the streaming process

orders_df.writeStream.format("delta") \
    .option("checkpointLocation", checkpoint_path) \
        .outputMode('append') \
            .trigger(availableNow=True) \
            .toTable('orderdeltanew')

%fs
ls /Volumes/workspace/default/my_volume/retail_data/orders_checkpoint/

path	                                                                            name	size	modificationTime
dbfs:/Volumes/workspace/default/my_volume/retail_data/orders_checkpoint/_schemas/	_schemas/	0	1766644524407
dbfs:/Volumes/workspace/default/my_volume/retail_data/orders_checkpoint/commits/	commits/	0	1766644530955
dbfs:/Volumes/workspace/default/my_volume/retail_data/orders_checkpoint/metadata	metadata	45	1766644529378
dbfs:/Volumes/workspace/default/my_volume/retail_data/orders_checkpoint/offsets/	offsets/	0	1766644530806
dbfs:/Volumes/workspace/default/my_volume/retail_data/orders_checkpoint/sources/	sources/	0	0
dbfs:/Volumes/workspace/default/my_volume/retail_data/orders_checkpoint/state/	    state/	    0	0


%sql
select * from orderdeltanew;
-- customer_id	order_date	order_id	order_status	_rescued_data
-- 11599	2013-07-25 00:00:00.0	1	CLOSED	null
-- 256	2013-07-25 00:00:00.0	2	PENDING_PAYMENT	null
-- 12111	2013-07-25 00:00:00.0	3	COMPLETE	null
-- 8827	2013-07-25 00:00:00.0	4	CLOSED	null


# re-run the code after uploading th files in orders_data folder

orders_df = spark.readStream.format('cloudFiles') \
    .option('cloudFiles.format', 'json') \
        .option('cloudFiles.inferColumnTypes', 'true') \
            .option('cloudFiles.schemaLocation', checkpoint_path ) \
                .load(orders_data)

# don't use display in readstream process.

orders_df.writeStream.format("delta") \
    .option("checkpointLocation", checkpoint_path) \
        .outputMode('append') \
            .trigger(availableNow=True) \
            .toTable('orderdeltanew')

%sql
select * from orderdeltanew;
-- customer_id	order_date	order_id	order_status	_rescued_data
-- 11599	2013-07-25 00:00:00.0	5	CLOSED	null
-- 256	2013-07-25 00:00:00.0	6	PENDING_PAYMENT	null
-- 12111	2013-07-25 00:00:00.0	7	COMPLETE	null
-- 8827	2013-07-25 00:00:00.0	8	CLOSED	null
-- 11599	2013-07-25 00:00:00.0	1	CLOSED	null
-- 256	2013-07-25 00:00:00.0	2	PENDING_PAYMENT	null
-- 12111	2013-07-25 00:00:00.0	3	COMPLETE	null
-- 8827	2013-07-25 00:00:00.0	4	CLOSED	null


# in this way re-run the code multiple times if you want for serverless cluster.
# never call display or show on a streaming df that u plan to write using a checkpoint.

# inside the checkpoint, spark stores : query pan hash, sink informations, state operators, output mode, if anythhing
# doesn't match exactly, then job will fails, that's why never use display or show method in streaming process.

# in serverless, we have availableNow(manual re-run), in standard cluster we have a processingTime (auto)
# finite streaming triggers: .trigger(availableNow = True), .trigger(once = True), .trigger(once = False)
# infinite/continuous streaming triggers: .trigger(processingTime = '10 seconds').

# for continous ingestion use:
# standard cluster, all-purpose cluster, job cluster

%sql
describe detail orderdeltanew;
-- format	delta
-- id	fbdf5522-81ae-4d10-a7a8-cee02f9b4275
-- name	workspace.default.orderdeltanew
-- description	null
-- location	
-- createdAt	2025-12-25T17:54:41.674Z
-- lastModified	2025-12-25T17:56:20.733Z
-- partitionColumns	[]
-- clusteringColumns	[]
-- numFiles	2
-- sizeInBytes	3329
-- properties	{"delta.enableDeletionVectors":"true","delta.writePartitionColumnsToParquet":"true","delta.enableRowTracking":"true","delta.rowTracking.materializedRowCommitVersionColumnName":"_row-commit-version-col-cb1dc07d-ffa7-4a7d-8c38-e14a994aef59","delta.rowTracking.materializedRowIdColumnName":"_row-id-col-e0166b88-ce98-4a40-9c11-cc34edddaa4b"}
-- minReaderVersion	3
-- minWriterVersion	7
-- tableFeatures	["appendOnly","deletionVectors","domainMetadata","invariants","rowTracking"]
-- statistics	{"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
-- clusterByAuto	FALSE


# for file 3 (5 columns, and 4 rows)

orders_df = spark.readStream.format('cloudFiles') \
    .option('cloudFiles.format', 'json') \
        .option('cloudFiles.inferColumnTypes', 'true') \
            .option('cloudFiles.schemaLocation', checkpoint_path ) \
                .load(orders_data)

## never use display in streaming method. it will block the streaming process

orders_df.writeStream.format("delta") \
    .option("checkpointLocation", checkpoint_path) \
        .outputMode('append') \
            .trigger(availableNow=True) \
            .toTable('orderdeltanew')

## will throw an error:
# autoloader does not evolve schema automatically for streaming writes to delta unless we explicitly enable schema evolution on both read and write sides.

# streaming schema is fixed after first run (file1.json), spark Infers schema from inital files, saves it in checkpoint
# reuse the same schema on every restart. so if we : add ew files / restart the notebook / re-run the same code - spark
# still uses the old schema from checkpoint.
