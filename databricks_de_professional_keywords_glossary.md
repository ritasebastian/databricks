# Databricks Data Engineer Professional — Keyword & Definition Review

**442 concise terms** · Based on the seven attached question-set PDFs (Questions 1–339) · Updated 2026-11-09

> **Purpose:** A GitHub-friendly study reference, not an official Databricks exam answer key. Keywords and topics were selected from the seven PDFs. The definitions are original explanatory summaries, supplemented by explicitly identified current-product documentation below. The question sets contain older and newer platform names and some repeated scenarios.

## Table of contents

- [01. Lakehouse architecture and storage](#01-lakehouse-architecture-and-storage)
- [02. Delta Lake DML, retention, and optimization](#02-delta-lake-dml-retention-and-optimization)
- [03. CDC and slowly changing dimensions](#03-cdc-and-slowly-changing-dimensions)
- [04. Spark fundamentals and PySpark transformations](#04-spark-fundamentals-and-pyspark-transformations)
- [05. Structured Streaming and state](#05-structured-streaming-and-state)
- [06. Auto Loader and ingestion](#06-auto-loader-and-ingestion)
- [07. Lakeflow Spark Declarative Pipelines and expectations](#07-lakeflow-spark-declarative-pipelines-and-expectations)
- [08. Unity Catalog, access, and security](#08-unity-catalog-access-and-security)
- [09. Jobs, scheduling, and orchestration](#09-jobs-scheduling-and-orchestration)
- [10. Development, CI/CD, and dependencies](#10-development-cicd-and-dependencies)
- [11. Performance monitoring and compute](#11-performance-monitoring-and-compute)
- [12. SQL, views, dashboards, and modeling](#12-sql-views-dashboards-and-modeling)
- [13. Delta Sharing, federation, and governance integration](#13-delta-sharing-federation-and-governance-integration)
- [14. Quick contrasts that frequently appear in exam answers](#14-quick-contrasts-that-frequently-appear-in-exam-answers)
- [Current terminology and documentation](#current-terminology-and-documentation)
- [Study workflow and GitHub](#study-workflow-and-github)

---

## 01. Lakehouse architecture and storage

**Relevant PDF questions (examples):** Q13, Q29, Q33, Q39, Q84, Q151, Q203, Q205, Q210, Q324

| Keyword | Short definition / exam reminder |
|---|---|
| **Lakehouse** | Architecture combining data-lake storage with warehouse-style transactions, governance, and analytics. |
| **Bronze layer** | Raw landing layer that preserves source records and metadata for replay or auditing. |
| **Silver layer** | Cleaned, validated, deduplicated, and conformed data prepared for downstream use. |
| **Gold layer** | Curated aggregates and business-ready datasets used for reporting or models. |
| **Medallion architecture** | Organizes pipelines into bronze, silver, and gold processing layers. |
| **Delta Lake** | Open table format and storage layer using Parquet files plus a transaction log for reliable reads and writes. |
| **Parquet** | Columnar file format that supports efficient analytical scans and compression. |
| **JSON** | Semi-structured data format; schema inference and nested fields need special attention. |
| **CSV** | Delimited text format that often requires explicit schema and parsing rules. |
| **ACID transactions** | Atomicity, consistency, isolation, and durability for data changes. |
| **Transaction log (`_delta_log`)** | Records Delta table commits, metadata, and file actions. |
| **Snapshot isolation** | Queries read a consistent table snapshot while other transactions commit. |
| **Optimistic concurrency control** | Detects incompatible concurrent Delta writes during commit rather than locking all writers. |
| **Schema enforcement** | Rejects incompatible writes instead of silently accepting unexpected columns or types. |
| **Schema evolution** | Controlled adaptation of a table schema when new compatible data fields arrive. |
| **Schema inference** | Derives field names and types from source data; convenient, but not a substitute for data contracts. |
| **Managed table** | Table whose data lifecycle and storage are controlled by its catalog/table management system. |
| **External table** | Table registered over data in an explicitly supplied external storage location. |
| **`LOCATION` clause** | Sets a table's physical storage path when creating an external table. |
| **`DROP TABLE`** | Removes a table registration; whether underlying files are deleted depends on managed/external semantics and platform rules. |
| **DBFS** | Databricks File System abstraction; legacy root and mounts differ from recommended Unity Catalog volumes. |
| **DBFS root** | Workspace-root storage; generally avoid it for production governed data. |
| **Cloud object storage** | S3, Azure Blob/ADLS, or GCS used for durable files and datasets. |
| **Unity Catalog volume** | Governed file storage location for non-tabular data in Unity Catalog. |
| **Time travel** | Reads a previous Delta table version or timestamp while required files remain available. |
| **Table history (`DESCRIBE HISTORY`)** | Shows Delta commits, versions, timestamps, and operation details. |

## 02. Delta Lake DML, retention, and optimization

**Relevant PDF questions (examples):** Q8, Q10, Q11, Q22, Q27, Q28, Q59, Q64, Q81, Q97, Q107, Q130, Q189, Q201, Q202, Q233, Q238, Q240, Q248, Q266, Q287, Q288, Q294, Q327

| Keyword | Short definition / exam reminder |
|---|---|
| **`MERGE INTO`** | Upserts or conditionally deletes target rows by matching them to source records. |
| **`WHEN MATCHED`** | MERGE clause applied to matching source/target rows, often for UPDATE or DELETE. |
| **`WHEN NOT MATCHED`** | MERGE clause applied to source rows absent from the target, often for INSERT. |
| **Insert-only merge** | Uses MERGE to add unseen records without changing existing target rows. |
| **Composite key** | Two or more fields that jointly identify a business entity or record. |
| **Deduplication** | Removes repeated records according to an explicitly defined key and ordering rule. |
| **`dropDuplicates`** | Spark DataFrame operation for removing repeated rows or key combinations. |
| **`DELETE FROM`** | Logically deletes rows from a Delta table; physical cleanup is separate. |
| **`UPDATE`** | Modifies matching rows in a Delta table. |
| **`INSERT INTO`** | Adds rows to an existing table. |
| **`CREATE TABLE AS SELECT` (CTAS)** | Creates and populates a table from query results. |
| **`INSERT OVERWRITE`** | Replaces data in a target or targeted partition, according to its supported syntax. |
| **`overwrite` write mode** | Replaces existing target data rather than appending new data. |
| **`append` write mode** | Adds new output rows without replacing prior table data. |
| **`CHECK` constraint** | Rejects writes that violate a declared Boolean data-quality rule. |
| **`NOT NULL` constraint** | Requires a column to contain non-null values. |
| **Generated column** | Column whose value is computed from an expression as rows are written. |
| **Deletion vectors** | Mark deleted or modified rows without immediately rewriting whole Parquet files. |
| **`REORG TABLE ... APPLY (PURGE)`** | Rewrites files to physically purge records represented by deletion vectors where supported. |
| **`VACUUM`** | Removes obsolete data files outside the configured retention window. |
| **Retention period** | Time obsolete files are kept for time travel or recovery before physical cleanup. |
| **`delta.deletedFileRetentionDuration`** | Table setting controlling retention of unreferenced data files for VACUUM. |
| **`delta.logRetentionDuration`** | Table setting governing retention of Delta log history, separate from deleted files. |
| **`OPTIMIZE`** | Compacts and reorganizes Delta files to improve read efficiency. |
| **Bin-packing** | Combines small data files into fewer larger files. |
| **Auto compaction** | Automatically compacts eligible small Delta files after writes. |
| **Optimized writes** | Writes data using more efficient output file sizing and partitioning. |
| **Z-Ordering** | Multidimensional data layout technique to improve file skipping for common filter columns. |
| **Liquid clustering** | Flexible clustering strategy for Delta tables without rigid directory partitions. |
| **`CLUSTER BY`** | Declares clustering keys for supported Delta tables. |
| **`CLUSTER BY AUTO`** | Allows automatic liquid-clustering key selection on supported tables. |
| **Predictive optimization** | Databricks-managed maintenance that can perform operations such as OPTIMIZE, VACUUM, and ANALYZE on eligible tables. |
| **Data skipping** | Avoids reading files when stored statistics show they cannot satisfy a filter. |
| **Min/max statistics** | Recorded value ranges used for pruning irrelevant files. |
| **Partition pruning** | Skips partition directories/files outside filter predicates. |
| **Predicate pushdown** | Pushes supported filters toward the data source to reduce data read. |
| **Partitioning** | Physically groups table data by column values; excessive partitions can create small-file overhead. |
| **High-cardinality column** | Column with many distinct values, often unsuitable as a traditional partition key. |
| **Shallow clone** | Creates a Delta clone that initially references source data files; source-file retention matters. |
| **Deep clone** | Copies source table data and metadata to a separate location. |
| **Change data feed (CDF)** | Delta feature exposing row-level insert/update/delete changes from enabled table commits. |
| **`delta.enableChangeDataFeed`** | Table property to enable future CDF capture. |
| **`_change_type`** | CDF field identifying insert, delete, update preimage, or update postimage. |
| **`_commit_version`** | CDF metadata identifying the Delta commit version of a change. |
| **`_commit_timestamp`** | CDF metadata showing the timestamp of a Delta commit. |

## 03. CDC and slowly changing dimensions

**Relevant PDF questions (examples):** Q13, Q14, Q17, Q28, Q40, Q75, Q109, Q130, Q131, Q132, Q134, Q147, Q242, Q267, Q296, Q304, Q330

| Keyword | Short definition / exam reminder |
|---|---|
| **Change data capture (CDC)** | Source-system record of inserts, updates, and deletes, often with an ordering column. |
| **CDC versus CDF** | CDC captures changes from a source system; CDF exposes changes committed to a Delta table. |
| **SCD Type 1** | Updates attributes in place to keep only their current values. |
| **SCD Type 2** | Preserves historical versions with validity ranges and/or current-row indicators. |
| **Natural key** | Business-defined identifier, such as customer_id, used to recognize the same entity across events. |
| **Surrogate key** | Generated identifier for dimension rows, especially useful for SCD Type 2 versions. |
| **Sequencing column** | Timestamp or monotonic field used to establish the order of CDC updates. |
| **`SEQUENCE BY STRUCT(...)`** | Orders AUTO CDC events by multiple fields, such as event timestamp and tie-breaker ID. |
| **Tombstone** | Marker representing a delete event that may need retention to handle late events. |
| **Out-of-order event** | Record delivered after later changes for the same key; requires event ordering or reconciliation. |
| **Idempotency** | Reprocessing the same input has no unintended additional effect. |
| **Upsert** | Updates an existing keyed row or inserts it when absent. |
| **AUTO CDC** | Lakeflow pipeline functionality that applies ordered CDC events to a target and supports SCD patterns. |
| **AUTO CDC FROM SNAPSHOT** | Derives changes from successive source snapshots for supported pipelines. |
| **`APPLY CHANGES INTO`** | Older declarative CDC syntax appearing in some materials; AUTO CDC is the newer terminology. |
| **`TRACK HISTORY ON`** | Controls which column changes create history in supported SCD Type 2 CDC configurations. |
| **`__START_AT`** | Technical validity start column used by certain AUTO CDC SCD Type 2 targets. |
| **`__END_AT`** | Technical validity end column used by certain AUTO CDC SCD Type 2 targets. |
| **Current-state table** | One latest row per business key, usually resulting from SCD Type 1 processing. |
| **History table** | Keeps earlier valid states for audit, temporal analysis, or SCD Type 2 queries. |

## 04. Spark fundamentals and PySpark transformations

**Relevant PDF questions (examples):** Q9, Q24, Q25, Q26, Q52, Q66, Q70, Q74, Q88, Q93, Q94, Q112, Q142, Q143, Q199, Q200, Q247, Q252, Q254, Q274, Q279, Q284, Q290, Q322, Q329

| Keyword | Short definition / exam reminder |
|---|---|
| **Apache Spark** | Distributed processing engine for batch, SQL, and streaming workloads. |
| **SparkSession** | Main entry point for creating DataFrames, running SQL, and reading data. |
| **DataFrame** | Distributed, schema-aware collection of rows manipulated through lazy transformations. |
| **Lazy evaluation** | Spark postpones most computation until an action needs a result. |
| **Transformation** | Builds a new logical dataset, such as `filter`, `select`, `join`, or `groupBy`. |
| **Action** | Triggers execution, such as `count`, `collect`, `show`, or a write. |
| **Narrow transformation** | Can process output partitions without a full shuffle, such as many filters and projections. |
| **Wide transformation** | Redistributes data across partitions, often requiring a shuffle. |
| **Shuffle** | Network redistribution of data for joins, grouping, sorting, and other wide operations. |
| **Spark partition** | Unit of distributed data processed by tasks; not the same as a Delta table partition. |
| **Task** | Smallest Spark execution unit, usually working on one partition at a stage. |
| **Stage** | Set of tasks separated by shuffle or other execution boundaries. |
| **Job** | Execution triggered by an action; one job can contain multiple stages. |
| **Driver** | Coordinates Spark query plans, schedules tasks, and gathers metadata/results. |
| **Executor** | Worker process that runs tasks and holds cached/shuffle data. |
| **Spark SQL** | SQL execution engine and API for structured data queries. |
| **Catalyst optimizer** | Spark component that analyzes and optimizes relational query plans. |
| **Tungsten** | Spark execution improvements focused on efficient memory and CPU use. |
| **`groupBy`** | Groups rows for aggregations such as sums, counts, and averages. |
| **`agg`** | Applies aggregate expressions after grouping or across a DataFrame. |
| **`join`** | Combines rows according to match conditions; cardinality and join strategy affect cost. |
| **Broadcast join** | Replicates a sufficiently small input to executors to avoid a large shuffle join. |
| **`broadcast()`** | PySpark hint suggesting a broadcast strategy for an eligible join input. |
| **Sort-merge join** | Spark join strategy that shuffles and sorts inputs by join key. |
| **Data skew** | Uneven distribution of keys or tasks causing some workers to run much longer. |
| **Salting** | Adds synthetic key variation to split large skewed groups before recombining results. |
| **Spill** | Movement of intermediate data from memory to disk when memory is insufficient. |
| **Cache** | Stores reused query data in memory and/or disk to avoid recomputing upstream work. |
| **`cache()`** | Requests caching; an action is typically needed to materialize the cache. |
| **`persist()`** | Caches with a selected storage level such as memory-and-disk. |
| **`unpersist()`** | Removes an RDD/DataFrame from cache. |
| **`repartition()`** | Redistributes data, usually with a shuffle, to change partition count or layout. |
| **`coalesce()`** | Commonly reduces partition count with less shuffle than repartition. |
| **`spark.sql.files.maxPartitionBytes`** | Controls a target maximum size of file-scan partitions during Spark ingestion. |
| **Window function** | Computes row-wise results over a partition and ordering without collapsing all rows. |
| **`partitionBy` in a window** | Separates rows into independent calculation groups. |
| **`orderBy` in a window** | Defines row order for rank, running total, lag, and lead operations. |
| **`dense_rank()`** | Assigns consecutive ranks with ties sharing a rank and no gaps. |
| **`row_number()`** | Assigns sequential unique row numbers within ordered window partitions. |
| **`lag()` and `lead()`** | Access earlier and later rows within a window ordering. |
| **`rangeBetween()`** | Defines value-based bounds for a window frame. |
| **`rowsBetween()`** | Defines row-position-based bounds for a window frame. |
| **`transform()`** | Applies a reusable DataFrame-to-DataFrame function for modular ETL logic. |
| **Python UDF** | User-defined function evaluated through Python rather than a native Spark SQL expression. |
| **Pandas UDF** | Vectorized UDF using Arrow-backed batches to reduce Python call overhead. |
| **Arrow** | Columnar memory format enabling efficient data exchange between Spark and Python. |
| **Grouped map Pandas operation** | Applies Pandas logic to each group, with scalability considerations for large/skewed groups. |
| **Stateful Pandas operation** | Stateful grouped processing API allowing per-key state across streaming batches when supported. |
| **Null-safe equality (`<=>`)** | SQL comparison that treats two NULL values as equal. |
| **`explain()`** | Displays logical/physical query plans for performance investigation. |

## 05. Structured Streaming and state

**Relevant PDF questions (examples):** Q3, Q17, Q18, Q19, Q20, Q21, Q23, Q71, Q72, Q73, Q75, Q76, Q93, Q106, Q108, Q114, Q137, Q139, Q141, Q146, Q149, Q194, Q195, Q196, Q197, Q243, Q275, Q285, Q289, Q306, Q320, Q336

| Keyword | Short definition / exam reminder |
|---|---|
| **Structured Streaming** | Spark API that incrementally processes streaming sources using DataFrame-style logic. |
| **Micro-batch** | Default mode for many streams: process new data in small, repeatable batches. |
| **Continuous processing** | Lower-latency streaming execution mode with different guarantees and limitations. |
| **Checkpoint** | Durable record of offsets, progress, and state needed to restart a streaming query. |
| **Checkpoint location** | Storage path used for a stream's progress/state; keep stable for the same query. |
| **Exactly-once semantics** | Intended end-to-end effect of each input once, subject to source/sink and processing design. |
| **At-least-once delivery** | Processing may be retried, so idempotent sinks are essential. |
| **Trigger** | Controls when streaming execution runs and how much data is processed. |
| **`availableNow` trigger** | Processes all available input incrementally and stops after completion. |
| **`once` trigger** | Legacy single-batch processing mode; often superseded by availableNow. |
| **Processing-time trigger** | Starts micro-batches at a configured interval when work is available. |
| **Output mode: append** | Emits final new rows without revising previously emitted output. |
| **Output mode: update** | Emits rows whose result changed during the current trigger. |
| **Output mode: complete** | Re-emits the full aggregation result table each trigger. |
| **Watermark** | Event-time threshold controlling how late data is allowed to affect stateful processing. |
| **Event time** | Timestamp when the business event occurred. |
| **Processing time** | Timestamp when the pipeline processes the event. |
| **Late-arriving data** | Events that reach the pipeline after their event-time order or expected window. |
| **State store** | Persistent streaming state for aggregations, deduplication, and joins. |
| **Stateful aggregation** | Running calculation across micro-batches, typically requiring state and a watermark strategy. |
| **Stream-static join** | Combines each stream micro-batch with a static lookup relation. |
| **Stream-stream join** | Combines two streaming inputs; often requires time bounds and watermarking to manage state. |
| **`foreachBatch`** | Calls custom code on each micro-batch DataFrame, commonly for merges or complex writes. |
| **`batchId`** | Micro-batch identifier useful for deduplicating effects in custom sinks. |
| **`readStream`** | Creates a streaming DataFrame from a supported source. |
| **`writeStream`** | Configures a streaming sink, checkpoint, and trigger. |
| **Kafka offset** | Position in a Kafka partition indicating where consumption should resume. |
| **Kafka retention** | Broker-side limit on how long events remain available; separate durable bronze ingestion helps replay. |
| **Streaming query failure/retry** | Resumes from checkpoint on restart when settings, state, and retention remain compatible. |
| **Backpressure** | Adjusting ingestion throughput to processing capacity to avoid excessive lag. |
| **`maxFilesPerTrigger`** | Limits new files read per micro-batch in supported file sources. |
| **`maxBytesPerTrigger`** | Limits approximate input bytes per trigger in supported streaming sources. |
| **Rate limiting** | Bounds streaming input volume to manage latency and compute cost. |
| **Append-only source** | Source with only additions, simplifying streaming reads and downstream behavior. |
| **Change-aware streaming source** | Stream design able to handle upstream updates/deletes via CDF or other appropriate mechanisms. |
| **`skipChangeCommits`** | Streaming option to ignore file-rewrite commits instead of processing their changed rows; not a CDC substitute. |

## 06. Auto Loader and ingestion

**Relevant PDF questions (examples):** Q29, Q65, Q70, Q76, Q83, Q95, Q106, Q108, Q145, Q198, Q203, Q257, Q273, Q282, Q286, Q289, Q298, Q326, Q338

| Keyword | Short definition / exam reminder |
|---|---|
| **Auto Loader** | Databricks incremental file ingestion using `cloudFiles` with file tracking and schema controls. |
| **`cloudFiles` format** | Source format used to configure Auto Loader reads. |
| **File notification mode** | Detects new files through cloud events and managed file-event services where supported. |
| **Directory listing mode** | Discovers new files by checking object storage directories. |
| **`cloudFiles.schemaLocation`** | Stores inferred schema and schema-change metadata for an Auto Loader source. |
| **`cloudFiles.schemaEvolutionMode`** | Controls behavior when source columns or types change. |
| **`addNewColumns`** | Schema evolution behavior that adds newly detected fields with controlled restart behavior. |
| **`rescue` schema mode** | Keeps unexpected fields in rescued data without evolving the primary schema. |
| **`_rescued_data`** | Column for fields that could not be parsed or matched to the expected schema. |
| **Bad records** | Malformed or unexpected input requiring handling, logging, rescue, or quarantine. |
| **Quarantine table** | Separate dataset for rejected or suspicious records requiring investigation. |
| **Ingestion checkpoint** | State that tracks which files or messages have already been processed. |
| **Exactly-once file ingestion** | File ingestion bookkeeping that prevents unintended rereads under supported usage. |
| **Binary file data source** | Reads entire files such as images as rows containing metadata and bytes. |
| **`binaryFile`** | Spark format for ingesting images and other binary objects as records. |
| **Schema hints** | Auto Loader options for setting selected field types without fully declaring every field. |
| **Source metadata** | File name, path, modification time, offsets, or ingestion timestamps attached to records. |
| **Raw payload preservation** | Retains original source content so lost/incorrectly parsed fields can be recovered later. |
| **Incremental ingestion** | Processes only newly discovered data rather than rescan/rewrite all history. |
| **Backfill** | Reprocesses historical source data to correct gaps or apply new logic. |
| **Data contract** | Agreement on field meaning, types, freshness, and quality requirements between producer and consumer. |

## 07. Lakeflow Spark Declarative Pipelines and expectations

**Relevant PDF questions (examples):** Q35, Q38, Q148, Q207, Q212, Q218, Q227, Q239, Q242, Q265, Q267, Q268, Q302, Q304, Q323, Q328, Q330, Q331, Q337

| Keyword | Short definition / exam reminder |
|---|---|
| **Lakeflow Spark Declarative Pipelines (LDP)** | Databricks declarative framework for managed batch/streaming data pipelines; older materials may call it DLT. |
| **Delta Live Tables (DLT)** | Former product name commonly used by older exam questions and code. |
| **Declarative pipeline** | Defines desired datasets and logic while the platform manages dependency scheduling. |
| **Streaming table** | Continuously or incrementally maintained table in a declarative pipeline. |
| **Materialized view** | Stored query results refreshed by a managed pipeline, where supported incrementality varies. |
| **Pipeline graph (DAG)** | Dependency graph that determines stage order and parallel execution. |
| **Flow** | Pipeline operation that reads sources and writes data to a target dataset. |
| **Append flow** | Adds records from one or more source flows to a target. |
| **`CREATE OR REFRESH STREAMING TABLE`** | SQL statement for defining a streaming table in supported declarative pipelines. |
| **`CREATE OR REFRESH MATERIALIZED VIEW`** | SQL statement for defining a refreshed materialized result. |
| **`read_stream`** | Pipeline source operation indicating incremental streaming consumption. |
| **Expectation** | Named data-quality predicate evaluated against pipeline records. |
| **`EXPECT`** | SQL declaration for an LDP data-quality expectation. |
| **`EXPECT ... ON VIOLATION DROP ROW`** | Drops rows that fail the data-quality test while continuing processing. |
| **`EXPECT ... ON VIOLATION FAIL UPDATE`** | Fails the pipeline update on invalid data. |
| **Warn/track expectation** | Tracks expectation results without discarding failing records. |
| **Expectation metrics** | Counts and other information showing how many records passed or failed rules. |
| **Pipeline event log** | Operational log for lineage, updates, progress, and expectation metrics. |
| **`flow_progress` event** | Event-log event that reports progress and may contain expectation counts. |
| **Pipeline full refresh** | Recomputes eligible pipeline datasets rather than relying on prior incremental state. |
| **Pipeline development mode** | Development-oriented configuration that can trade production durability for iteration speed. |
| **`@dp.table`** | Python decorator used to define pipeline datasets in current declarative pipeline APIs. |
| **`@dp.expect`** | Python decorator for declaring expectations in supported pipeline Python APIs. |
| **Data quality rules as configuration** | Stores reusable expectations in metadata, reducing duplicate code. |

## 08. Unity Catalog, access, and security

**Relevant PDF questions (examples):** Q2, Q6, Q34, Q37, Q42, Q43, Q44, Q45, Q46, Q65, Q79, Q80, Q92, Q98, Q100, Q116, Q153, Q154, Q155, Q157, Q215, Q228, Q230, Q231, Q234, Q241, Q250, Q255, Q258, Q259, Q281, Q291, Q295, Q297, Q303, Q307, Q308, Q313, Q315, Q321

| Keyword | Short definition / exam reminder |
|---|---|
| **Unity Catalog (UC)** | Databricks unified governance system for data objects, permissions, lineage, and sharing. |
| **Metastore** | Top-level Unity Catalog metadata service attaching catalogs to workspaces. |
| **Catalog** | Top-level namespace for schemas in the three-part UC naming system. |
| **Schema** | Namespace grouping tables, views, functions, and other data assets inside a catalog. |
| **Three-level name** | `catalog.schema.object` reference to a governed asset. |
| **Securable object** | UC entity to which ownership or permissions may be assigned. |
| **Ownership** | Principal responsible for an object and authorized to manage it. |
| **`USE CATALOG`** | Permission to access a catalog namespace while other object privileges still apply. |
| **`USE SCHEMA`** | Permission to access a schema namespace while table/view privileges still apply. |
| **`SELECT`** | Permission to read data from tables or views. |
| **`MODIFY`** | Permission to insert, update, or delete table data where supported. |
| **`CREATE TABLE`** | Permission to create tables in a given schema. |
| **`MANAGE`** | Delegated privilege for managing UC object privileges and related administration without full metastore admin rights. |
| **Privilege inheritance** | Grants on eligible parent objects propagate to present and future child objects. |
| **`GRANT`** | SQL command that assigns an object privilege to a principal. |
| **`REVOKE`** | SQL command that removes a previously assigned privilege. |
| **Least privilege** | Give users or service principals only the access required for their duties. |
| **Access control list (ACL)** | Rules associating principals with permitted operations on a resource. |
| **Workspace admin** | Administrative role within a Databricks workspace; not automatically identical to UC ownership. |
| **Metastore admin** | Admin role with broad Unity Catalog metastore management powers. |
| **Service principal** | Non-human identity used to authenticate automated jobs and deployments. |
| **`CAN ATTACH TO`** | Compute permission allowing a user to attach workloads to eligible existing compute. |
| **`CAN RESTART`** | Compute permission allowing start/restart of a cluster when granted. |
| **Cluster policy** | Governance mechanism constraining compute configuration and allowed instance types. |
| **Shared access mode** | Multi-user compute access configuration with governance and isolation rules. |
| **Dedicated access mode** | Compute reserved for an identity or defined group of users, depending on configuration. |
| **Column mask** | Policy or function that changes a sensitive column's visible value based on access rules. |
| **Row filter** | Function/policy determining which rows a user may see. |
| **Dynamic masking** | Applies masking at query time according to the accessing principal or policy. |
| **Attribute-based access control (ABAC)** | Centralized policies based on tags/attributes, including row/column restrictions. |
| **Tag** | Key/value metadata label on governed objects, used for discovery and sometimes policy. |
| **Governed tag** | Controlled UC tag usable for centrally enforced governance policies. |
| **PII** | Personally identifiable information such as names, email addresses, SSNs, or phone numbers. |
| **GDPR** | European privacy regulation relevant to data deletion, purpose limitation, and access governance. |
| **Sensitive-data redaction** | Obscures secret values in notebook output or logs; not a substitute for proper access controls. |
| **Databricks secret scope** | Named collection for access-controlled secret values. |
| **`dbutils.secrets.get`** | Retrieves a secret into code for secure connection configuration. |
| **Credential passthrough** | Legacy approach for propagating user cloud credentials; behavior depends on platform and compute. |
| **Storage credential** | UC object storing cloud authentication to governed external storage. |
| **External location** | UC securable object pairing a cloud path with a storage credential. |
| **Workspace-catalog binding** | Restricts which workspaces may access certain UC objects. |
| **Audit log** | Records administrative and data-access events for security investigation. |
| **Data lineage** | Tracks source-to-target relationships of tables, columns, and data pipelines. |
| **Data classification** | Identifies sensitivity or type of information to guide controls. |

## 09. Jobs, scheduling, and orchestration

**Relevant PDF questions (examples):** Q1, Q3, Q4, Q12, Q41, Q47, Q48, Q57, Q58, Q60, Q61, Q62, Q82, Q89, Q95, Q104, Q115, Q117, Q118, Q120, Q121, Q191, Q213, Q225, Q235, Q253, Q258, Q260, Q261, Q262, Q301, Q312, Q317, Q325

| Keyword | Short definition / exam reminder |
|---|---|
| **Lakeflow Jobs** | Databricks orchestration service for tasks, dependencies, schedules, and retries. |
| **Job definition** | Persistent configuration for one or more tasks, parameters, compute, and triggers. |
| **Job run** | One execution instance of a defined job. |
| **`job_id`** | Identifier of a job definition. |
| **`run_id`** | Identifier of a particular job run, distinct from job_id. |
| **Multi-task job** | Job containing a DAG of interdependent tasks. |
| **Task dependency** | Rule requiring another task to finish before execution can begin. |
| **Task-level retry** | Automatically reruns a failed task according to configured retry rules. |
| **Repair run** | Reruns selected failed or skipped tasks of a multi-task run, preserving successful work where applicable. |
| **Run-as identity** | Principal whose privileges are used when a job executes. |
| **Concurrency limit** | Cap on overlapping runs of the same job. |
| **Job cluster** | Compute provisioned for a job run or shared tasks, generally terminated after use. |
| **All-purpose cluster** | Interactive/general-use compute typically used for development or shared exploration. |
| **Serverless jobs** | Managed execution compute with reduced cluster administration where supported. |
| **Auto termination** | Shuts down idle compute after a configured period to reduce costs. |
| **Cron schedule** | Time-based recurring execution expression. |
| **File-arrival trigger** | Starts a job upon detection of eligible new files. |
| **If/else task** | Branching task that routes execution based on a Boolean expression. |
| **For each task** | Runs a parameterized nested task for items from an input list. |
| **Task values** | Mechanism for passing small values between tasks during a job run. |
| **Job parameters** | Key/value arguments supplied to tasks or notebooks. |
| **`dbutils.widgets`** | Notebook interface for reading and setting runtime input parameters. |
| **`dbutils.jobs.taskValues`** | Utility for exchanging configured small values between job tasks. |
| **Dynamic value references** | `{{...}}` substitutions for job, task, or trigger metadata. |
| **`job.start_time`** | Job start-time dynamic reference; time-zone handling matters for date/day logic. |
| **SQL alert** | Scheduled/triggered condition evaluating SQL query results and notifying subscribers. |
| **Dashboard refresh** | Updates displayed data according to scheduled or manual query execution. |
| **`jobs/create`** | REST API endpoint for defining a job; creating a job does not itself run it. |
| **`jobs/run-now`** | REST API endpoint for starting an existing job. |
| **`jobs/get`** | REST API endpoint for retrieving a specific job definition and tasks. |
| **`jobs/runs/get`** | REST API endpoint for retrieving run state/details. |
| **`jobs/runs/list`** | REST API endpoint for enumerating job runs and their statuses. |
| **`jobs/runs/get-output`** | REST API endpoint returning supported task run output; size and task-type restrictions apply. |
| **`jobs/runs/repair`** | REST API operation to repair a run by rerunning needed tasks. |
| **Backoff** | Increasing delay between retries to reduce repeated load on failing systems. |
| **Idempotent task** | Task that can be rerun without creating duplicate side effects. |

## 10. Development, CI/CD, and dependencies

**Relevant PDF questions (examples):** Q5, Q41, Q53, Q54, Q55, Q56, Q68, Q87, Q88, Q91, Q102, Q103, Q111, Q123, Q129, Q153, Q190, Q213, Q221, Q222, Q223, Q224, Q226, Q247, Q249, Q256, Q263, Q270, Q278, Q291, Q292, Q316, Q318, Q325, Q333, Q334, Q335

| Keyword | Short definition / exam reminder |
|---|---|
| **Git folders (formerly Repos)** | Workspace integration for developing code backed by Git repositories. |
| **Git branch** | Independent line of development for isolated changes. |
| **Pull request (PR)** | Proposed code change for review before merging to a shared branch. |
| **Merge conflict** | Overlapping source edits that must be reconciled before combining branches. |
| **`git pull`** | Fetches and integrates remote repository changes into the current branch. |
| **CI/CD** | Automated source validation, testing, packaging, and deployment. |
| **Declarative Automation Bundles** | Current name for Databricks Asset Bundles; declarative package of jobs, pipelines, code, and target settings. |
| **Databricks Asset Bundles (DAB)** | Former name retained in the provided exam PDFs and many example repos. |
| **`databricks.yml`** | Required root configuration file for a bundle project. |
| **Bundle target** | Environment-specific overrides such as development, staging, or production. |
| **`databricks bundle validate`** | Validates a bundle configuration before deployment. |
| **`databricks bundle deploy`** | Deploys bundle resources to the selected target. |
| **`databricks bundle run`** | Runs a deployed bundle resource. |
| **`databricks bundle generate`** | Generates supported bundle configuration from existing workspace resources. |
| **Databricks CLI** | Command-line interface for workspace resources, jobs, authentication, and bundles. |
| **Databricks REST API** | HTTP interface for programmatic management of platform resources. |
| **OAuth authentication** | Token-based identity flow commonly used for users and automation. |
| **Personal access token (PAT)** | Token credential for API or CLI authentication when supported and permitted. |
| **Terraform** | Infrastructure-as-code tool for provisioning Databricks infrastructure and permissions. |
| **Dev/test/prod separation** | Isolates experimentation and verification from production systems and data. |
| **Unit test** | Checks a small function or processing unit in isolation. |
| **Integration test** | Checks how multiple modules or services work together. |
| **End-to-end test** | Validates a realistic workflow across the application or data pipeline. |
| **Pytest** | Python test runner used to organize and execute automated tests. |
| **Mock** | Test substitute for an external dependency, such as a database or REST service. |
| **Test fixture** | Reusable setup data or state used by tests. |
| **Wheel (`.whl`)** | Built Python distribution artifact installable on Databricks compute. |
| **Source archive (`.tar.gz`)** | Packaged source distribution that may require building/installing dependencies. |
| **`%pip install`** | Notebook-scoped Python package installation command. |
| **PyPI** | Public Python package index for common third-party dependencies. |
| **Workspace file** | File stored in the Databricks workspace, often used for source code or configs. |
| **`sys.path`** | Python list of directories searched when importing modules. |
| **`requirements.txt`** | Text file listing Python dependency requirements. |
| **`pyproject.toml`** | Standard Python project metadata and build configuration file. |
| **JAR** | Java archive for JVM libraries usable by Spark/Scala/Java workloads. |
| **Secret-backed configuration** | Keeps passwords/API keys out of source control and logs. |

## 11. Performance monitoring and compute

**Relevant PDF questions (examples):** Q21, Q24, Q25, Q26, Q49, Q50, Q51, Q63, Q66, Q69, Q70, Q85, Q86, Q94, Q101, Q112, Q142, Q143, Q144, Q188, Q197, Q200, Q219, Q220, Q236, Q245, Q251, Q252, Q266, Q269, Q280, Q288, Q309, Q311, Q319, Q339

| Keyword | Short definition / exam reminder |
|---|---|
| **Spark UI** | Debugging interface for jobs, stages, tasks, SQL plans, storage, and executors. |
| **SQL query profile** | Breakdown of operators, runtime, throughput, and bottlenecks for SQL queries. |
| **Physical plan** | Actual execution operators selected by Spark after optimization. |
| **Logical plan** | Relational operations describing what to compute before physical execution choices. |
| **Adaptive Query Execution (AQE)** | Runtime optimizer that can adjust shuffle partitioning, join strategy, and skew handling. |
| **Task duration distribution** | Min/median/max task times; wide disparities can indicate skew or contention. |
| **GC time** | Time spent in garbage collection; high values may indicate memory pressure. |
| **Executor memory** | Memory available for tasks, caches, and shuffle data in an executor process. |
| **Driver memory** | Memory available to the coordinating process; avoid collecting huge datasets to the driver. |
| **Memory spill** | Intermediate data evicted from memory, often to disk. |
| **Disk spill** | Data spilled to local storage because execution memory is insufficient. |
| **Shuffle read/write** | Volume exchanged among workers due to wide operators. |
| **Input size** | Data scanned from source files or upstream stages. |
| **Wall-clock time** | Actual elapsed time from query start to completion. |
| **CPU utilization** | Portion of computing capacity spent executing rather than waiting. |
| **Autoscaling** | Automatically adjusts worker count within configured limits. |
| **Cluster metrics** | CPU, memory, disk, and network telemetry for the driver and workers. |
| **Ganglia** | Historical cluster monitoring UI referenced in older questions; current monitoring surfaces can differ. |
| **Photon** | Databricks vectorized execution engine accelerating supported SQL/DataFrame workloads. |
| **SQL warehouse** | Managed compute for SQL analytics and dashboards. |
| **Warehouse sizing** | Capacity and concurrency setting that affects latency and cost. |
| **Cold start** | Startup delay while compute resources initialize. |
| **Cost attribution** | Reporting workload consumption by principal, job, workspace, or billing SKU. |
| **`system.billing.usage`** | System table exposing metered Databricks usage with time, SKU, and identity metadata. |
| **DBU** | Databricks Unit used for Databricks consumption metering. |
| **SKU** | Product/service billing category for consumption analysis. |
| **`system.query.history`** | System-table source for supported query history and execution insights. |
| **Query operator** | Physical step such as scan, hash aggregation, join, or sort. |
| **Top operators** | Query profile view highlighting the stages consuming the most time/resources. |
| **Sort bottleneck** | Expensive ordering operation that may involve shuffle and spill. |
| **Data pruning** | Reduces scans by eliminating irrelevant partitions/files before execution. |
| **File size tuning** | Selecting output file size to balance metadata overhead and scan parallelism. |

## 12. SQL, views, dashboards, and modeling

**Relevant PDF questions (examples):** Q4, Q9, Q15, Q16, Q32, Q35, Q36, Q42, Q59, Q60, Q63, Q67, Q69, Q78, Q80, Q96, Q105, Q121, Q125, Q127, Q152, Q155, Q193, Q204, Q207, Q214, Q245, Q262, Q274, Q293, Q310, Q329, Q336

| Keyword | Short definition / exam reminder |
|---|---|
| **SQL view** | Stored query definition, generally computed when referenced rather than storing its result. |
| **Temporary view** | Session-scoped SQL name for a DataFrame or query result. |
| **Global temporary view** | View accessible across sessions in a Spark application through a special namespace. |
| **Materialized view** | Persisted query output refreshed from its sources. |
| **SQL dashboard** | Visual collection of queries/metrics used for business monitoring. |
| **SQL alert condition** | Comparison applied to a query result, often with a threshold. |
| **Aggregation** | Summarizes multiple records using count, sum, average, min, or max. |
| **`COUNT(*)`** | Counts all rows, including rows containing NULL values. |
| **`COUNT(column)`** | Counts non-null values in the specified column. |
| **`GROUP BY`** | Produces a result per group of shared key values. |
| **`HAVING`** | Filters aggregated groups after grouping. |
| **CTE (`WITH`)** | Named temporary query expression used within a SQL statement. |
| **`CASE WHEN`** | Conditional branching expression inside SQL. |
| **`COALESCE`** | Returns the first non-null expression among supplied arguments. |
| **`CAST`** | Converts values between compatible data types. |
| **`date_trunc()`** | Truncates timestamps to units such as day, week, or hour. |
| **`to_date()`** | Converts a compatible input expression to a DATE value. |
| **`dense_rank()` in SQL** | Ranking window function whose tied values share rank with no gaps. |
| **Star schema** | Dimensional model of fact tables linked to descriptive dimension tables. |
| **Fact table** | Contains measured business events and foreign keys to dimensions. |
| **Dimension table** | Holds descriptive attributes used to group/filter fact records. |
| **Conformed dimension** | Shared dimension definitions reusable across facts or business units. |
| **Date dimension** | Calendar-related attributes used for standard time-based analytics. |
| **Range join** | Join using inequalities or interval containment, such as IP addresses within an IP range. |
| **Geospatial function** | Function for processing geographic coordinates, shapes, or distances. |
| **Point-in-polygon** | Spatial test determining whether coordinates fall inside a polygon. |
| **MLflow** | Platform for tracking, registering, and managing machine-learning models and experiments. |
| **MLflow model registry** | Store for model versions, metadata, and managed deployment lifecycle. |
| **Model scoring** | Applies a trained model to records to produce predictions. |
| **Pandas model UDF** | Wraps an ML model as a batch-oriented function to score Spark DataFrames. |

## 13. Delta Sharing, federation, and governance integration

**Relevant PDF questions (examples):** Q37, Q80, Q100, Q155, Q217, Q246, Q264, Q281, Q283, Q299, Q300, Q314

| Keyword | Short definition / exam reminder |
|---|---|
| **Delta Sharing** | Open protocol for securely sharing governed data without making recipients manage copied source tables. |
| **Share** | Named group of tables or other assets made available to selected recipients. |
| **Recipient** | Entity authorized to access a Databricks data share. |
| **Provider** | Organization/account that publishes data through Delta Sharing. |
| **Databricks-to-Databricks sharing** | Sharing model in which Databricks identities and catalogs manage access between accounts. |
| **Open sharing** | Sharing model for external clients using supported credentials and connectors. |
| **Share permissions** | Grants authorizing recipients to access specific shared datasets. |
| **Credential rotation** | Replaces expiring or compromised access credentials to maintain secure sharing. |
| **Read-only sharing** | Recipients query shared data without writing to the provider's source. |
| **Lakehouse Federation** | Queries supported external systems through governed connections without first ingesting every source table. |
| **Foreign catalog** | UC catalog representing objects exposed from a connected external data system. |
| **Connection object** | Governed configuration defining access to an external data source. |
| **Federated query** | SQL query that reads data from an external system through an established connection. |
| **Pushdown to external system** | Executes supported filters or aggregations at the remote source to avoid transferring excess data. |
| **Federation consistency** | Read results may reflect each external source's transactional and timing semantics; not a unified global snapshot. |
| **Data residency** | Requirement governing where certain data may be stored or accessed. |
| **Cross-workspace governance** | Consistent access policies across teams and workspaces using UC and shared identities. |

## 14. Quick contrasts that frequently appear in exam answers

**Relevant PDF questions (examples):** Q10, Q11, Q13, Q18, Q22, Q28, Q55, Q56, Q75, Q81, Q97, Q106, Q107, Q148, Q210, Q237, Q242, Q266, Q302, Q305

| Keyword | Short definition / exam reminder |
|---|---|
| **Bronze vs Silver vs Gold** | Raw/replayable data versus validated conformed data versus curated business-ready data. |
| **CDC vs Delta CDF** | Source-system change log versus Delta table commit-level change stream. |
| **SCD1 vs SCD2** | Overwrite changed attributes versus preserve historical versions. |
| **Time travel vs CDF** | Query old complete snapshots versus consume row-level changes between commits. |
| **`OPTIMIZE` vs `VACUUM`** | Improve active file layout versus delete obsolete physical files after retention. |
| **Z-ORDER vs liquid clustering** | Explicit multidimensional layout versus flexible clustering with evolving keys. |
| **Partition pruning vs data skipping** | Filter out partition groups versus skip individual data files from statistics. |
| **Managed vs external tables** | Catalog-governed data lifecycle versus registration of an externally located dataset. |
| **Stream-static vs stream-stream joins** | Streaming events with lookup snapshot versus two evolving inputs requiring bounded state. |
| **Checkpoint vs watermark** | Recovery progress/state versus event-time lateness/state cleanup rules. |
| **Append vs update vs complete output** | Emit only finalized additions versus changed rows versus the whole result table. |
| **Unit vs integration vs end-to-end tests** | Isolated component versus subsystem interactions versus complete realistic workflow. |
| **ETL vs ELT** | Transform before target load versus load raw data and transform inside the lakehouse. |
| **Job cluster vs all-purpose cluster** | Run-specific compute versus interactive/general-purpose compute. |
| **View vs materialized view** | Compute query results when accessed versus persist refreshed results. |
| **Auto compaction vs optimized writes** | Post-write small-file compaction versus improved file layout during writes. |
| **Data masking vs encryption** | Hide field values per access policy versus protect stored/in-transit bytes. |
| **Row filter vs column mask** | Restrict which records are visible versus change values of sensitive fields. |
| **Schema enforcement vs schema evolution** | Reject incompatible changes versus safely adopt allowed changes. |
| **File notification vs directory listing** | Object-store event discovery versus periodic object listing. |
| **`repartition` vs `coalesce`** | Full data redistribution often for balance versus usually cheaper partition reduction. |
| **Broadcast join vs shuffle join** | Copy a small input to workers versus redistribute larger inputs by key. |
| **Lakeflow pipeline vs Structured Streaming** | Managed declarative dependency/orchestration layer versus lower-level streaming programming API. |
| **Deletion vectors vs purge** | Logical row deletion markers versus physical file rewrite/removal. |
| **Autoscaling vs auto termination** | Change running worker count versus shut down compute when idle. |

## Current terminology and documentation

The points below are **external documentation updates**, not quotations from the seven question PDFs.

- **Bundles naming:** Databricks Asset Bundles were renamed **Declarative Automation Bundles** in March 2026; the `databricks bundle` CLI syntax remains. [Databricks documentation](https://docs.databricks.com/aws/en/dev-tools/bundles/faqs).
- **Current UC security:** Current UC documentation describes privilege inheritance and both per-table row/column rules and centralized ABAC policies. [Databricks documentation](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control).
- **Pipeline terminology:** The PDFs contain DLT and Lakeflow terms; newer documentation uses Lakeflow Spark Declarative Pipelines, including expectations, event logs, and AUTO CDC. [Databricks documentation](https://docs.databricks.com/aws/en/ldp/concepts/spark-declarative-pipelines).
- **Unity Catalog permissions:** The exact privilege list evolves; consult the current UC privileges reference when answering administrative-access questions. [Databricks documentation](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference).

## Study workflow and GitHub

1. Read one category at a time, then open the listed original question numbers in your HTML simulator.
2. Use these keywords as flashcards: cover the definition and explain the term out loud.
3. Revisit contrast terms when two options appear similar.
4. Check the latest Databricks documentation when an older question uses changed syntax, product names, or permissions.
5. Commit this Markdown file next to your exam simulator; GitHub renders Markdown tables and the table of contents automatically.

**Recommended repo paths:** `docs/databricks-de-professional-glossary.md` and `databricks_de_professional_339_question_simulator.html`.

**Source set mapping:** PDF 1: Q1–50 · PDF 2: Q51–100 · PDF 3: Q101–150 · PDF 4: Q151–200 · PDF 5: Q201–250 · PDF 6: Q251–300 · PDF 7: Q301–339.

**Disclaimer:** The list is comprehensive for the recurring named concepts and techniques identified across these question sets, but does not attempt to treat every ordinary English word, identifier, or invented example name as a technical keyword.

