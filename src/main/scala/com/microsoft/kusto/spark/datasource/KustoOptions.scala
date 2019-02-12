package com.microsoft.kusto.spark.datasource

import java.util.Locale

object KustoOptions {
  private val kustoOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    kustoOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  // AAD application identifier of the client
  val KUSTO_AAD_CLIENT_ID: String = newOption("kustoAADClientID")
  // AAD authentication authority
  val KUSTO_AAD_AUTHORITY_ID: String = newOption("kustoAADAuthorityID")
  // AAD application key for the client
  val KUSTO_AAD_CLIENT_PASSWORD: String = newOption("kustoClientAADClientPassword")
  // Target/source Kusto cluster for writing/reading the data.
  val KUSTO_CLUSTER: String = newOption("kustoCluster")
  val KUSTO_CONNECTION_TIMEOUT: String = newOption("kustoConnectionTimeout")
  val KUSTO_CREATE_TABLE_COLUMN_TYPES: String = newOption("createTableColumnTypes")
  val KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES: String = newOption("customSchema")

  // Target/source Kusto database for writing/reading the data. See KustoSink.md/KustoSource.md for
  // required permissions
  val KUSTO_DATABASE: String = newOption("kustoDatabase")
  val KUSTO_QUERY: String = newOption("kustoQuery")
  val KUSTO_QUERY_RETRY_TIMES: String = newOption("kustoQueryRetryTimes")
  val KUSTO_SESSION_INIT_STATEMENT: String = newOption("sessionInitStatement")
  // Target/source Kusto table for writing/reading the data. See KustoSink.md/KustoSource.md for
  // required permissions
  val KUSTO_TABLE: String = newOption("kustoTable")
  // If set to 'FailIfNotExist', the operation will fail if the table is not found
  // in the requested cluster and database.
  // If set to 'CreateIfNotExist' and the table is not found in the requested cluster and database,
  // it will be created, with a schema matching the DataFrame that is being written.
  // Default: 'FailIfNotExist'
  val KUSTO_TABLE_CREATE_OPTIONS: String = newOption("tableCreateOptions")
  val KUSTO_TXN_ISOLATION_LEVEL: String = newOption("isolationLevel")
  val KUSTO_TRUNCATE: String = newOption("truncate")
  // When writing to Kusto, allows the driver to complete operation asynchronously.  See KustoSink.md for
  // details and limitations. Default: 'false'
  val KUSTO_WRITE_ENABLE_ASYNC: String = newOption("writeEnableAsync")
  // When writing to Kusto, limits the number of rows read back as BaseRelation. Default: '1'.
  // To read back all rows, set as 'none'
  val KUSTO_WRITE_RESULT_LIMIT: String = newOption("writeResultLimit")
  val KUSTO_READ_MODE: String = newOption("readMode")

  // Partitioning parameters
  val KUSTO_PARTITION_MODE_HASH: String = newOption("hash")
  val KUSTO_LOWER_BOUND: String = newOption("lowerBound")
  val KUSTO_UPPER_BOUND: String = newOption("upperBound")
  val KUSTO_NUM_PARTITIONS: String = newOption("numPartitions")
  val KUSTO_PARTITION_COLUMN: String = newOption("partitionColumn")

  object SinkTableCreationMode extends Enumeration {
    type SinkTableCreationMode = Value
    val CreateIfNotExist, FailIfNotExist = Value
  }

  // Blob Storage access parameters for source connector when working in 'scale' mode (read)

  // Transient storage account when reading from Kusto
  val KUSTO_BLOB_STORAGE_ACCOUNT_NAME: String = newOption("blobStorageAccountName")
  // Storage account key. Use either this or SAS key to access the storage account
  val KUSTO_BLOB_STORAGE_ACCOUNT_KEY: String = newOption("blobStorageAccountKey")
  // SAS access key: a complete query string of the SAS as a container
  // Use either this or storage account key to access the storage account
  val KUSTO_BLOB_STORAGE_SAS_KEY: String = newOption("blobStorageSasKey")
  // Blob container name
  val KUSTO_BLOB_CONTAINER: String = newOption("blobContainer")

  val supportedReadModes: Set[String] = Set("lean", "scale")
  val supportedPartitioningModes: Set[String] = Set("hash", "integral", "timestamp", "predicate")
}
