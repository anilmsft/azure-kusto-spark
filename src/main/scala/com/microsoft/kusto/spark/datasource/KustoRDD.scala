package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.UUID

import com.microsoft.azure.kusto.data.Client
import com.microsoft.kusto.spark.utils.{CslCommandsGenerator, KustoClient, KustoQueryUtils, KustoDataSourceUtils => KDSU}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.Map
import scala.collection.JavaConverters._
import scala.collection.mutable

private[kusto] case class KustoPartition(predicate: Option[String], idx: Int) extends Partition {
  override def index: Int = idx
}

private[kusto] case class KustoPartitionInfo(num: Int, column: String, mode: String)

private[kusto] case class KustoStorageParameters(account: String,
                                                 secrete: String,
                                                 container: String,
                                                 isKeyNotSas: Boolean)

private[kusto] case class KustoRddParameters(
                                              sparkSession: SparkSession,
                                              schema: StructType,
                                              cluster: String,
                                              database: String,
                                              query: String,
                                              appId: String,
                                              appKey: String,
                                              authorityId: String)

private[kusto] object KustoRDD {
  private val myName = this.getClass.getSimpleName

  private[kusto] def leanBuildScan(params: KustoRddParameters): RDD[Row] = {
    val kustoClient = KustoClient.getAdmin(params.cluster, params.appId, params.appKey, params.authorityId)

    val kustoResult = kustoClient.execute(params.database, params.query)
    val serializer = KustoResponseDeserializer(kustoResult)
    params.sparkSession.createDataFrame(serializer.toRows, serializer.getSchema).rdd
  }

  private[kusto] def scaleBuildScan(params: KustoRddParameters, storage: KustoStorageParameters, partitionInfo: KustoPartitionInfo): RDD[Row] = {

    setupBlobAccess(params, storage)
    val partitions = calculatePartitions(params, partitionInfo)
    val reader = new KustoReader(partitions, params, storage)//.asInstanceOf[RDD[Row]]
    val directory = KustoQueryUtils.simplifyName(s"${params.appId}/dir${UUID.randomUUID()}/")

    for(partition <- partitions){
      reader.exportPartitionToBlob(partition.asInstanceOf[KustoPartition], params, storage, directory)
    }

    val path = s"wasbs://${storage.container}@${storage.account}.blob.core.windows.net/$directory"
//    params.sparkSession.read
//      .option("basepath", path).
//      parquet(s"$path/*").rdd
    params.sparkSession.read.parquet(s"$path").rdd
  }

  private[kusto] def setupBlobAccess(params: KustoRddParameters, storage: KustoStorageParameters) = {
    val config = params.sparkSession.conf
    if (storage.isKeyNotSas) {
      config.set(s"fs.azure.account.key.${storage.account}.blob.core.windows.net", s"${storage.secrete}")
    }
    else {
      config.set(s"fs.azure.sas.${storage.container}.${storage.account}.blob.core.windows.net", s"${storage.secrete}")
    }
    config.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
  }

  private def calculatePartitions(params: KustoRddParameters, partitionInfo: KustoPartitionInfo): Array[Partition] = {
    partitionInfo.mode match {
      case "hash" => calculateHashPartitions(params, partitionInfo)
      case "integral" | "timestamp" | "predicate" => throw new InvalidParameterException(s"Partitioning mode '${partitionInfo.mode}' is not yet supported ")
      case _ => throw new InvalidParameterException(s"Partitioning mode '${partitionInfo.mode}' is not valid")
    }
  }

  private def calculateHashPartitions(params: KustoRddParameters, partitionInfo: KustoPartitionInfo): Array[Partition] = {
    // Single partition
    if (partitionInfo.num <= 1) return Array[Partition] (KustoPartition(None, 0))

    val partitions = new Array[Partition](partitionInfo.num)
    for(partitionId <- 0 until partitionInfo.num) {
      val partitionPredicate = s" hash(${partitionInfo.column}, ${partitionInfo.num}) == $partitionId"
      partitions(partitionId) = KustoPartition(Some(partitionPredicate), partitionId)
    }
    partitions
  }
}

private[kusto] class KustoReader(partitions: Array[Partition], params: KustoRddParameters, storage: KustoStorageParameters)
  {
    private val myName = this.getClass.getSimpleName
    val client = KustoClient.getAdmin(params.cluster, params.appId, params.appKey, params.authorityId)

  // Export a single partition from Kusto to transient Blob storage.
  // Returns the directory path for these blobs
  private[kusto] def exportPartitionToBlob(partition: KustoPartition,
                                           params: KustoRddParameters,
                                           storage: KustoStorageParameters,
                                           directory: String): Unit = {

    val exportCommand = CslCommandsGenerator.generateExportDataCommand(
      params.appId,
      params.query,
      storage.account,
      storage.container,
      directory,
      storage.secrete,
      storage.isKeyNotSas,
      partition.idx,
      partition.predicate
    )

    client.execute(params.database, exportCommand).getValues.asScala.map(row => row.get(0)).toList
  }

  private[kusto] def importPartitionFromBlob(
                                              params: KustoRddParameters,
                                              storage: KustoStorageParameters,
                                              directory: String
                                            ): DataFrame = {

    params.sparkSession.read.parquet(s"wasbs://${storage.container}@${storage.account}.blob.core.windows.net/$directory")
  }
}
