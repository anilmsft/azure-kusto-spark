package com.microsoft.kusto.spark

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.microsoft.azure.kusto.ingest.IngestClient
import com.microsoft.azure.kusto.ingest.result.{IngestionResult, IngestionStatus}
import com.microsoft.kusto.spark.datasink.KustoSink
import com.microsoft.kusto.spark.datasource.{AadApplicationAuthentication, KustoOptions, WriteOptions, KustoCoordinates}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU, KustoConstants => KCONST}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class KustoSinkTests extends FlatSpec with MockFactory with Matchers with BeforeAndAfterAll {
  private val loggingLevel: Option[String] = Option(System.getProperty("logLevel"))
  if (loggingLevel.isDefined) KDSU.setLoggingLevel(loggingLevel.get)

  private val nofExecutors = 4
  private val spark: SparkSession = SparkSession.builder()
    .appName("KustoSink")
    .master(f"local[$nofExecutors]")
    .getOrCreate()

  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _
  private val kustoCluster = "KustoCluster"
  private val kustoDatabase = "kustoDatabase"
  private val kustoTable = "kustoTable"
  private val appId: String = "KustoSinkTestApplication"
  private val appKey: String = "KustoSinkTestKey"
  private val appAuthorityId: String = "KustoSinkAuthorityId"

  override def beforeAll(): Unit = {
    super.beforeAll()

    sc = spark.sparkContext
    sqlContext = spark.sqlContext
  }

  override def afterAll(): Unit = {
    super.afterAll()

    sc.stop()
  }

  private def getSink: KustoSink =
    new KustoSink(
      sqlContext,
      KustoCoordinates(kustoCluster, kustoDatabase, Some(kustoTable)),
      AadApplicationAuthentication(appId, appKey, appAuthorityId),
      WriteOptions(writeResultLimit = KustoOptions.NONE_RESULT_LIMIT, timeout = KCONST.defaultTimeoutLongRunning))

  private val rowId = new AtomicInteger(1)

  private def newRow(): String = s"row-${rowId.getAndIncrement()}"

  // "KustoSink"
  ignore should "ingest per partition and fail on authentication with invalid key" in {
    val kustoSink = getSink

    import spark.implicits._

    val seq = Seq("1", "2", "3", "4").map(v => (newRow(), v))
    val df = seq.toDF("name", "value")

    val kustoIngestionClient = stub[IngestClient]
    val result = new IngestionResult {
      override def getIngestionStatusCollection: util.List[IngestionStatus] = {
        null
      }

      override def getIngestionStatusesLength: Int = {
        0
      }
    }

    (kustoIngestionClient.ingestFromStream _).when(*, *).returns(result)

    try {
      kustoSink.addBatch(1L, df)
    }
    catch {
      case ex: Exception =>
        assert(ex.getCause.toString.contains("AuthenticationException"))
    }
  }
}
