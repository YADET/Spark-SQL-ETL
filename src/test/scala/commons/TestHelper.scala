package commons


import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, WordSpec}
import org.apache.spark.SparkContext

object TestHelper {


  implicit def strToSqlDate(str: String): java.sql.Date =
    new java.sql.Date(new java.text.SimpleDateFormat("yyyy-MM-dd").parse(str).getTime)

  implicit def strToSqlTime(str: String): java.sql.Timestamp =
    new java.sql.Timestamp(new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm a").parse(str).getTime)


}

abstract class TestHelper extends FlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll with Eventually {

  val cfg = ConfigFactory.load("config.conf")


  private val sparkSession = SparkSession
    .builder
    .appName("test")
    .config("spark.driver.memory", "3g")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.ui.enabled", "true")
    .config("spark.sql.autoBroadcastJoinThreshold", 1)
    .config("spark.default.parallelism", 4)
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "536870912")
    .config("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
    .config("spark.streaming.stopSparkContextByDefault", "false")
    .config("spark.debug.maxToStringFields", 50)
    .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
    .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
    .master("local[*]")
    .getOrCreate()



  def withSparkSession[A](f: SparkSession => A): A =
    f(sparkSession.newSession())

  def withSparkContext[A](f: SparkContext => A): A =
    f(sparkSession.newSession().sparkContext)


  override protected def afterAll(): Unit = {
    sparkSession.stop()

  }


  }

