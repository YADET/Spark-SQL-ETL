package utiltest

import org.scalatest.time._
import utils.Utils
import commons._
class MethodTests extends TestHelper{

  it should "the parse correct genders" in withSparkSession  { spark =>

    import spark.implicits._

    val df =spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("nullValue","NA")
      .option("timestampFormat","yyyy-MM-dd'T'HH:mm​:ss")
      .option("mode","failfast")
      .option("path",cfg.getConfig("sampledata").getString("sd1"))
      .load()

    eventually (timeout(Span(1, Seconds))) {
      val t1= Utils.parseGender("woman")
      t1 should be ("Female")
    }

  }


}
