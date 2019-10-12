package job

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import utils.Utils._
import schema.Schema._



class Job extends SessionStarter {

  val logger = Logger.getLogger(getClass.getName)

    def taskone() {


      import sparkSession.implicits._

      ////////////////////////////////////////
      //ETL using dataset API //
      ///////////////////////////////////////

      val df3 = sparkSession.read
        .format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .option("nullValue","NA")
        .option("timestampFormat","yyyy-MM-dd'T'HH:mm​:ss")
        .option("mode","failfast")
        .option("path","./src/main/resources/data/survey.csv")
        .load()
      df3.show
      df3.schema
      val df4 = df3.select($"Gender",
        (when($"treatment" === "Yes", 1).otherwise(0)).alias("All-Yes"),
        (when($"treatment" === "No", 1).otherwise(0)).alias("All-Nos")
      )

      val parseGenderUDF = udf(parseGender _)
      val df5 = df4.select((parseGenderUDF($"Gender")).alias("Gender"),
        $"All-Yes",
        $"All-Nos"
      )
      df5.show()
      val df6 = df5.groupBy("Gender").agg( sum($"All-Yes"),sum($"All-Nos"))
      val df7 = df6.filter($"Gender" =!= "Transgender")
      df6.show()
      df7.show()

      ////////////////////////////////////////
      //ETL using SQL //
      ///////////////////////////////////////

      val df8 = sparkSession.read
        .format("csv")
        .schema(surveySchema)
        .option("header","true")
        .option("nullValue","NA")
        .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
        .option("mode","failfast")
        .load("./src/main/resources/data/survey.csv")

      df8.createOrReplaceTempView("survey_tbl")
      sparkSession.sql("""select gender, sum(yes), sum(no)

            from (select case when lower(trim(gender)) in ('male','m','male-ish','maile','mal',
                                                           'male (cis)','make','male ','man','msle',
                                                           'mail','malr','cis man','cis male')
                              then 'Male'
                              when lower(trim(gender)) in ('cis female','f','female','woman',
                                                           'female','female ','cis-female/femme',
                                                           'female (cis)','femail')
                              then 'Female'
                              else 'Transgender'
                              end as gender,
                              case when treatment == 'Yes' then 1 else 0 end as yes,
                              case when treatment == 'No' then 1 else 0 end as no
                  from survey_tbl)

         where gender != 'Transgender'
         group by gender""").show


      ////////////////////////////////////////
      //Read avro //
      ///////////////////////////////////////
      val avrodf = sparkSession.read
        .format("avro")
        .option("mode","failfast")
//        .option("mode","permissive")
//        .option("mode","failFast")
        .load("./src/main/resources/data/tweet.avro")
      avrodf.show()

      ////////////////////////////////////////
      //Read xml //
      ///////////////////////////////////////
      val xmldf = sparkSession.read
        .format("com.databricks.spark.xml")
        .option("mode","failfast")
        .load("./src/main/resources/data/note.xml")

      xmldf.show()

      ///////////////////////
      ///// txt ////////////
      /////////////////////

      val txtdf = sparkSession.read.textFile("./src/main/resources/data/wordcount.txt")
      val a=txtdf.count()
      println(txtdf.first())
      println(txtdf.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b))
      println(txtdf.filter(line => line.contains("is")).count())
      val wordCounts = txtdf.flatMap(line => line.split(" ")).groupByKey(identity).count().agg(max($"count(1)"))
      wordCounts.show()

    }

}
