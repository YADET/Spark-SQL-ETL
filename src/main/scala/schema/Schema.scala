package schema

import org.apache.spark.sql.types._


object Schema {

  val surveySchema = StructType(Array(
    StructField("timestamp",TimestampType,true),
    StructField("age",LongType,true),
    StructField("gender",StringType,true),
    StructField("country",StringType,true),
    StructField("state",StringType,true),
    StructField("self_employed",StringType,true),
    StructField("family_history",StringType,true),
    StructField("treatment",StringType,true),
    StructField("work_interfere",StringType,true),
    StructField("no_employees",StringType,true),
    StructField("remote_work",StringType,true),
    StructField("tech_company",StringType,true),
    StructField("benefits",StringType,true),
    StructField("care_options",StringType,true),
    StructField("wellness_program",StringType,true),
    StructField("seek_help",StringType,true),
    StructField("anonymity",StringType,true),
    StructField("leave",StringType,true),
    StructField("mental_health_consequence",StringType,true),
    StructField("phys_health_consequence",StringType,true),
    StructField("coworkers",StringType,true),
    StructField("supervisor",StringType,true),
    StructField("mental_health_interview",StringType,true),
    StructField("phys_health_interview",StringType,true),
    StructField("mental_vs_physical",StringType,true),
    StructField("obs_consequence",StringType,true),
    StructField("comments",StringType,true))
  )

}
