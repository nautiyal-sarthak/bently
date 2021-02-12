package org.cn.ca

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object BentleyIngestion {

  def main(args: Array[String]): Unit = {

    //Input parameters
    val schema = args(0) //"C:\\Users\\193463\\Documents\\cnrail-otds-ingestion\\ingestion\\src\\programs\\bentley-standardized\\src\\main\\resources\\schema.json"//
    val process_date_in = args(1) //"2020082410"//
    val indir = args(2) //"C:\\Users\\193463\\Desktop\\spark_test\\csv"//
    val outdir = args(3) //"C:\\Users\\193463\\Desktop\\spark_test\\dest"//
    val tbl_str = args(4) //"missingInspection"//"missingInspection,remedialAction,inspectionRecord,functionalLocation,trackCompliance,exception"

    val success_status = "_SUCCESS"
    val processed_status = "_PROCESSED"
    val error_status = "_ERROR"

    implicit val spark: SparkSession = SparkSession.builder().appName("BentleyIngestion")
      .config("spark.debug.maxToStringFields", "100")
      .config("spark.default.parallelism", "200")
      .config("spark.network.timeout", "3600s")
      .config("spark.driver.maxResultSize", "0")
      .config("spark.rdd.compress", "true")
      .config("spark.shuffle.spill", "true")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      //.master("local")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val bentleyUtility = BentleyUtility()
    //UserGroupInformation.loginUserFromKeytab("193463@CN.CA", "C:\\Apps\\.193463.keytab")

    import spark.implicits._


    val tbl_lst = tbl_str.split(",")

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val schema_str = scala.io.Source.fromFile(schema).getLines().mkString

    for (tbl <- tbl_lst) {
      val successDir = new Path(indir + "/" + tbl + "/*/" + success_status)
      val successDir_status = fs.globStatus(successDir)
      for (partation <- successDir_status) {
        val path = partation.getPath.toString()
        val partation_str = path.split("/").reverse(1).split("=")(1)

        if (partation_str <= process_date_in) {
          try {

            val process_date_hr = partation_str
            val process_month = process_date_hr.substring(0, 6)
            val inPath = indir + "/" + tbl + "/process_date=" + process_date_hr + "/*.CSV"
            val stanDir = outdir + "/" + tbl
            val invalidDir = outdir + "/failedReport"

            val ingestionHeaderCol = "ingestionheader"
            val failedReasonCol = "failedReason"
            val sourceFileNameCol = "sourceFileLocation"
            val ingestionTimeCol = "ingestionTime"
            val tablenameCol = "tableName"
            val payloadCol = "payload"

            println("Starting the process for " + tbl + " process date " + process_date_hr)

            val in_csv = fs.globStatus(new Path(inPath))
            val stan_csv = fs.globStatus(new Path(stanDir + "/*"))

            if (!in_csv.isEmpty) {
              val schema_obj = JsonUtil.fromJson[Schema](schema_str)
              val metadata = bentleyUtility.gettblDetails(schema_obj, tbl)

              val pk_hive_str = metadata._1
              val colMapping = metadata._3

              if (!pk_hive_str.isEmpty) {

                //Creating the DF from the raw folder
                var df_raw = spark.read
                  .option("quote", "\"")
                  .option("escape", "\"")
                  .option("header", true).csv(inPath)

                println("Primary Key :" + pk_hive_str)
                println("Total number of raw records :" + df_raw.count())

                //removing trailing and leading spaces from col names
                df_raw = df_raw.toDF(df_raw.columns.map(_.trim): _*)

                //removing . in col names
                df_raw = df_raw.toDF(df_raw.columns.map(_.replace(".", "_")): _*)

                //validating the Raw DF and populating the reason col
                var df_raw_validate = df_raw.withColumn(failedReasonCol,
                  bentleyUtility.validatorUDF(struct(df_raw.columns.map(df_raw(_)): _*), lit(tbl), lit(schema_str)))
                  .withColumn(sourceFileNameCol, input_file_name())
                  .withColumn(ingestionTimeCol, unix_timestamp())

                //renaming the src col names to hive col names
                val initialColumnNames = df_raw_validate.columns
                val renamedColumns = initialColumnNames.map {
                  name => {
                    val new_name = colMapping.get(name)
                    val updated_col_name = new_name match {
                      case Some(s) => s
                      case None => name.replace(" ", "_")
                    }
                    col(name).as(updated_col_name)
                  }
                }
                df_raw_validate = df_raw_validate.select(renamedColumns: _*)

                //Spliting the raw df into valid and invalid records
                val condition = col(failedReasonCol) === ""
                var df_valid = df_raw_validate.filter(condition)

                df_valid = df_valid.drop(failedReasonCol)
                  .withColumn(
                    ingestionHeaderCol,
                    struct(
                      col(sourceFileNameCol),
                      col(ingestionTimeCol)
                    )
                  ).drop(sourceFileNameCol).drop(ingestionTimeCol)

                val df_invalid = df_raw_validate.filter(not(condition))
                  .withColumn(
                    ingestionHeaderCol,
                    struct(
                      col(sourceFileNameCol),
                      col(ingestionTimeCol),
                      col(failedReasonCol),
                      lit(tbl).as(tablenameCol)
                    )
                  ).drop(sourceFileNameCol).drop(ingestionTimeCol).drop(failedReasonCol)

                df_valid.persist(StorageLevel.MEMORY_AND_DISK_SER)
                df_invalid.persist(StorageLevel.MEMORY_AND_DISK_SER)

                val cnt_valid = df_valid.count()
                val cnt_invalid = df_invalid.count()

                println("Total number of valid records matching the schema :" + cnt_valid)

                if (cnt_invalid > 0) {
                  println("writing invalid records to hdfs")
                  bentleyUtility.writeToFail(df_invalid, process_month, invalidDir, ingestionHeaderCol, payloadCol)
                  df_invalid.unpersist()
                }

                //Fetch the PK from the schema
                val pk = pk_hive_str.split(",")
                if (cnt_valid > 0) {

                  //enforce the data types and formats to the cols
                  val datatypes = metadata._5
                  val cols = df_valid.columns.map(x => {
                    datatypes.get(x) match {
                      case Some(("string", "")) => col(x).cast(StringType).alias(x)
                      case Some(("double", "")) => col(x).cast(DoubleType).alias(x)
                      case Some(("int", "")) => col(x).cast(IntegerType).alias(x)
                      case Some(("timestamp", format: String)) => col(x).cast(StringType).alias(x)
                      case Some(("decimal", format: String)) => col(x).cast("Decimal(" + format + ")").alias(x)
                      case Some(("boolean", "")) => bentleyUtility.getBoolUDF(col(x)).alias(x)
                      case Some(("date", format: String)) => col(x).cast(StringType).alias(x)
                      case None => col(x)
                    }
                  })

                  println("Sample valid records before transformation")
                  println(df_valid.show(1, false))

                  df_valid = df_valid.select(cols: _*)

                  println("Sample valid records after transformation")
                  println(df_valid.show(1, false))

                  //Concat the PK for raw
                  df_valid = df_valid.withColumn("pk_in", concat_ws("@", pk.map(col): _*))
                  df_valid.persist(StorageLevel.MEMORY_AND_DISK_SER)

                  //removing duplicates from the incomming msgs
                  val byPK = Window.partitionBy("pk_in").orderBy("pk_in")
                  val raw_dup_chk = df_valid.repartition(col("pk_in"))
                    .withColumn("rowCount", row_number().over(byPK))

                  var raw_dup = raw_dup_chk.filter("rowCount != 1")
                    .drop("rowCount")

                  raw_dup.persist(StorageLevel.MEMORY_AND_DISK_SER)

                  val cnt_raw_dup = raw_dup.count()
                  if (cnt_raw_dup > 0) {
                    println("writing duplicate raw records to hdfs")
                    raw_dup = raw_dup
                      .withColumn(
                        ingestionHeaderCol,
                        struct(
                          col(ingestionHeaderCol + "." + sourceFileNameCol),
                          col(ingestionHeaderCol + "." + ingestionTimeCol),
                          lit("Duplicate record failure in raw").as(failedReasonCol),
                          lit(tbl).as(tablenameCol)
                        )
                      )

                    bentleyUtility.writeToFail(raw_dup, process_month, invalidDir, ingestionHeaderCol, payloadCol)
                    raw_dup.unpersist()
                  }

                  df_valid = raw_dup_chk.filter("rowCount == 1").drop("rowCount")

                  if (stan_csv != null && !stan_csv.isEmpty) {
                    //reading te standardied location
                    var df_stan = spark.read.orc(stanDir)
                    val cnt_stan = df_stan.count()
                    println("Total number of records in standardized :" + cnt_stan)

                    //Validating if the PK col are present in the stan
                    val stan_cols = df_stan.columns
                    val stan_pk = pk intersect stan_cols

                    //creating the PK col
                    df_stan = df_stan.withColumn("pk_stan", concat_ws("@", stan_pk.map(col): _*))

                    df_stan.persist(StorageLevel.MEMORY_AND_DISK_SER)

                    //Getting the duplicate records
                    val tempdf = df_stan.select($"pk_stan")
                    df_valid = df_valid.join(tempdf, df_valid("pk_in") === tempdf("pk_stan"), "left")
                    var df_duplicate = df_valid.filter("pk_stan is not null")
                    val duppk = df_duplicate.select($"pk_stan".alias("dupKey"))

                    val emptySchema = StructType(Seq())
                    var df_out = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], emptySchema)

                    val isOverwrite = metadata._6
                    if (isOverwrite) {
                      val stan_filter = df_stan.join(duppk, df_stan("pk_stan") === duppk("dupKey"), "left")
                      df_stan = stan_filter.filter("dupKey is null").drop("dupKey").drop("pk_stan")

                      val collst = df_valid.columns.filter(_ != "ingestionheader")
                      df_valid = df_valid.select("ingestionheader", collst:_*)

                      df_out = df_stan
                        .union(
                          df_valid.drop("pk_in")
                            .drop("pk_stan")
                            .withColumn("process_month", lit(process_month))
                        )

                      df_out.persist(StorageLevel.MEMORY_AND_DISK_SER)
                      if (df_out.count() > 0) {
                        println("writing valid standardized records to hdfs")
                        bentleyUtility.writeToStan(df_out.drop("process_month"), process_month, stanDir, SaveMode.Overwrite)
                        df_stan.unpersist()
                        df_out.unpersist()
                      }
                    }
                    else {
                      val cnt_dup = df_duplicate.count()
                      if (cnt_dup > 0) {
                        println("writing duplicate records to hdfs")
                        df_duplicate = df_duplicate
                          .drop("pk_in")
                          .drop("pk_stan")
                          .withColumn(
                            ingestionHeaderCol,
                            struct(
                              col(ingestionHeaderCol + "." + sourceFileNameCol),
                              col(ingestionHeaderCol + "." + ingestionTimeCol),
                              lit("Duplicate record failure").as(failedReasonCol),
                              lit(tbl).as(tablenameCol)
                            )
                          )

                        bentleyUtility.writeToFail(df_duplicate, process_month, invalidDir, ingestionHeaderCol, payloadCol)
                      }

                      df_valid = df_valid.filter("pk_stan is null")
                      df_valid = df_valid
                        .drop("pk_in")
                        .drop("pk_stan")

                      val cnt_out = df_valid.count()
                      if (cnt_out > 0) {
                        println("writing valid standardized records to hdfs")
                        bentleyUtility.writeToStan(df_valid, process_month, stanDir, SaveMode.Append)
                        df_valid.unpersist()
                      }
                    }
                  } else {
                    if (df_valid.count() > 0) {
                      println("writing valid standardized records to hdfs")
                      bentleyUtility.writeToStan(df_valid, process_month, stanDir, SaveMode.Append)
                      df_valid.unpersist()
                    }
                  }
                }
              }
              else {
                println("Primary key not set for " + tbl + " in the schema ")
              }
            }
            else {
              println("No files to process for " + tbl + " process date " + process_date_hr)
            }

            val processed = path.replace(success_status, processed_status)
            fs.rename(new Path(path), new Path(processed))
          }
          catch {
            case e: Exception => {
              println(e.printStackTrace())
              val error = path.replace(success_status, error_status)
              fs.rename(new Path(path), new Path(error))
              throw e
            }
          }
        }
      }
    }
  }
}