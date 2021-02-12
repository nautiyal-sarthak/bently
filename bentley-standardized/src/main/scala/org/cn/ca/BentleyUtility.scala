package org.cn.ca

import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp

import org.apache.spark.sql.functions.{col, lit, map}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks.{break, breakable}
import scala.util.control.Exception.allCatch

case class BentleyUtility()(implicit spark: SparkSession){
  def validator = (r: Row, tbl: String, schema: String) => {
    var msg = ""

    try {
      val schema_obj = JsonUtil.fromJson[Schema](schema)
      val tbl_metadata = gettblDetails(schema_obj, tbl)

      val tbl_pk = tbl_metadata._4.split(",")
      val tbl_schema: Map[String, (String, String)] = tbl_metadata._2

      if (tbl_schema.count(z => true) != r.length) {
        throw new Exception("Mismatch in the count of cols")
      }

      breakable {
        //validate Schema
        val incommingSchema: StructType = r.schema
        for (elem <- incommingSchema) {
          if (tbl_schema.contains(elem.name)) {

            if (tbl_pk.contains(elem.name)) {
              if (!Option(r.getAs(elem.name)).isDefined) {
                msg = "PK is null : " + elem.name
                break
              }
            }

            val col_type = tbl_schema.get(elem.name)
            if (Option(r.getAs(elem.name)).isDefined){
              msg = col_type match {
                case Some(("int", "")) => if (isNumber(r.getAs(elem.name))) "" else "CSV template failure:Data type mismatch for " + elem.name + " expected int got " + r.getAs(elem.name)
                case Some(("string", "")) => ""
                case Some(("boolean","")) => {
                  val bool = List("true","false")
                  if (!bool.contains(r.getAs(elem.name).toString.toLowerCase)){
                    "CSV template failure:Data type mismatch for " + elem.name + " expected boolean got " + r.getAs(elem.name)
                  }
                  else ""
                }
                case Some(("double", "")) => if (isDoubleNumber(r.getAs(elem.name))) "" else "CSV template failure:Data type mismatch for " + elem.name + " expected double got " + r.getAs(elem.name)
                case Some(("timestamp", format: String)) => {
                  if(!format.isEmpty) {
                    if (isTimeStamp(r.getAs(elem.name), format)) "" else "CSV template failure:Data type mismatch for " + elem.name + " expected timestamp " + format + " got " + r.getAs(elem.name)
                  }
                  else
                    {
                      "format is not set in the schema for " + elem.name
                    }
                  }
                case Some(("date", format: String)) => {
                  if(!format.isEmpty) {
                    if (isTimeStamp(r.getAs(elem.name), format)) "" else "CSV template failure:Data type mismatch for " + elem.name + " expected date " + format + " got " + r.getAs(elem.name)
                  }
                  else
                  {
                    "format is not set in the schema for " + elem.name
                  }
                }
                case Some(("decimal", format)) => {
                  if(!format.isEmpty) {
                    if (isDoubleNumber(r.getAs(elem.name))) "" else "CSV template failure:Data type mismatch for " + elem.name + " expected decimal got " + r.getAs(elem.name)
                  }
                  else
                    {
                      "format is not set in the schema for " + elem.name
                    }
                }
                case _ => {
                  "CSV template failure:datatype " + col_type + " not found for : " + elem.name + " in the schema provided"
                }
              }
            }

            if (msg != "") break()
          }
          else {
            msg = "CSV template failure:Col not in schema : " + elem.name
            break
          }
        }
      }
    }
    catch {
      case e: Exception => msg = e.getMessage
    }

    msg
  }

  def isLongNumber(s: String): Boolean = (allCatch opt s.toLong).isDefined

  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

  def isNumber(s: String): Boolean = (allCatch opt s.toInt).isDefined

  def isTimeStamp(timeStamp: String, format: String): Boolean = {
    val formatter = new SimpleDateFormat(format)
    val test = Try[Date](formatter.parse(timeStamp))

    test match {
      case Success(date) => true // ok
      case Failure(exception) => false // not ok
    }
  }

  @throws(classOf[Exception])
  def gettblDetails(schema: Schema, tbl: String) = {
    var table_selected: Table = null
    val pk = scala.collection.mutable.MutableList[String]()
    var datatypes: Map[String, (String, String)] = Map[String, (String, String)]()
    var hive_datatypes: Map[String, (String, String)] = Map[String, (String, String)]()
    var colNames: Map[String, String] = Map[String, String]()
    val pk_before = scala.collection.mutable.MutableList[String]()
    val isOverwrite : Boolean = false

    breakable(
      schema.tables.foreach(tblobj => {
        if (tblobj.name == tbl) {
          table_selected = tblobj
          break
        }
      }))

    if (table_selected != null) {
      for (elem <- table_selected.cols) {
        if (elem.isPK) {
          pk += elem.hiveName
          pk_before += elem.name
        }
        datatypes += (elem.name -> {
          if (List("timestamp","decimal","date").contains(elem.colType)) (elem.colType, elem.format) else (elem.colType, "")
        })
        colNames += (elem.name -> elem.hiveName)
        hive_datatypes += (elem.hiveName -> {
          if (List("timestamp","decimal","date").contains(elem.colType)) (elem.colType, elem.format) else (elem.colType, "")
        })
      }
    } else {
      throw new Exception(tbl + " not found in the schema")
    }

    //1. Primary Key hive
    //2. Src Col name datatype mapping
    //3. src --> dest col name mapping
    //4. Primary Key src
    //5. Hive Col name datatype mapping
    //6. Is Overwrite
    (pk.mkString(","), datatypes, colNames, pk_before.mkString(","), hive_datatypes,table_selected.isOwerwrite)
  }

  val getTimestampWithMilis: ((String , String) => Option[Timestamp]) = (input, frmt) => input match {
    case "" => None
    case _ => {
      val format = new SimpleDateFormat(frmt)
      Try(new Timestamp(format.parse(input).getTime)) match {
        case Success(t) => Some(t)
        case Failure(_) => None
      }
    }
  }

  val getBool: (String => Boolean) = input => {
    val input_formate: String = input.toLowerCase()
    input_formate match {
      case "true" => true
      case "false" => false
    }
  }

  val validatorUDF = spark.udf.register("validator", validator)
  val getTimestampWithMilisUDF = spark.udf.register("getTimestampWithMilis",getTimestampWithMilis)
  val getBoolUDF = spark.udf.register("getBool",getBool)

  def writeToStan(data:DataFrame,process_month:String,stanDir:String,mode:SaveMode)={
    println("Total number of records written to stan :" + data.count())

    val collst = data.columns.filter(_ != "ingestionheader")
    val formated_data = data.select("ingestionheader", collst:_*)

    println(formated_data.show(1, false))

    formated_data
      .drop("pk_in")
      .withColumn("process_month", lit(process_month))
      .coalesce(1)
      .write
      .partitionBy("process_month")
      .option("header", "true")
      .option("sep", ",")
      .mode(mode)
      .orc(stanDir)
  }

  def writeToFail(data:DataFrame,process_month:String,invalidDir:String,ingestionHeaderCol:String,payloadCol:String)={
    println("Total number of invalid records :" + data.count())
    //changing the scema of the invalid table to hold only two cols , ingestionHeader and the payload
    val kvCols = data.columns.filter(_ != ingestionHeaderCol).flatMap(c => Seq(lit(c), col(c).cast(StringType)))
    val df_invalid = data.withColumn(payloadCol, map(kvCols: _*)).select(col(ingestionHeaderCol), col(payloadCol))

    println(df_invalid.show(1, false))

    df_invalid
      .withColumn("process_month", lit(process_month))
      .coalesce(1)
      .write
      .partitionBy("process_month")
      .mode(SaveMode.Append)
      .orc(invalidDir)
  }
}
