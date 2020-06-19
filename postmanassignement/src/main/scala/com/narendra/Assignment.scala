package com.narendra

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}


object Assignment extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]").set("spark.cores.max", "2")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "DROPMALFORMED")
    .load(Constants.FILE_PATH)

  df.show()


  df.write
    .format("org.apache.spark.sql.cassandra")
    .mode(SaveMode.Overwrite)
    .option("spark.cassandra.connection.host", Constants.CASSANDRA_HOST)
    .option("spark.cassandra.connection.port", Constants.CASSANDRA_PORT)
    .option("spark.cassandra.auth.username", Constants.CASSANDRA_USER)
    .option("spark.cassandra.auth.password", Constants.CASSANDRA_PASS)
    .option("keyspace", Constants.CASSANDRA_DB)
    .option("table", Constants.CASSANDRA_TABLE)
    .save()

  println("written successfully !!! ")
  spark.close()


}
