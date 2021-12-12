package main.scala.countcolors

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CountColors {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("CountColors")
      .getOrCreate()

    if (args.length < 1) {
      print("Usage: CountColors <path to dataset>")
      sys.exit(1)
    }

    val path = args(0)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)

    val countColors = df
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    countColors.show(60)
    println(s"Total Rows = ${countColors.count()}")
    println()

    val caCount = df
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    caCount.show(10)
    spark.stop()
  }
}