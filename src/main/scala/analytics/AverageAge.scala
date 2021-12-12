package main.scala.analytics

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession

object AverageAge {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession
        .builder
        .appName("AuthorsAges")
        .getOrCreate()

      val df = spark.createDataFrame(Seq(
        ("Brooke", 20),
        ("Brooke", 25),
        ("Denny", 31),
        ("Jules", 30),
        ("TD", 35))
      ).toDF("name", "age")

      val avgDF = df.groupBy("name").agg(avg("age"))

      avgDF.show()
    }
}