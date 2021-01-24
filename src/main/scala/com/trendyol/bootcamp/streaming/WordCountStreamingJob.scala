package com.trendyol.bootcamp.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object WordCountStreamingJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("sss - reformat input data job")
      //.config() = partition sayısını girmek için. 
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val inputLines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

//    WATERMARK, WINDOWDURATION 10 sn VERMİŞTİK 10 sn GEÇİNCE 5 sn BEKLEYİP (late even tler gelebilir.) SİLİYOR STATE'İ.
//    val transformedLines = inputLines
//      .as[String]
//      .filter(_ != "")
//      .flatMap(_.split(" "))
//      .withWatermark("timestamp","5 second")
//      .groupBy(window($"timestamp",windowDuration = "10 seconds"),$"word")
//      .count()

    val transformedLines = inputLines
      .as[String]
      .filter(_ != "")
      .flatMap(_.split(" "))
      .map(word => WordWithCount(word, 1))
      //groupByKey yaparken state sonsuza kadar duruyor.
      .groupByKey(_.word)
      .reduceGroups { (first, second) =>
        val updatedCount = first.count + second.count
        WordWithCount(first.word, updatedCount)
      }
      .map { case (_, value) => value }

    val query = transformedLines.writeStream
      .outputMode("update")//En son çıktı verdiğim şeyi yazar. //complete modda olsaydı tüm çıktıları yazardı.
      .format("console")
      //.trigger(Trigger.ProcessingTime(5000))
      .start()

    query.awaitTermination()
  }

}

case class WordWithCount(word: String, count: Long)
