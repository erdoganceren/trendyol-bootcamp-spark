package com.trendyol.bootcamp.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object TransformLinesStreamingJob {
//Yapılan işlem gelen datayı farklı bir formata dönüştürmek. O yüzden stateless stream processing
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("sss - reformat input data job")
      .getOrCreate()

    import spark.implicits._
    
    //spark.readStream.format("soket").load("/path")

    val inputLines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val transformedLines = inputLines.as[String]
      .filter(_ != "")
      .map(convertToLineDetail)

    val query = transformedLines.writeStream
      .outputMode("update")
      .format("console")//Test için console yapıyoruz.
      .trigger(Trigger.ProcessingTime(5000))//Ne zamanda bir tetiklenecek. Her 5 saniyede bir soket'i kontrol et. 
                                            //Bu micro-batch olarak yapılıyor.(ProcessingTime yerine Continuous yaz) 
      .start()

    query.awaitTermination()
  }

  def convertToLineDetail(line: String): LineDetail = {
    val totalWordCount = line.split(" ").length
    LineDetail(line, totalWordCount)
  }

}

case class LineDetail(inputLine: String, totalWordCount: Int, receivedTimestamp: Long = System.currentTimeMillis())
