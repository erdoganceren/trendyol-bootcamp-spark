package com.trendyol.bootcamp.homework

object ProductMergerJob {

  def main(args: Array[String]): Unit = {

    /**
    * Bu problem şuan Batch olarak yapılıyor. 
    * Key,value şeklinde bir datam var. product owner; name'i değiştiriyor, kategorisini değiştirebiliyor. Ben en son halini istiyorum.
    * Datalarım var elimde 10 satır. Yeni gelenler de var 15 tane. Ortak olmayanlarını target datasına yazıyorum. Ortak olanlarda (yeni gelenler birden fazlaysa timestamp'e göre)
    * son hallerini alıyorum. id'ye göre gruplanarak yapılır. 
    * Bu bir merger'dır aslında.
    * Find the latest version of each product in every run, and save it as snapshot.
    *
    * Product data stored under the data/homework folder.
    * Read data/homework/initial_data.json for the first run.
    * Read data/homework/cdc_data.json for the nex runs.
    *
    * Save results as json, parquet or etc.
    *
    * Note: You can use SQL, dataframe or dataset APIs, but type safe implementation is recommended.
    */

  }

}
