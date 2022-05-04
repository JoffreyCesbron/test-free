package org.free2move

import org.apache.spark.sql.functions.col
import org.free2move.common.SparkJob
import org.free2move.processing.Analytics.{computeBasicStats, getRepeaters, getStatsForAPeriod}
import org.free2move.processing.Processing.getJoinedDf


object Main extends SparkJob {

  println("starting application")

  val df = getJoinedDf()
  println(s"joining operations done, size of the df is <${df.count()}>")
  val dfFiltered = getStatsForAPeriod(df, "2000-10-02", "2020-10-02")
  println(s"size of the df filtered is <${dfFiltered.count()}>")
  val dfAgg = computeBasicStats(dfFiltered)
  println(s"stats have been computed")
  val dfRepeaters = getRepeaters(dfFiltered)
  println(s"repeaters have been retrieved")

  println("ending application")

  //##### Answers exerice
  println(s"top 10 customers")
  dfAgg.show(10, false)

  println(s"number of repeaters ${dfRepeaters.filter(col("count_purchase_different_dates") >= 2)}")

  //Optionnal
  //Lancer un coordinator quotidien qui va lancer un workflow avec une spark action
  //Créer un cluster avec Terraform et lancer l'appli dessus
  //mettre les logs dans ES puis utiliser Kibana
  //Spark streaming pour regarder en temps réel


  //TODO
  //aller chercher les chemins à partir de getResource
  //les tests unitaire avec DataFrameSuiteBase.scala notamment
  //problème de doublons
  //test sur la date rentrée en paramètre
  //mettre en place un logger



}
