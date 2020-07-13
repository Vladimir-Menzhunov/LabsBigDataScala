package ru.philit.bigdata.vsu.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.{Order, Product}

object Lab5GroupBy extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val products: RDD[(Int, String)] = sc.textFile(Parameters.path_product)
    .map(str => Product(str)).map {
    case Product(id, name, _, _) => (id, name)
  }

  val orders: RDD[(Int,(Int,Int,Double))] = sc.textFile(Parameters.path_order)
    .map(str => Order(str)).filter(_.status.equals("delivered")).map {
    case Order(customerID, _, _, numberOfProduct, _, _) => (customerID, numberOfProduct)
  }.groupBy {
    case (customerID, _) => customerID
  }.map {
    case (key, group) => (key, (group.map(_._2).sum, group.size, group.map(_._2).sum/group.size))
  }

  orders.map {
    case (key, (sum, size, avg)) => s"CustomerID: $key - SumPrice: $sum - Size: $size - Average: $avg"
  }.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab5")

}
