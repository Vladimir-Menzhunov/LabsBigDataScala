package ru.philit.bigdata.vsu.spark.exercise


import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.{Order, Product}
import ru.philit.bigdata.vsu.spark.exercise.Parameters


object Lab2Join  extends  App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  def aggregateSellProduct(acc: Int, record: Int): Int = acc match {
    case numberOfProduct => numberOfProduct + record
  }

  val products: RDD[(Int,String)] = sc.textFile(Parameters.path_product)
    .map(str => Product(str)).map {
      case Product(id,name,_,_) => (id, name)
    }

  val orders: RDD[(Int,Int)] = sc.textFile(Parameters.path_order)
    .map(str => Order(str)).filter(_.status.equals("delivered")).map {
      case Order(_, _, productID, numberOfProduct, _, _) => (productID, numberOfProduct)
    }

  orders.reduceByKey(aggregateSellProduct).foreach(x => println(x))

  products.map {
    case product@(id, _) => (id, product)
  }.leftOuterJoin(orders.map {
    case order@(productID, _) => (productID ,order)
  }).filter(_._2._2.isEmpty).map {
    case (_, ((productID, productName), _)) => s"Product ID - $productID - Product Name: $productName; sell - 0"
  }.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab2")








}