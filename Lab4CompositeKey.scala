package ru.philit.bigdata.vsu.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.{Customer, Order, Product}

object Lab4CompositeKey extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val orders: RDD[((Int,Int), Int)] = sc.textFile(Parameters.path_order)
    .map(str => Order(str)).filter(_.status.equals("delivered"))
    .map {
      case Order(customerID, _, productID, numberOfProduct, _, _) => ((customerID, productID), numberOfProduct)
    }

  val products: RDD[Product] = sc.textFile(Parameters.path_product)
    .map(str => Product(str))

  val customers: RDD[Customer] = sc.textFile(Parameters.path_customer)
    .map(str => Customer(str)).filter(_.status.equals("active"))

  def aggregateCustomerAndProduct(acc: Double, record: Double): Double = acc match {
    case spend => spend + record
  }

  products.cartesian(customers).map {
    case (Product(productID, productName, price, _),Customer(customerID, customerName, _, _, _)) =>
      ((customerID, productID),(customerName, productName, price))
  }.join(orders).map {
    case ((_, _),((customerName, productName, price), numberOfProduct)) =>
      ((customerName, productName), price * numberOfProduct)
  }.reduceByKey(aggregateCustomerAndProduct).map {
    case ((customerName, productName), spend) => s"Customer: $customerName - Product: $productName - Spend: $spend"
  }.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab4")




}
