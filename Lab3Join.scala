package ru.philit.bigdata.vsu.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.{Customer, Order, Product}
import java.sql.Date

object Lab3Join extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val orders: RDD[(Int,(Date, Int, Int) )] = sc.textFile(Parameters.path_order)
    .map(str => Order(str)).filter(_.status.equals("delivered")).map {
    case Order(customerID, _, productID, numberOfProduct, orderDate, _) => (customerID,(orderDate, numberOfProduct,productID))
  }

  //orders.foreach(x => println(x))

  val customers: RDD[(Int, String)] = sc.textFile(Parameters.path_customer)
    .map(str => Customer(str)).filter(_.status.equals("active")).map {
    case Customer(id, name, _, _, _) => (id, name)
  }

  //customers.foreach(x => println(x))

  val orderJoin = orders.join(customers).map {
    case (_, ((orderDate, numberOfProduct, productID), name)) => (productID,(name, orderDate, numberOfProduct))
  }

  val products: RDD[(Int, Double)] = sc.textFile(Parameters.path_product)
    .map(str => Product(str)).map {
    case Product(id, _, price, _) => (id, price)
  }

  def aggregateCustomerAndDate(acc: Double, record: Double): Double = acc match {
    case price => price + record
  }

  orderJoin.join(products).map {
    case (productID, ((customerName, orderDate, numberOfProduct),price)) =>
      ((customerName, orderDate), numberOfProduct * price)
  }.reduceByKey(aggregateCustomerAndDate).map {
    case ((customerName, orderDate), spent) => s"Customer: $customerName - Order Date: $orderDate - Spent: $spent"
  }.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab3")

}
