package ru.philit.bigdata.vsu.spark.exercise




import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.Parameters
import ru.philit.bigdata.vsu.spark.exercise.domain.Order

object Lab1ReduceByKey  extends  App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  def aggregateOrders(acc: (Int, Int), record: (Int, Int)): (Int, Int) = acc match {
    case (numberOfProduct, count) => (numberOfProduct + record._1, count + 1)
  }


  val orders: RDD[Order] = sc.textFile(Parameters.path_order)
    .map(str => Order(str)).filter(_.status.equals("delivered")) // apply - помогает выполнить разбиение

  orders.map {
    case Order(costumerID, _,_, numberOfProduct,_,_) => (costumerID,(numberOfProduct, 1))
  }.reduceByKey(aggregateOrders).map {
    case (costID, (numberOfProduct, count)) => s"$costID - Number of Product: $numberOfProduct; count: $count"
  }.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab1")

 // orders.foreach(x => println(x))



  sc.stop()
}