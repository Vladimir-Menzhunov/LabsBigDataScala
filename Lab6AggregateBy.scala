package ru.philit.bigdata.vsu.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.Order

object Lab6AggregateBy extends App{
  /*
   * Lab6 - пример использования aggregateByKey
   * Определить кол-во уникальных заказов, максимальный объем заказа,
   * минимальный объем заказа, общий объем заказа за всё время
   * Итоговое множество содержит поля: order.customerID, count(distinct order.productID),
   * max(order.numberOfProduct), min(order.numberOfProduct), sum(order.numberOfProduct)
   *
   * 1. Создать экземпляр класса SparkConf +
   * 2. Установить мастера на local[*] и установить имя приложения +
   * 3. Создать экземпляр класса SparkContext, используя объект SparkConf +
   * 4. Загрузить в RDD файл src/test/resources/input/order +
   * 5. Используя класс [[ru.phil_it.bigdata.entity.Order]], распарсить строки в RDD +
   * 6. Выбрать только те транзакции у которых статус delivered +
   * 7. Выбрать ключ (customerID), значение (productID, numberOfProducts) +
   * 8. Создать кейс класс Result(uniqProducts: Set[Int], uniqNumOfProducts: Seq[Int], sumNumOfProduct: Int)
   * 9. Создать аккумулятор с начальным значением
   * 10. Создать ананимную функцию для заполнения аккумульятора
   * 11. Создать ананимную функцию для слияния аккумуляторов
   * 12. Выбрать id заказчика, размер коллекции uniqProducts,
   *   максимальное и минимальное значение из uniqNumOfProducts и sumNumOfProduct
   * 13. Вывести результат или записать в директорию src/test/resources/output/lab6
   * */

  case class Result(uniqProducts: Set[Int], uniqNumOfProducts: Seq[Int], sumNumOfProduct: Int)

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val orders: RDD[(Int, (Int, Int))] = sc.textFile(Parameters.path_order)
    .map(str => Order(str)).filter(_.status.equals("delivered")).map {
    case Order(customerID, _, productID, numberOfProduct, _, _) => (customerID, (productID,numberOfProduct))
  }

  orders.aggregateByKey(Result(Set[Int](),Seq[Int](),0))(
    (acc, value) => acc.copy(
      uniqProducts = acc.uniqProducts.union(Set(value._1)),
      uniqNumOfProducts = acc.uniqNumOfProducts.union(Seq(value._2)),
      sumNumOfProduct = acc.sumNumOfProduct + value._2),
    (acc1, acc2) => acc1.copy(
      uniqProducts = acc1.uniqProducts.union(acc2.uniqProducts),
      uniqNumOfProducts = acc1.uniqNumOfProducts.union(acc2.uniqNumOfProducts),
      sumNumOfProduct = acc1.sumNumOfProduct + acc2.sumNumOfProduct
    )
  ).map {
    case (customerID, Result(uniqProducts, uniqNumOfProducts, sumNumOfProduct)) =>
      (customerID, uniqProducts.size, uniqNumOfProducts.max, uniqNumOfProducts.min, sumNumOfProduct)
  }.map {
    case (customerID, uniqProducts, numOfProductsMax, numOfProductsMin, sumNumOfProduct) =>
    s"CustomerID: $customerID, Number unique product: $uniqProducts," +
      s" Max buy Product: $numOfProductsMax, Min buy Product: $numOfProductsMin, All buy: $sumNumOfProduct"
  }.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab6")


}
