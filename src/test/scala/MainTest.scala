import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite


class MainTest extends AnyFunSuite with Matchers {
  test("gross") {
    val spark:SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark Trendyol Test Case")
      .getOrCreate()

    import spark.implicits._

    val inputDF = Seq(
      ("1", "Istanbul", "2", "2021-01-21T12:24:08.000+03:00", "1", 2.0, "1", "Created"),
      ("1", "Ankara", "2",  "2021-01-22T12:24:08.000+03:00", "1", 2.0, "1", "Created"),
      ("1", "Istanbul", "2", "2021-01-23T12:24:08.000+03:00", "1", 2.0, "1", "Created"),
      ("1", "Izmir", "2", "2021-01-22T12:24:08.000+03:00", "1", 2.0, "2", "Cancelled"),
      ("1", "Trabzon", "2",  "2021-01-22T12:24:08.000+03:00", "1", 2.0, "2", "Created"))
      .toDF("customer_id", "location", "seller_id", "order_date", "order_id", "fiyat", "product_id", "status")

    val result_gross = Main.calculate_gross_stats(inputDF, spark)

    result_gross.collect() shouldBe Array()
  }
  test("net") {
    val spark:SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark Trendyol Test Case")
      .getOrCreate()

    import spark.implicits._

    val inputDF = Seq(
      ("1", "Istanbul", "2", "2021-01-21T12:24:08.000+03:00", "1", 2.0, "1", "Created"),
      ("1", "Ankara", "2",  "2021-01-22T12:24:08.000+03:00", "1", 2.0, "1", "Created"),
      ("1", "Istanbul", "2", "2021-01-23T12:24:08.000+03:00", "1", 2.0, "1", "Created"),
      ("1", "Izmir", "2", "2021-01-22T12:24:08.000+03:00", "1", 2.0, "2", "Cancelled"),
      ("1", "Trabzon", "2",  "2021-01-22T12:24:08.000+03:00", "1", 2.0, "2", "Created"))
      .toDF("customer_id", "location", "seller_id", "order_date", "order_id", "price", "product_id", "status")

    val fakeDF = Seq(
      ("fake1", "fake2", "fake3"),
      ("fake1", "fake2", "fake3"))
      .toDF("fake_col1", "fake_col2", "fake_col3")

    val result = Seq(
      ("1"),
      ("2"))
      .toDF("product_id")

    // when result df is correct type
    val result_net_correct = Main.calculate_net_stats(inputDF,result, spark)

    // when result df is null
    val result_net_null = Main.calculate_net_stats(inputDF,null, spark)

    // when result df is wrong type
    val result_net_wrong = Main.calculate_net_stats(inputDF,fakeDF, spark)

    val asArray_correct = result_net_correct.collect()
    asArray_correct(1).getAs[Double]("net_sales_amount") shouldBe 0
    asArray_correct(1).getAs[Double]("net_sales_price") shouldBe 0

    val asArray_null = result_net_null.collect()
    asArray_null(1).getAs[Double]("net_sales_amount") shouldBe 0
    asArray_null(1).getAs[Double]("net_sales_price") shouldBe 0

    val asArray_wrong = result_net_wrong.collect()
    asArray_wrong(1).getAs[Double]("net_sales_amount") shouldBe 0
    asArray_wrong(1).getAs[Double]("net_sales_price") shouldBe 0
  }
  test("avgSales_location") {
    val spark:SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark Trendyol Test Case")
      .getOrCreate()

    import spark.implicits._

    val inputDF = Seq(
      ("1", "Istanbul", "2", "2021-01-21T12:24:08.000+03:00", "1", 2.0, "1", "Created"),
      ("1", "Ankara", "2",  "2021-01-22T12:24:08.000+03:00", "1", 2.0, "1", "Created"),
      ("1", "Istanbul", "2", "2021-01-23T12:24:08.000+03:00", "1", 2.0, "1", "Created"),
      ("1", "Trabzon", "2",  "2021-01-22T12:24:08.000+03:00", "1", 2.0, "2", "Created"),
      ("1", "Trabzon", "2",  "2021-01-22T12:24:08.000+03:00", "1", 2.0, "2", "Created"))
      .toDF("customer_id", "location", "seller_id", "order_date", "order_id", "price", "product_id", "status")
      .withColumn("order_date", to_timestamp(col("order_date"))).orderBy(col("order_date").desc)


    val fakeDF = Seq(
      ("fake1", "fake2", "fake3"),
      ("fake1", "fake2", "fake3"))
      .toDF("fake_col1", "fake_col2", "fake_col3")

    val result = Seq(
      ("1","null"),
      ("2","null"))
      .toDF("product_id","null")

    // when result df is correct type
    val result_location_correct = Main.calculate_highest_location(inputDF,result, spark)

    // when result df is correct type
    val result_avg_correct = Main.calculate_avg_sales(inputDF,result, spark)

    // when result df is null
    val result_location_null = Main.calculate_highest_location(inputDF,null, spark)

    // when result df is null
    val result_avg_null = Main.calculate_avg_sales(inputDF,null, spark)

    // when result df is wrong type
    val result_location_wrong = Main.calculate_highest_location(inputDF,fakeDF, spark)

    // when result df is wrong type
    val result_avg_wrong = Main.calculate_avg_sales(inputDF,fakeDF, spark)

    val asArray_location_correct = result_location_correct.collect()
    asArray_location_correct(0).getAs[String]("top_selling_location") shouldBe "Istanbul"
    asArray_location_correct(1).getAs[String]("top_selling_location") shouldBe "Trabzon"

    val asArray_avg_correct = result_avg_correct.collect()
    asArray_avg_correct(0).getAs[Double]("average_sales_amount_of_last_5_selling_days") shouldBe 1.0
    asArray_avg_correct(1).getAs[Double]("average_sales_amount_of_last_5_selling_days") shouldBe 2.0

    result_location_null.collect() shouldBe Array()
    result_avg_null.collect() shouldBe Array()

    result_location_wrong.collect() shouldBe Array()
    result_avg_wrong.collect() shouldBe Array()
  }
  test("price_change"){
    val spark:SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark Trendyol Test Case")
      .getOrCreate()

    import spark.implicits._

    val inputDF = Seq(
      ("1", "Istanbul", "2", "2021-01-21T12:24:08.000+03:00", "1", 2.0, "1", "Created"),
      ("1", "Ankara", "2",  "2021-01-22T12:24:08.000+03:00", "1", 3.0, "1", "Created"),
      ("1", "Istanbul", "2", "2021-01-23T12:24:08.000+03:00", "1", 1.0, "1", "Created"),
      ("1", "Trabzon", "2",  "2021-01-22T12:24:08.000+03:00", "1", 2.0, "2", "Created"),
      ("1", "Trabzon", "2",  "2021-01-22T12:25:08.000+03:00", "1", 2.0, "2", "Created"),
      ("1", "Trabzon", "2",  "2021-01-22T12:26:08.000+03:00", "1", 30.0, "2", "Created"))
      .toDF("customer_id", "location", "seller_id", "order_date", "order_id", "price", "product_id", "status")
      .withColumn("order_date", to_timestamp(col("order_date"))).orderBy(col("order_date").desc)


    val fakeDF = Seq(
      ("fake1", "fake2", "fake3"),
      ("fake1", "fake2", "fake3"))
      .toDF("fake_col1", "fake_col2", "fake_col3")


    // when result df is correct type
    val result_change_correct = Main.calculate_price_change(inputDF, spark)

    // when result df is null
    val result_change_null = Main.calculate_price_change(null,spark)

    // when result df is wrong type
    val result_change_wrong = Main.calculate_price_change(fakeDF, spark)

    // If the product is firstly sold, then it is ignored because lag price is null.
    // If the lag price is bigger than current price, it means change is marked as fall
    // If the lag price is smaller than current price, it means change is marked as rise.
    // If the lag price is equal to the current price, it is also ignored.
    // All the cases are covered in the inputDF
    val asArray_change_correct = result_change_correct.sort("product_id","order_date").collect()
    asArray_change_correct(0).getAs[String]("product_id") shouldBe "1"
    asArray_change_correct(0).getAs[String]("change") shouldBe "rise"
    asArray_change_correct(1).getAs[String]("product_id") shouldBe "1"
    asArray_change_correct(1).getAs[String]("change") shouldBe "fall"
    asArray_change_correct(2).getAs[String]("product_id") shouldBe "2"
    asArray_change_correct(2).getAs[String]("change") shouldBe "rise"


    result_change_null.collect() shouldBe Array()
    result_change_wrong.collect() shouldBe Array()
  }
}
