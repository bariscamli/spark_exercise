import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Try

object Main {
    def main(args: Array[String]): Unit = {
        val spark:SparkSession = SparkSession
          .builder()
          .master("local")
          .appName("Spark Case")
          .getOrCreate()

        import spark.implicits._


        var df:DataFrame = Try(
            spark.read
              .json("./raw-data/orders.json")
        ).getOrElse(spark.emptyDataFrame)

        // ------------------- Problem 1 -------------------

        df = df.withColumn("order_date", to_timestamp(col("order_date"))).orderBy(col("order_date").desc)

        // Orders with status as Created will be used more than once so it is cached
        val df_created_orders = df.filter(df("status") === "Created").cache()


        var result = calculate_gross_stats(df_created_orders,spark)
        result = calculate_net_stats(df,result,spark)
        result = calculate_avg_sales(df_created_orders,result,spark)
        result = calculate_highest_location(df_created_orders,result,spark)
        result.orderBy(col("product_id").desc).write.mode(SaveMode.Overwrite).json("./results/problem_one.json")

        val result_two = calculate_price_change(df_created_orders,spark)
        df_created_orders.unpersist()
        result_two.write.mode(SaveMode.Overwrite).json("./results/problem_two.json")
    }

    def calculate_gross_stats(dataFrame: DataFrame,spark: SparkSession):DataFrame ={
        if(dataFrame.columns.contains("product_id") && dataFrame.columns.contains("price")){

            // --------- Problem 1-b:
            // Extraction of gross sales amount and price. This is also result dataframe so following columns will be added to this dataframe
            dataFrame.groupBy("product_id").agg(
                count("price").as("gross_sales_amount"),
                sum("price").as("gross_sales_price"))
        }
        else{
            spark.emptyDataFrame
        }
    }

    def calculate_net_stats(dataFrame: DataFrame,result: DataFrame,spark: SparkSession):DataFrame ={
        if(dataFrame.columns.contains("product_id") && dataFrame.columns.contains("status") && dataFrame.columns.contains("price")){
            // --------- Problem 1-a:
            // Price and amount of returned and cancelled orders are determined so that they will be removed from gross amount and price.
            val df_net = dataFrame.filter(dataFrame("status") === "Returned" || dataFrame("status") === "Cancelled").groupBy("product_id").agg(
                count("price").as("net_sales_amount"),
                sum("price").as("net_sales_price"))

            // returned and cancelled orders are removed from gross amount and price. Then, net_sales_amount and net_sales_price columns are added to result dataframe

            if(result != null){
              if(result.columns.contains("gross_sales_amount") && result.columns.contains("gross_sales_price")){
                result.join(df_net,Seq("product_id"),"left").na.fill(0).withColumn(
                  "net_sales_amount",col("gross_sales_amount")-col("net_sales_amount")).withColumn(
                  "net_sales_price",col("gross_sales_price")-col("net_sales_price"))
              }
              else{
                calculate_gross_stats(dataFrame.filter(dataFrame("status") === "Created"),spark).join(df_net,Seq("product_id"),"left").na.fill(0).withColumn(
                  "net_sales_amount",col("gross_sales_amount")-col("net_sales_amount")).withColumn(
                  "net_sales_price",col("gross_sales_price")-col("net_sales_price"))
              }
            }
            else{
                calculate_gross_stats(dataFrame.filter(dataFrame("status") === "Created"),spark).join(df_net,Seq("product_id"),"left").na.fill(0).withColumn(
                    "net_sales_amount",col("gross_sales_amount")-col("net_sales_amount")).withColumn(
                    "net_sales_price",col("gross_sales_price")-col("net_sales_price"))
            }
        }
        else{
            spark.emptyDataFrame
        }
    }

    def calculate_avg_sales(dataFrame: DataFrame,result: DataFrame,spark: SparkSession):DataFrame ={
        if(result == null){
          spark.emptyDataFrame
        }
        else if(dataFrame.columns.contains("order_date") && dataFrame.columns.contains("product_id")){

            // --------- Problem 1-c:
            // single_date column is created by removing time information from order_date (timestamp) so only year-month-day information will be used.
            // To figure last 5 days for a product sold, daily amount of product sold for each unique day is determined for each product (groupBy).

            val df_single_date = dataFrame.withColumn("single_date",to_date(col("order_date"),"yyyy-MM-dd")).
              groupBy("product_id","single_date").count()

            // For each product, dates are sorted by descending order. Then, up to first 5 row (latest 5 days) are taken for each product.
            // Then, average count is acquired from these days. Lastly, it is added to result table as average_sales_amount_of_last_5_selling_days column.
            val window_single_date = Window.partitionBy("product_id").orderBy(col("single_date").desc)

          if(result.columns.contains("product_id")) {

            result.join(df_single_date.select(df_single_date.col("*"), rank().over(window_single_date).alias("rank"))
              .filter(col("rank") <= 5)
              .groupBy("product_id")
              .avg("count")
              .withColumnRenamed("avg(count)","average_sales_amount_of_last_5_selling_days"),Seq("product_id"),"left")
          }
          else{
            spark.emptyDataFrame
          }
        }
        else{
            spark.emptyDataFrame
        }
    }

    def calculate_highest_location(dataFrame: DataFrame,result: DataFrame,spark: SparkSession):DataFrame ={
        if(result == null){
          spark.emptyDataFrame
        }
        else if(dataFrame.columns.contains("location") && dataFrame.columns.contains("product_id")){
          // --------- Problem 1-d:
          // Number of products sold is calculated for of each location that products were sold.
          val df_location = dataFrame.groupBy("product_id","location").count()

          // Amount of products sold is sorted in decreasing order so first row for each product is the highest number of product sold.
          // This row is taken and then location of this row is joined with result table.
          val window_location = Window.partitionBy("product_id").orderBy(col("count").desc)
          if(result.columns.contains("product_id")) {
            result.join(df_location.withColumn("row",row_number.over(window_location))
              .where(col("row") === 1).drop("row")
              .withColumnRenamed("location","top_selling_location")
              .select("product_id","top_selling_location"),Seq("product_id"),"left")
          }
          else{
              spark.emptyDataFrame
          }
        }
        else{
            spark.emptyDataFrame
        }
    }


    // ------------------- Problem 2 -------------------

    def calculate_price_change(dataFrame: DataFrame,spark: SparkSession):DataFrame ={
        if(dataFrame == null){
          spark.emptyDataFrame
        }
        else if(dataFrame.columns.contains("order_date") && dataFrame.columns.contains("product_id")){
            // Orders are sorted earliest data to latest for each product.
            val window_time = Window.partitionBy("product_id").orderBy(col("order_date").asc)

            // All prices column is lagged by one so current price and latest price on same row so that they can be compared.
            // If the product is firstly sold, then it is ignored because lag price is null.
            // If the lag price is bigger than current price, it means change is marked as fall
            // If the lag price is smaller than current price, it means change is marked as rise.
            // If the lag price is equal to the current price, it is also ignored.
            var result_two = dataFrame.withColumn("lag",lag("price",1).over(window_time))
            result_two = result_two.withColumn("change",when(col("lag") > col("price"),"fall")
              .when(col("lag") < col("price"),"rise")
              .when(col("lag").isNull,"null").otherwise("null"))
            result_two.filter(result_two("change") === "rise" || result_two("change") === "fall").select("product_id","price","order_date","change")
        }
        else{
            spark.emptyDataFrame
        }

    }
}
