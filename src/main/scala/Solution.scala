import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Solution {
  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      println("need exact 2 arguments: <inputPath> <outputPath>")
      System.exit(0)
    }
    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder
      .appName("Solution")
      .getOrCreate()
    val routesDF: DataFrame = spark.read
      .options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .format("csv")
      .load(s"$inputPath/ROUTES.csv")
    routesDF.createTempView("routes")

    val result1 = spark.sql(
      """SELECT regexp_extract(ROUTE_CODE, '([А-Яа-я]{2}[0-9]*)(?=-|$)') as POINT,
        |avg(CAPACITY_CAR) as AVG_CAPACITY,
        |COUNT(*) AS ROUTE_COUNT
        |FROM routes
        |GROUP BY POINT""".stripMargin)
    result1.write.mode(SaveMode.Overwrite).csv(s"$outputPath/result1")

    val result2 = spark.sql(
      """SELECT LOGIN_CREATE, FIRST_VALUE(DATE) AS DATE, MAX(ROUTE_COUNT) AS ROUTE_COUNT
        |FROM (
        |SELECT LOGIN_CREATE, DATE_FORMAT(DATE_CREATE, 'y-MM-dd') AS DATE, COUNT(*) AS ROUTE_COUNT
        |FROM routes
        |GROUP BY LOGIN_CREATE, DATE)
        |GROUP BY LOGIN_CREATE
        |ORDER BY ROUTE_COUNT DESC
        |LIMIT 3""".stripMargin)
    result2.write.mode(SaveMode.Overwrite).csv(s"$outputPath/result2")
  }
}
