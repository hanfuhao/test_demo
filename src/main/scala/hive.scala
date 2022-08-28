import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object hive {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境---需要增加参数配置和开启hivesql语法支持
    val spark: SparkSession = SparkSession.builder().appName("Hive_Hbase_all").master("local[*]")
      .enableHiveSupport() //开启对hive语法的支持
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val sql_all=
      """
        |select * from app_data.app_int_app_sczs_profit_d limit 10
        |
        |""".stripMargin
    var frame: Dataset[Row] = spark.sql(sql_all)
    frame.printSchema()
    frame.rdd.foreach(println)

    sc.stop()

  }

}
