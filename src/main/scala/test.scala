import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object test {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val conf = new SparkConf().setMaster("yarn").setAppName("test")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val row1 = Row(1,"abc", 1.2,"0.0300",0.031,null)

    row1.getAs[String](1)
    row1.getAs[String](2)
    println(row1.getAs[String](0))
    println(row1.getAs[String](1))
    println(row1.getAs[String](2))
    println(row1.getAs[String](3))
//    var frame: DataFrame = spark.sql(
//      """
//        |select
//        |account_id,
//        |dt
//        |from labels.shop_table__tmp_1
//        |
//        |where dt<= 20220616 and dt>=20220615
//        |group by dt,account_id
//        |limit 10
//        |""".stripMargin)
//    frame.rdd.foreach(println)
//    println("=============================")
//    println("===================================")
//    var frame2: DataFrame = spark.sql(
//      """
//        |select
//        |account_id,
//        |dt
//        |from labels.bro_table_tmp_4
//        |
//        |where dt<= 20220616 and dt>=20220615
//        |group by dt,account_id
//        |limit 10
//        |""".stripMargin)
//    frame2.rdd.foreach(println)
//
//    println(124445)
//
//    spark.stop()

  }

}
