package util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object Hive_Es {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    var frame: DataFrame = spark.sql(
      """
        |select
        |account_id,age,sex,account_type,
        |city,occu,jiguan,province,nation
        |from labels.account_info
        |""".stripMargin)


    val options = Map(
      "es.index.auto.create" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "192.168.0.161:9200",
      "es.port" -> "9200",
      "es.mapping.id" -> "account_id"
    )

    frame
      .write
      .format("org.elasticsearch.spark.sql")
      .options(options)
      .mode(SaveMode.Append)
      .save("tag/_doc")

    spark.stop()
  }

}
