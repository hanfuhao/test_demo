package swhy

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Hive_Es {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("yarn").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    val table = args(0);
    val id = args(1)
    val index = args(2)
    val nodes = args(3)
    val es = args(4)
    val user=args(5)
    val pass=args(6)
    var frame: DataFrame = spark.sql(
      s"""
         |select *
         |from ${table}
         |""".stripMargin)

    val options = Map(
      "es.index.auto.create" -> "true",
      "es.nodes.wan.only" -> "true",
     // "es.nodes.discovery"-> "true",
      "es.nodes" -> nodes,
      "es.port" -> "9200",
      "es.mapping.id" -> id,
      "es.net.http.auth.user"->user,
      "es.net.http.auth.pass"->pass,
      "cluster.name"->es
    )
    frame
      .write
      .format("org.elasticsearch.spark.sql")
      .options(options)
      .mode(SaveMode.Append)
      .save(index)
    //.save("tag/_doc")

    spark.stop()
  }

}
