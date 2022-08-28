package swhy

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.elasticsearch.spark.sparkStringJsonRDDFunctions
import org.elasticsearch.spark.sql.EsSparkSQL

object Hive_Es_Json {
  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL的运行环境
    val conf = new SparkConf().setMaster("yarn").setAppName("Hive_Es_Json")
    val table = args(0);
    val id = args(1)
    val index = args(2)
    val nodes = args(3)
    val para = args(4)
    val es=args(5)
    val user=args(6)
    val pass=args(7)

    conf.set("cluster.name", es)
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", nodes);
    conf.set("es.port", "9200");
    //默认为 true，表示自动发现集群可用节点；
    //conf.set("es.nodes.discovery", "true");
    conf.set("org.elasticsearch.spark.sql", "true");
    conf.set("out.es.batch.size.entries", para); //默认是一千
    //默认为 true，设置为 true 之后，会关闭节点的自动 discovery，只使用 es.nodes 声明的节点进行数据读写操作；
    //    conf.set("es.nodes.wan.only", "false");
    conf.set("es.nodes.wan.only", "true");
    //如果装有x-pack 可以使用下面方式添加用户名密码
    conf.set("es.net.http.auth.user",user)
    conf.set("es.net.http.auth.pass",pass)
    conf.set("es.mapping.id", id)
    conf.set("es.http.timeout","3m")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()


//    var frame: DataFrame = spark.sql(
//      s"""
//         |select *
//         |from ${table}
//         |""".stripMargin)

    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""

    spark.sparkContext.makeRDD(Seq(json1, json2)).saveJsonToEs("spark/json-trips")


    //    EsSparkSQL.saveToEs(frame,index)

    spark.stop()


  }

}
