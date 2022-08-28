import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.StringUtils

import scala.collection.mutable.ArrayBuffer

object sf {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
//    val table_tmp =
//      """
//        |(select
//        |*
//        |from dbo.table_rule) table_tmp
//        |""".stripMargin
//    // 读取MySQL数据
//    val df = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://192.168.0.161:3306/dbo")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "training")
//      .option("dbtable", table_tmp)
//      .load()

    //表名.字段、表名.字段；((表名.字段名*表名.字段名+表名.字段名))/表名.字段名；规则 ；值
    val rule = "source_tmp1.accoun_id、source_tmp2.accoun_id、source_tmp3.accoun_id;((source_tmp1.share*source_tmp2.hq+source_tmp3.assets))/source_tmp1.rate;>= ;10"

    //获取来源表，并进行join处理
    var strings = rule.split(";")(0).split("、")
    var table_arr = new ArrayBuffer[String]()
    var fiter_arr = new ArrayBuffer[String]()
    strings.foreach(table_clu => {
      var strings1: Array[String] = table_clu.split("\\.")
      table_arr += strings1(0)
      fiter_arr += table_clu
    })
    var join_table = "from " + table_arr(0)
    for (i <- Range(1, table_arr.size, 1)) {
      join_table += " join " + table_arr(i) + " on " + fiter_arr(0) + "=" + fiter_arr(i)
    }
    //输出join表
    println(join_table)

    //将公式特殊字符替换为""
    var str1: String = rule.split(";")(1)
    str1 = str1.replaceAll("\\(", "").replaceAll("\\)", "")
    str1 = str1.replaceAll("\\+", ",").
      replaceAll("\\-", ",").
      replaceAll("\\*", ",").
      replaceAll("\\/", ",").
      replaceAll("\\%", ",")
    //将公式内的字段类型进行转换
    var result: Array[String] = str1.split(",").map(x => {
      s"cast(${x} as double)"
    })
    //替换公式
    var rule_end: String = rule.split(";")(1)
    val mod = str1.split(",")
    for (i <- 0 to mod.size - 1) {
      rule_end = rule_end.replaceAll(mod(i), result(i))
    }

    spark.sql("use sszc")

    val sql =
      s"""
         |select
         |*
         |from
         |(
         |select
         |${fiter_arr(0)},
         |${rule_end} as end
         |${join_table}
         |)tmp
         |where
         |end ${rule.split(";")(2)} ${rule.split(";")(3)}
         |""".stripMargin

    spark.sql(sql).rdd.foreach(println)
  }

}
