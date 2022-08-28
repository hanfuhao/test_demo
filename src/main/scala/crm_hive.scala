import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark

import java.text.SimpleDateFormat

object crm_hive {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("crm_hive")
    conf.set("cluster.name", "es")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "192.168.0.161:9200")
    conf.set("es.port", "9200")
    conf.set(ConfigurationOptions.ES_SCROLL_SIZE, "1000")
    conf.set(ConfigurationOptions.ES_MAX_DOCS_PER_PARTITION, "500000")
    conf.set(ConfigurationOptions.ES_HTTP_TIMEOUT, "5m")
    conf.set(ConfigurationOptions.ES_SCROLL_KEEPALIVE, "10m")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val table_tmp =
      """
        |(select
        |*
        |from dbo.table_rule) table_tmp
        |""".stripMargin
    // 读取MySQL数据
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.0.161:3306/dbo")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "training")
      .option("dbtable", table_tmp)
      .load()

    // bro_table,bro,1,0.7,1;shop_table,shop,1.5,0.8,1;pay_table,pay,3,0.9,1 | top,100 |
    import spark.implicits._
    //根据模块id获取规则行
    var row1: Row = df.filter($"tag_id" === 1003)
      .head()
    //获取规则
    val rule = row1.getAs[String]("rule")
    //获取输出的类型
    val result = row1.getAs[String]("result")
    val begin_time = row1.getAs[String]("begin_time").toLong
    val end_time = row1.getAs[String]("end_time").toLong
    //解析rule规则
    var tuples: Array[(String, String, String, String, String)] = rule.split(";").map(per_rule => {
      var strings: Array[String] = per_rule.split(",")
      //spark, name, clumns, begin_time, end_time
      (strings(0), strings(1), strings(2), strings(3), strings(4))
    })


    //EsSpark.esJsonRDD(sc, "tag", query)
    //esJsonRDD。这种返回的也是一个tuple2类型的RDD，第一个元素依然是id，第二个是json字符串。
    //esRDD。这种返回的是一个tuple2的类型的RDD，第一个元素是id，第二个是一个map，包含ES的document元素。
    val sql=
    """
      |select account_id from labels.account_info
      |where city='北京' and age>30 and age<=60
      |""".stripMargin

    val frame1 = spark.sql(sql).cache()

    val account_info=frame1.createTempView("account_info")

    var array: Array[RDD[(String, Double)]] = tuples.map {
      case (name, clumns, weight, ratio, time) => {
        var frame = getTableRulst(spark, name, clumns, begin_time, end_time)
        frame.rdd.map(row => (row.getAs[String](0), row.getAs[String](1))).groupByKey().map(x => {
          val account_id = x._1
          val value = x._2
          var weight_result = 0.0
          for (dt <- value) {
            var range: Int = getDay(dt, end_time.toString)
            if (range > 1) {
              var part_weight = weight.toDouble
              //按照递减周期计算权重
              for (x <- Range(1, range, time.toInt)) {
                part_weight = part_weight * ratio.toDouble

              }
              weight_result += part_weight
            }
            {
              weight_result += weight.toDouble
            }
          }
          (account_id, weight_result)
        })
      }
    }


    val bro_df = array(0).toDF("account_id", "weight").createTempView("bro_df")
    val shop_df = array(1).toDF("account_id", "weight").createTempView("shop_df")
    val pay_df = array(2).toDF("account_id", "weight").createTempView("pay_df")

    var weight_result: DataFrame = spark.sql(
      """
        |select
        |tmp_table.account_id,
        |sum(weight) as weight
        |from
        |(
        |select account_id,weight
        |from
        |bro_df union all
        |select  account_id,weight
        |from
        |shop_df union all
        |select  account_id,weight
        |from
        |pay_df)tmp_table group by account_id
        |""".stripMargin)
    weight_result.createTempView("end_table")


    var strings: Array[String] = result.split(",")
    val flag = strings(0)
    val tag_rule = strings(1)

    //模式匹配
    var frame: DataFrame = flag match {
      case "top" => getEnd_top(spark, tag_rule.toLong)
      case "parcent" => getEnd_parcet(spark, tag_rule)
      case "max" => getEnd_max(spark, tag_rule.toLong)
      case "min" => getEnd_min(spark, tag_rule.toLong)
      case _ => throw new RuntimeException("类型不存在")
    }

    frame.rdd.foreach(println)
    spark.stop()

  }

  def getTableRulst(spark: SparkSession, table_Name: String, clumn: String, begin_time: Long, end_time: Long): DataFrame = {
    val sql =
      s"""
         |select
         |account_info.account_id,tmp_a.dt
         |from account_info
         |left join
         |(
         |select
         |account_id,dt
         |from
         |labels.${table_Name}
         |where ${begin_time}<= cast(dt  as bigint) and cast(dt as bigint)<= ${end_time}
         |group by account_id,dt
         |)tmp_a on account_info.account_id=tmp_a.account_id
         |where tmp_a.account_id is not null
         |order by dt desc
         |""".stripMargin

    println(sql)

    var frame: DataFrame = spark.sql(sql)
    frame
  }

  def getDay(begin_time: String, end_time: String): Int = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val st = dateFormat.parse(begin_time)
    val end = dateFormat.parse(end_time)
    val tm1 = st.getTime
    val tm2 = end.getTime
    val btDays = (tm2 - tm1) / (1000 * 3600 * 24)
    btDays.toInt
  }

  def getEnd_top(spark: SparkSession, size: Long): DataFrame = {
    var frame: DataFrame = spark.sql(
      s"""
         |select
         |account_id,weight
         |from(
         |select
         |account_id,weight,
         |row_number()over(order by weight desc) as flag
         |from
         |end_table) tmp_a
         |where flag<=${size}
         |""".stripMargin)
    frame
  }

  def getEnd_parcet(spark: SparkSession, percent: String): DataFrame = {
    var frame: DataFrame = spark.sql(
      """
        |select
        |account_id,weight
        |from(
        |select
        |account_id,weight,
        |row_number()over(order by weight desc) as flag
        |from
        |end_table) tmp_a
        |where flag<=20
        |""".stripMargin)
    frame
  }

  def getEnd_max(spark: SparkSession, size: Long): DataFrame = {
    var frame: DataFrame = spark.sql(
      s"""
         |select
         |account_id,weight
         |from(
         |select
         |account_id,weight,
         |row_number()over(order by weight desc) as flag
         |from
         |end_table) tmp_a
         |where flag<=${size}
         |""".stripMargin)
    frame
  }

  def getEnd_min(spark: SparkSession, size: Long): DataFrame = {
    var frame: DataFrame = spark.sql(
      s"""
         |select
         |account_id,weight
         |from(
         |select
         |account_id,weight,
         |row_number()over(order by weight desc) as flag
         |from
         |end_table) tmp_a
         |where flag<=${size}
         |""".stripMargin)
    frame
  }
}
