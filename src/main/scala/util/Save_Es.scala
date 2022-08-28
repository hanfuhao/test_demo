package util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Save_Es {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("hive_es")
    conf.set("cluster.name", "es")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "192.168.0.161:9200");
    conf.set("es.port", "9200");
    conf.set("es.nodes.discovery", "false");
    conf.set("org.elasticsearch.spark.sql", "true");
    conf.set("out.es.batch.size.entries", "5000"); //默认是一千
    conf.set("es.nodes.wan.only", "true");


    val sc: SparkContext = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    import org.elasticsearch.spark._
    var random: Random = new Random()

    val sex = Array("男", "女")
    val age = Array(20, 21, 22, 23, 31, 34, 38, 39, 49, 44, 43, 50, 55, 58, 60, 66, 69, 70, 73)
    val city = Array("北京", "上海", "郑州", "哈尔滨", "深圳", "广州", "杭州", "齐齐哈尔", "银川", "吉林", "苏州", "武汉")
    val occu = Array("教师", "程序员", "医生", "护士", "科研人员", "金融", "企业家", "销售", "代购", "服务员")
    val jiguan = Array("中国", "中国香港", "中国台湾", "美国", "俄罗斯", "日本", "奥地利", "中国", "中国", "中国", "中国", "中国", "中国", "中国", "中国", "中国", "中国", "中国", "中国")
    val account_type = Array(1, 2, 3)
    val province = Array("河南", "河北", "江苏", "云南", "上海", "北京", "四川", "甘肃", "贵州", "安徽", "浙江", "辽宁", "吉林", "湖南", "湖北")
    val nation = Array("汉族", "回族", "蒙古族", "傣族", "苗族", "汉族", "汉族", "汉族", "汉族", "汉族", "汉族", "汉族", "汉族")

    for (i <- 1 to 7000000) {
      val str = Map("account_id" -> i,
        "sex" -> sex(random.nextInt(sex.size)),
        "age" -> age(random.nextInt(age.size)),
        "city" -> city(random.nextInt(city.size)),
        "occu" -> occu(random.nextInt(occu.size)),
        "jiguan" -> jiguan(random.nextInt(jiguan.size)),
        "account_type" -> account_type(random.nextInt(account_type.size)),
        "province" -> province(random.nextInt(province.size)),
        "nation" -> nation(random.nextInt(nation.size))
      )

      val arr = ArrayBuffer[Map[String, Any]]()
      arr += str

      if (i % 10 == 0) {
        var value: RDD[Map[String, Any]] = sc.makeRDD(arr)
        EsSpark.saveToEs(value, "tag/_doc", Map("es.mapping.id" -> "account_id"))
        arr.clear()
      }else{
        println(i)
      }


    }

    sc.stop()

  }

}
