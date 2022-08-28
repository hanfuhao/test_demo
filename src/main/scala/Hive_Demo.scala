import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Hive_Demo {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境---需要增加参数配置和开启hivesql语法支持
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
//      .config("spark.sql.shuffle.partitions", "4")
//      .config("spark.sql.warehouse.dir", "hdfs://master-1.example.com:8020/warehouse/tablespace/managed/hive") //指定Hive数据库在HDFS上的位置
//      .config("hive.metastore.uris", "thrift://master-2.example.com:9083")
      .enableHiveSupport() //开启对hive语法的支持
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    val str =
      """
        |select
        |tmp_table.grade,
        |tmp_table.clas,
        |rank()over(partition by tmp_table.grade,tmp_table.clas order by tmp_table.total_math desc) as flag
        |from
        |(
        |select
        |grade,
        |clas,
        |sum(math) as total_math
        |from sbv_file.sourcefile_table
        |group by grade,clas
        |)tmp_table
        |""".stripMargin


    spark.sql(str).createOrReplaceTempView("tmp_table1")


    spark.sql(
      """
        |select
        |grade,
        |clas,
        |case when flag>=3 then 'A'
        |when flag>=10 and flag<3 then 'B' else 'C' END as flag
        |from
        |tmp_table1
        |""".stripMargin).createOrReplaceTempView("tmp_table2")

    spark.sql(
      """
        |create table if not exists default.file_end_table2(
        |grade int,
        |clas int,
        |flag String
        |)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |STORED AS textfile
        |TBLPROPERTIES('transactional'='false')
        |""".stripMargin)

    spark.sql(
      """
        |insert into table default.file_end_table2
        |select * from tmp_table2
        |""".stripMargin)

    spark.stop()

  }

}
