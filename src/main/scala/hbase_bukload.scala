import org.apache.hadoop.conf.Configuration

//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object hbase_bukload {

//  def main(args: Array[String]): Unit = {
//    //TODO 0.准备环境---需要增加参数配置和开启hivesql语法支持
//    val spark: SparkSession = SparkSession.builder().appName("Hive_Hbase").master("yarn")
//      .enableHiveSupport() //开启对hive语法的支持
//      .getOrCreate()
//    val sc: SparkContext = spark.sparkContext
//
//    val table=args(0)
//    val rowkey= args(1)
//    val zk=args(2)
//    val hbase=args(3)
//    val jq=args(4)
//    val principald=args(5)
//    val path=args(6)
//    import spark.implicits._
//    val sql_all=
//      s"""
//         |select * from ${table}
//         |
//         |""".stripMargin
//    var frame: Dataset[Row] = spark.sql(sql_all).sort(rowkey)
//
//    // 3. 数据转换
//    val columns: Array[String] = frame.columns
//    val rowKeyColumn=rowkey
//    val family="base_info"
//
//    write(frame,zk,"2181",hbase,family,rowKeyColumn,principald,path,jq)
//
//
//  }
//  def write(
//             dataframe: DataFrame,
//             zks: String,
//             port: String,
//             table: String,
//             family: String,
//             rowKeyColumn: String,
//             prin:String,
//             path:String,
//             jq:String
//           ): Unit = {
//    // 1. 设置HBase依赖Zookeeper相关配置信息
//    val conf: Configuration = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", zks)
//    conf.set("hbase.zookeeper.property.clientPort", port)
//    // 2. 数据写入表的名称
//    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
//
//    conf.set("hadoop.security.authentication", "kerberos");
//    conf.set("hbase.security.authentication", "kerberos");
//    conf.set("hbase.regionserver.kerberos.principal", prin);
//    conf.set("zookeeper.znode.parent", jq)
//
//    // 3. 将DataFrame中数据转换为RDD[(RowKey, Put)]
//    val cfBytes: Array[Byte] = Bytes.toBytes(family)
//    val columns: Array[String] = dataframe.columns // 从DataFrame中获取列名称
//    val datasRDD: RDD[(ImmutableBytesWritable, Put)] = dataframe.rdd.map{ row =>
//      // TODO: row 每行数据 转换为 二元组(RowKey, Put)
//      // a. 获取RowKey值
//      val rowKey: String = row.getAs[String](rowKeyColumn)
//      val rkBytes: Array[Byte] = Bytes.toBytes(rowKey)
//      // b. 构建Put对象
//      val put: Put = new Put(rkBytes)
//      // c. 设置列值
//      columns.foreach{column =>
//        val value = row.getAs[String](column)
//        put.addColumn(cfBytes, Bytes.toBytes(column), Bytes.toBytes(value))
//      }
//      // d. 返回二元组
//      (new ImmutableBytesWritable(rkBytes), put)
//    }
//
//    // 4. 保存RDD数据至HBase表中
//    datasRDD.saveAsNewAPIHadoopFile(
//      s"$path-${System.nanoTime()}", //
//      classOf[ImmutableBytesWritable], //
//      classOf[Put], //
//      classOf[TableOutputFormat[ImmutableBytesWritable]], //
//      conf
//    )
//  }

}
