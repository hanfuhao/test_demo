import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{HashPartitioner, SparkContext}

object hbase_bulk_demo {
//  def main(args: Array[String]): Unit = {
//
//    //TODO 0.准备环境---需要增加参数配置和开启hivesql语法支持
//    val spark: SparkSession = SparkSession.builder().appName("hbase_bulk_demo").master("yarn")
//      .enableHiveSupport() //开启对hive语法的支持
//      .getOrCreate()
//    val sc: SparkContext = spark.sparkContext
//
//    val table=args(0)
//    val rowKey= args(1)
//
//    val zookeeperQuorum = "node95.dsjdev.com,node96.dsjdev.com,node92.dsjdev.com"
//    val hdfsRootPath = "hdfs://nameservice1"
//    val hFilePath = "hdfs://nameservice1/tmp/hive/hfile2/"
//    val tableName = "emp3"
//    val columnFamily = "base_info"
//    val hadoopConf = new Configuration()
//    hadoopConf.set("fs.defaultFS", hdfsRootPath)
//    hadoopConf.set("dfs.client.use.datanode.hostname","true");
//    val config = HBaseConfiguration.create(hadoopConf)
//    config.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
//     //    config.set(TableOutputFormat.OUTPUT_TABLE, tableName)
//    //如果导入数据量过大,可以适当修改默认值32
//    config.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","32")
//    val sql_all=
//      s"""
//         |select * from ${table}
//         |
//         |""".stripMargin
//
//    var frame: Dataset[Row] = spark.sql(sql_all)
//
//    var columns: Array[String] = frame.columns
//    val rowkey1=rowKey
//
//    var value: RDD[(String, Iterable[(Array[Byte], Array[Byte], Array[Byte])])] = frame.rdd.map(row => {
//      val key = row.getAs[String](rowkey1)
//      columns.map(col => {
//        if (row.getAs[String](col) != null) {
//          (key, Bytes.toBytes(columnFamily), Bytes.toBytes(col), Bytes.toBytes(row.getAs[String](col)))
//        } else {
//          (key, Bytes.toBytes(columnFamily), Bytes.toBytes(col), Bytes.toBytes(""))
//        }
//      })
//    }).flatMap(x => x).map(x => (x._1, (x._2, x._3, x._4)))
//      .groupByKey().repartitionAndSortWithinPartitions(new HashPartitioner(1))
//    var value1= value
//
//    val hbaseContext1 = new HBaseContext(sc, config)
//    hbaseContext1.bulkLoadThinRows[(String, Iterable[(Array[Byte], Array[Byte], Array[Byte])])](value1, TableName.valueOf(tableName),t=>{
//      val row_Key = Bytes.toBytes(t._1)
//      val familyQualifiersValues = new FamiliesQualifiersValues
//      t._2.foreach(f => {
//        val family:Array[Byte] = f._1
//        val qualifier = f._2
//        val value:Array[Byte] = f._3
//        familyQualifiersValues +=(family, qualifier, value)
//      })
//      (new ByteArrayWrapper(row_Key), familyQualifiersValues)
//    },hFilePath)
//
//
//    try {
//      val conn = ConnectionFactory.createConnection(config)
//      val load = new LoadIncrementalHFiles(config)
//      val table = conn.getTable(TableName.valueOf(tableName))
//      load.doBulkLoad(new Path(hFilePath), conn.getAdmin, table,
//        conn.getRegionLocator(TableName.valueOf(tableName)))
//      println("写入hbase 完成!")
//      table.close()
//      conn.close()
//    }catch {
//      case e: Exception =>{
//        println(s"入库hbase失败! msg: ${e}")
//      }
//    }finally {
//      //入库完成，要删除hdfs缓存目录
//      //          HdfsUtil.delete(hFilePath,ss)
//
//    }
//
//  }

}
