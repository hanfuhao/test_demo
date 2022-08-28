package swhy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession

object hbase_bulk {
  def main(args: Array[String]): Unit = {
//    val ss = SparkSession.builder().master("local").appName("bulk-load").config(new SparkConf()).getOrCreate()
//    val sc = ss.sparkContext
//    val zookeeperQuorum = "huawei:2181"
//
//    val hdfsRootPath = "hdfs://huawei:9000"
//    val hFilePath = "hdfs://huawei:9000/tmp/hfile/"
//
//    val tableName = "test"
//    val columnFamily1 = "af"
//
//    val hadoopConf = new Configuration()
//    hadoopConf.set("fs.defaultFS", hdfsRootPath)
//    hadoopConf.set("dfs.client.use.datanode.hostname","true");
//
//
//    val config = HBaseConfiguration.create(hadoopConf)
//    config.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
//    config.set(TableOutputFormat.OUTPUT_TABLE, tableName)
//    //如果导入数据量过大,可以适当修改默认值32
//    config.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","32")
//
//
//    val rdd = sc.parallelize(Array(
//      ("1",
//        (Bytes.toBytes(columnFamily1), Bytes.toBytes("name"), Bytes.toBytes("foo1"))),
//      ("3",
//        (Bytes.toBytes(columnFamily1), Bytes.toBytes("age"), Bytes.toBytes("20"))),
//      ("3",
//        (Bytes.toBytes(columnFamily1), Bytes.toBytes("name"), Bytes.toBytes("foo3"))),
//      ("3",
//        (Bytes.toBytes(columnFamily1), Bytes.toBytes("age"), Bytes.toBytes("33"))),
//      ("5",
//        (Bytes.toBytes(columnFamily1), Bytes.toBytes("name"), Bytes.toBytes("foo5"))),
//      ("5",
//        (Bytes.toBytes(columnFamily1), Bytes.toBytes("age"), Bytes.toBytes("45"))),
//      ("4",
//        (Bytes.toBytes(columnFamily1), Bytes.toBytes("age"), Bytes.toBytes("46"))),
//      ("2",
//        (Bytes.toBytes(columnFamily1), Bytes.toBytes("name"), Bytes.toBytes("foo2"))),
//      ("2",
//        (Bytes.toBytes(columnFamily1), Bytes.toBytes("age"), Bytes.toBytes("12")))))
//      .groupByKey()
//      .repartitionAndSortWithinPartitions(new HashPartitioner(1))
//
//    val hbaseContext = new HBaseContext(sc, config)
//
//    //写Hfiles
//    hbaseContext.bulkLoadThinRows[(String, Iterable[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
//      TableName.valueOf(tableName),
//      t => {
//        val rowKey = Bytes.toBytes(t._1)
//
//        val familyQualifiersValues = new FamiliesQualifiersValues
//        t._2.foreach(f => {
//          val family:Array[Byte] = f._1
//          val qualifier = f._2
//          val value:Array[Byte] = f._3
//
//          familyQualifiersValues +=(family, qualifier, value)
//        })
//        (new ByteArrayWrapper(rowKey), familyQualifiersValues)
//      },
//      hFilePath)
//
//    try {
//
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

  }

}
