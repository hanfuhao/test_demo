package swhy
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object hbase_bulk2 {
  def main(args: Array[String]): Unit = {
//    val sc = new SparkContext("local", "test")
//    val config = new HBaseConfiguration()
//
//    val hbaseContext = new HBaseContext(sc, config)
//
//    val stagingFolder = ""
//    val rdd = sc.parallelize(Array(
//      (Bytes.toBytes("1"),
//        (Bytes.toBytes("columnFamily1"), Bytes.toBytes("a"), Bytes.toBytes("foo1"))),
//      (Bytes.toBytes("3"),
//        (Bytes.toBytes("columnFamily1"), Bytes.toBytes("b"), Bytes.toBytes("foo2.b"))), ...
//
//    val familyHBaseWriterOptions = new java.util.HashMap[Array[Byte], FamilyHFileWriteOptions]
//    val f1Options = new FamilyHFileWriteOptions("GZ", "ROW", 128, "PREFIX")
//
//    familyHBaseWriterOptions.put(Bytes.toBytes("columnFamily1"), f1Options)
//
//    rdd.hbaseBulkLoad(TableName.valueOf("table"),
//      t => {
//        val rowKey = t._1
//        val family:Array[Byte] = t._2(0)._1
//        val qualifier = t._2(0)._2
//        val value = t._2(0)._3
//
//        val keyFamilyQualifier= new KeyFamilyQualifier(rowKey, family, qualifier)
//
//        Seq((keyFamilyQualifier, value)).iterator
//      },
//      stagingFolder.getPath,
//      familyHBaseWriterOptions,
//      compactionExclude = false,
//      HConstants.DEFAULT_MAX_FILE_SIZE)
//
//    val load = new LoadIncrementalHFiles(config)
//    load.doBulkLoad(new Path(stagingFolder.getPath),
//      conn.getAdmin, table, conn.getRegionLocator(TableName.valueOf(tableName)))

  }

}
