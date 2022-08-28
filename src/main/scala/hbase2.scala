import org.apache.hadoop.hbase.client.{BufferedMutatorParams, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object hbase2 {
    def main(args: Array[String]): Unit = {
      //TODO 0.准备环境---需要增加参数配置和开启hivesql语法支持
      val spark: SparkSession = SparkSession.builder().appName("Hive_Hbase").master("yarn")
        .enableHiveSupport() //开启对hive语法的支持
        .getOrCreate()
      val sc: SparkContext = spark.sparkContext

      val table=args(0)
     val rowkey= args(1)
      val zk=args(2)
      val hbase=args(3)
      val jq=args(4)
      val sql_all=
        s"""
          |select * from ${table}
          |
          |""".stripMargin
      var frame: Dataset[Row] = spark.sql(sql_all)

      // 3. 数据转换
      val columns: Array[String] = frame.columns
      val rowKeyColumn=rowkey
      val family="base_info"
      val putsRDD: RDD[ Put] = {
        frame.rdd.map { row =>
         // println(row)
          // 获取RowKey
          val rowKey: String = row.getAs[String](rowKeyColumn)
          // 构建Put对象
          val put = new Put(Bytes.toBytes(rowKey))
          // 将每列数据加入Put对象中
          val familyBytes = Bytes.toBytes(family)
          columns.foreach { column =>
            if(row.getAs[String](column)==null || row.getAs[String](column).size==0){
              put.addColumn(
                familyBytes, //
                Bytes.toBytes(column), //
                Bytes.toBytes("") //
              )
            }else{
              put.addColumn(
                familyBytes, //
                Bytes.toBytes(column), //
                Bytes.toBytes(row.getAs[String](column)) //
              )
            }


          }
          // 返回put对象
          put
        }
      }

      putsRDD
        .foreachPartition(partition => {
          val hBaseConfiguration = HBaseConfiguration.create()
          hBaseConfiguration.set("hbase.zookeeper.quorum", zk)
          hBaseConfiguration.set("zookeeper.znode.parent", jq)
          hBaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
          hBaseConfiguration.set("hadoop.security.authentication", "kerberos");
          hBaseConfiguration.set("hbase.security.authentication", "kerberos");


          val connection = ConnectionFactory.createConnection(hBaseConfiguration)
          val mutatorParams = new BufferedMutatorParams(TableName.valueOf(hbase)).writeBufferSize(4 * 1024 * 1024)
          val bufferedMutator = connection.getBufferedMutator(mutatorParams)
          import scala.collection.JavaConversions._
          bufferedMutator.mutate(partition.toSeq)
          bufferedMutator.flush()
          bufferedMutator.close()
        })

    }




}
