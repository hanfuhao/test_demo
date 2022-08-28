import org.apache.hadoop.hbase.client.{BufferedMutatorParams, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Hive_Hbase_all {

  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境---需要增加参数配置和开启hivesql语法支持
    val spark: SparkSession = SparkSession.builder().appName("Hive_Hbase_all").master("local[*]")
      .enableHiveSupport() //开启对hive语法的支持
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._
      val sql_all=
        """
          |select max_dt,biz_dt from app_data.app_int_app_sczs_profit_d limit 10
          |
          |""".stripMargin
    var frame: Dataset[Row] = spark.sql(sql_all)//.sort($"type")

    // 3. 数据转换
    val columns: Array[String] = frame.columns
    val rowKeyColumn="biz_dt"
    val family="base_info"
    val putsRDD: RDD[ Put] = {
      frame.rdd.map { row =>
        println(row)
        // 获取RowKey
        val rowKey: String = row.getAs[String](rowKeyColumn)
        // 构建Put对象
        val put = new Put(Bytes.toBytes(rowKey))
        // 将每列数据加入Put对象中
        val familyBytes = Bytes.toBytes(family)
        columns.foreach { column =>
          put.addColumn(
            familyBytes, //
            Bytes.toBytes(column), //
            Bytes.toBytes(row.getAs[String](column)) //
          )
        }
        // 返回put对象
        put
      }
    }

    putsRDD
      .foreachPartition(partition => {
        val hBaseConfiguration = HBaseConfiguration.create()
        hBaseConfiguration.set("hbase.zookeeper.quorum", "node95.dsjdev.com,node96.dsjdev.com,node92.dsjdev.com")
        hBaseConfiguration.set("zookeeper.znode.parent", "/hbase")
        hBaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181")

        val connection = ConnectionFactory.createConnection(hBaseConfiguration)
        val mutatorParams = new BufferedMutatorParams(TableName.valueOf("emp")).writeBufferSize(4 * 1024 * 1024)
        val bufferedMutator = connection.getBufferedMutator(mutatorParams)
        import scala.collection.JavaConversions._
        bufferedMutator.mutate(partition.toSeq)
        bufferedMutator.flush()
        bufferedMutator.close()
      })



  }



}
