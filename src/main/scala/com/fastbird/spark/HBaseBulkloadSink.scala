package com.fastbird.spark

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangguo on 2018/8/23.
  */
object HBaseBulkloadSink {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf()
    sparkConfig.setAppName("HBaseBulkloadSink-job")
    sparkConfig.setMaster("local[4]")
    sparkConfig.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConfig)

    val hadoopConfig = sc.hadoopConfiguration

    hadoopConfig.addResource("core-site.xml")
    hadoopConfig.addResource("hbase-site.xml")

    val trainingRDD = sc.makeRDD((0 to 1000))

    val testHTableName = "T_TEST"

    hadoopConfig.set("hbase.mapred.outputtable", testHTableName)
    hadoopConfig.set("create.table", "yes")



    val hfileSavePath = s"/tmp/${testHTableName}"

    val savePath=new Path(hfileSavePath)
    val fs = FileSystem.get(hadoopConfig)
    if(fs.exists(savePath)) {
      val status=fs.delete(savePath,true)
      println(s"delete path[${hfileSavePath}] status:" + status)
    }
    val sortedRDD = trainingRDD.map(_.toString).sortBy(s => s)

    val sinkRDD = sortedRDD.flatMap { i =>
      val rowkeyV = s"r_${i}"
      val tCell = new KeyValue(rowkeyV.getBytes(), "t".getBytes(), "c1".getBytes(), s"c1_v_${i}_${i}".getBytes())
      val fCell = new KeyValue(rowkeyV.getBytes(), "f".getBytes(), "c2".getBytes(), s"c2_v_${i}".getBytes())
      val rowkey = new ImmutableBytesWritable(rowkeyV.getBytes())
      Seq(rowkey -> tCell, rowkey -> fCell)
    }
    sinkRDD.saveAsNewAPIHadoopFile(hfileSavePath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hadoopConfig)

    //bulk load to hbase
    val connect = ConnectionFactory.createConnection(hadoopConfig)
    val hbaseAdmin = connect.getAdmin()
    val tableName = TableName.valueOf(testHTableName)
    if (!hbaseAdmin.tableExists(tableName)) {
      val tableDescriptor = new HTableDescriptor(tableName)
      tableDescriptor.addFamily(new HColumnDescriptor("f"))
      tableDescriptor.addFamily(new HColumnDescriptor("t"))
      //new HTableDescriptor tableName
      hbaseAdmin.createTable(tableDescriptor)
    }
    if (!hbaseAdmin.isTableEnabled(tableName)) {
      hbaseAdmin.enableTable(tableName)
    }

    val bulkLoad = new LoadIncrementalHFiles(hadoopConfig)

    val hTable = connect.getTable(tableName).asInstanceOf[HTable]
    bulkLoad.doBulkLoad(new Path(hfileSavePath), hTable)
  }

}
