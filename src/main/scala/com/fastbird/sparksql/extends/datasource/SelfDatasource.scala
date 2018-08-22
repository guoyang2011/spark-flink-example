package com.fastbird.sparksql.`extends`.datasource

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode, _}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Created by yangguo on 2018/8/16.
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    parameters.foreach(println)
    new SelfDatasourceRelation(sqlContext)
  }
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val relation=new SelfDatasourceRelation(sqlContext)
    relation.insert(data,false)
    relation
  }

  override def shortName(): String = "fastbird.customer.datasource"
}
class SelfDatasourceRelation(@transient val sqlContext:SQLContext) extends BaseRelation  with PrunedFilteredScan with InsertableRelation{
  override def schema: StructType = StructType(Array(StructField("value",CatalystSqlParser.parseDataType("int"))))

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.schema.foreach(println)
    data.show(50)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new SelfDatasourceRDD(this)
  }
}
class SelfDatasourceRDD(relation:SelfDatasourceRelation) extends RDD[Row](relation.sqlContext.sparkContext,Nil){
  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to compute a given partition.
    */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[Row]={
    (1 to 100).map(idx=>Row.fromSeq(Seq(idx))).toIterator
  }

  /**
    * Implemented by subclasses to return the set of partitions in this RDD. This method will only
    * be called once, so it is safe to implement a time-consuming computation in it.
    *
    * The partitions in this array must satisfy the following property:
    *   `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
    */
  protected def getPartitions: Array[Partition]={
    Array(new SelfDatasourcePartition)
  }
}
class SelfDatasourcePartition extends Partition{
  override def index: Int = 0
}
