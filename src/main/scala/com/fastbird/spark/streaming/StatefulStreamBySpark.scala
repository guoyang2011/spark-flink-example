package com.fastbird.spark.streaming

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.receiver.Receiver

/**
  * Created by yangguo on 2018/8/16.
  */
class StatefulStreamBySpark {
  def mappingFunction(key:Int,value:Option[Int],state:State[Array[Int]]):Option[(Int,Array[Int])]={
    val a=state.get()
    val newB=if(value.isDefined){
      a++value
    }else{
      a
    }
    state.update(newB)

    Some((key,newB))
  }
  def main(args: Array[String]): Unit = {
    val config=new SparkConf()
    config.setAppName("stateful-streaming")
    config.setMaster("local[4]")

    val sc=new SparkContext(config)
    val streaming=new StreamingContext(sc,Duration.apply(5000))
    streaming.checkpoint("/tmp/spark")

    val source=streaming.receiverStream(new Receiver[Int](StorageLevel.MEMORY_ONLY) {
      private val atomicInt=new AtomicInteger(0)

      override def onStart(): Unit = {
        var isContinue = true
        var stopCount = 0
        while (true) {
          if (isContinue) {
            stopCount += 1
            if (stopCount > 100) {
              isContinue = false
              stopCount = 0
            } else
              store(atomicInt.getAndIncrement())
          } else {
            if (stopCount > 100) {
              isContinue = true
              stopCount = 0
            }else{
              stopCount+=1
            }
            Thread.sleep(500)
          }
          Thread.sleep(100)
        }
      }
      override def onStop(): Unit = {

      }
    })
    val mapFunction=(time: Time,key:Int,value:Option[Int],state:State[Array[Int]])=>{
      val a=state.get()
      val newB=if(value.isDefined){
        a++value
      }else{
        a
      }
      state.update(newB)

      Some((key,newB))
    }

    val updateState = (batchTime: Time, key: Int, value: Option[String], state: State[Array[String]]) => {
      val stateV = if (!state.isTimingOut()) {
        state.update(state.getOption().getOrElse(Array.empty[String]) ++ value)
        state.getOption()
      } else {
        None
      }
      Some((key, value, stateV))
    }
    val spec = StateSpec.function(updateState)
    spec.timeout(Seconds(6))

    val f:(Seq[String],Option[Seq[String]])=>Option[Seq[String]]=(values,state)=>{
      val newState=state.getOrElse(Seq.empty[String])
      Option(newState++values)
    }
    val mappedStatefulStream = source.transform(_.coalesce(5,false)).map(idx=>(idx%5,idx.toString))
    val statefulStream=mappedStatefulStream.groupByKey().mapValues(ir=>ir.toList.reduce(_+","+_)).mapWithState(spec)

    statefulStream.foreachRDD{
      rdd=>rdd.collect().foreach{
        case (key,value,state)=>{
          val sb= state.map{r=>
            if(r.length>0) r.reduce(_+","+_)
            else ""
          }
          if(sb.isDefined){
            println(key+","+value+","+sb)
          }else{
            println(key+","+value+"......................")
          }

        }
      }

    }

    streaming.start()
    streaming.awaitTermination()

  }
}
