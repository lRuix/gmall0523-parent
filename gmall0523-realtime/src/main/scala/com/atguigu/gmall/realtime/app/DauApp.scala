package com.atguigu.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.DauInfo
import com.atguigu.gmall.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.commons.codec.language.bm.Lang
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @Author iRuiX
 * @Date 2021/3/7 16:33
 * @Version 1.0
 * @Desc 日活业务
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DauApp").setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(5))
    var topic: String = "gmall_start_0523"
    var groupId: String = "gmall_dau_0523"

    //从Redis中获取Kafka分区偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null

    if(offsetMap!=null && offsetMap.size >0){
      //如果Redis中存在当前消费者组对该主题的偏移量信息，那么从执行的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      //如果Redis中没有当前消费者组对该主题的偏移量信息，那么还是按照配置，从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        //因为recodeDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


    val jsonDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        val ts = jsonObj.getLong("ts")
        val dateStr = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr: Array[String] = dateStr.split(" ")
        var dt = dateStrArr(0);
        var hr = dateStrArr(1);
        jsonObj.put("dt", dt);
        jsonObj.put("hr", hr);
        jsonObj
      }
    }

    //jsonDStream.print(1000)

    /** 通过Redis 对采集到的启动日志进行去重操作  方案1
     * //redis 类型 set    Key: dau:2021-03-07 value: mid expire 3600*24
     * val filterDStream: DStream[JSONObject] = jsonDStream.filter {
     * jsonObj => {
     * //获取登录日期
     * val dt = jsonObj.getString("dt")
     * //获取设备id
     * val mid = jsonObj.getJSONObject("common").getString("mid")
     * var dauKey = "dau:" + dt
     * //获取Jedis客户端
     * val jedis = MyRedisUtil.getJedisClient()
     * //从redis中判断当前设备是否已经登录过
     * val isFirst: lang.Long = jedis.sadd(dauKey, mid)
     * //设置key失效时间
     * if (jedis.ttl(dauKey) < 0) {
     *jedis.expire(dauKey, 3600 * 24)
     * }
     * *
     *jedis.close()
     * *
     * if (isFirst == 1L) {
     * true
     * } else {
     * false
     * }
     * }
     * }
     */

    //通过Redis 对采集到的启动日志进行去重操作  方案2 以分区为单位对数据进行处理，每一个分区获取一次Redis的连接
    //redis 类型 set    Key: dau:2021-03-07 value: mid expire 3600*24
    val filteredDStream: DStream[JSONObject] = jsonDStream.mapPartitions {
      jsonObjItr => { //以分区为单位对数据进行处理
        val jedis = MyRedisUtil.getJedisClient()
        //定义一个集合，用于存放当前分区中第一次登录的日志
        val filteredBuffer = new ListBuffer[JSONObject]()
        //对分区的数据进行遍历
        for (jsonObj <- jsonObjItr) {
          val mid = jsonObj.getJSONObject("common").getString("mid")
          val dt = jsonObj.getString("dt")
          var dauKey = "dau:" + dt
          val isFirst = jedis.sadd(dauKey, mid)

          // 设置key的失效时间
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }
          if (isFirst == 1L) {
            filteredBuffer.append(jsonObj)
          }
        }

        jedis.close()
        filteredBuffer.toIterator
      }
    }

    //filteredDStream.count().print()

    //将数据批量保存到ES中
    filteredDStream.foreachRDD {
      rdd => { //以分区为单位对数据进行处理
        rdd.foreachPartition {
          jsonItr => {
            val dauInfoList: List[(String,DauInfo)] = jsonItr.map {
              jsonObj => {
                //每次处理的是一个json对象 将json对象封装为样例类
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo: DauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00", //分钟没有转换，默认00
                  jsonObj.getLong("ts")
                )
                (dauInfo.mid,dauInfo)
              }
            }.toList

            //将数据批量保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList, "gmall0523_dau_info_" + dt)
          }
        }
        //提交偏移量到Redis中
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
